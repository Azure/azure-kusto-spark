//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.microsoft.azure.kusto.data.exceptions.{DataClientException, DataServiceException}
import com.microsoft.azure.kusto.data.{Client, ClientRequestProperties, KustoResultSetTable}
import com.microsoft.kusto.spark.authentication._
import com.microsoft.kusto.spark.common.{KustoCoordinates, KustoDebugOptions}
import com.microsoft.kusto.spark.datasink.KustoWriter.TempIngestionTablePrefix
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.datasink._
import com.microsoft.kusto.spark.datasource.ReadMode.ReadMode
import com.microsoft.kusto.spark.datasource._
import com.microsoft.kusto.spark.exceptions.{
  FailedOperationException,
  TimeoutAwaitingPendingOperationException
}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.KustoConstants.{
  DefaultBatchingLimit,
  DefaultExtentsCountForSplitMergePerNode,
  DefaultMaxRetriesOnMoveExtents
}
import com.microsoft.kusto.spark.utils.{KustoConstants => KCONST}
import io.github.resilience4j.retry.{Retry, RetryConfig}
import io.vavr.CheckedFunction0
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.{SQLContext, SaveMode}

import java.io.InputStream
import java.net.URI
import java.security.InvalidParameterException
import java.util
import java.util.concurrent.{Callable, CountDownLatch, TimeUnit}
import java.util.{NoSuchElementException, Properties, StringJoiner, Timer, TimerTask, UUID}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

object KustoDataSourceUtils {

  private final val className = this.getClass.getSimpleName
  private final val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  def getDedupTagsPrefix(requestId: String, batchId: String): String = s"${requestId}_$batchId"

  def generateTempTableName(
      appName: String,
      destinationTableName: String,
      requestId: String,
      batchIdAsString: String,
      userTempTableName: Option[String]): String = {
    if (userTempTableName.isDefined) {
      userTempTableName.get
    } else {
      KustoQueryUtils.simplifyName(
        TempIngestionTablePrefix +
          appName + "_" + destinationTableName + batchIdAsString + "_" + requestId)
    }
  }

  def getReadParameters(
      parameters: Map[String, String],
      sqlContext: SQLContext): KustoReadOptions = {
    val requestedPartitions = parameters.get(KustoDebugOptions.KUSTO_NUM_PARTITIONS)
    val partitioningMode = parameters.get(KustoDebugOptions.KUSTO_READ_PARTITION_MODE)
    val numPartitions = setNumPartitions(sqlContext, requestedPartitions, partitioningMode)
    // Set default export split limit as 1GB, maximal allowed
    val readModeOption = parameters.get(KustoSourceOptions.KUSTO_READ_MODE)
    val readMode: Option[ReadMode] = if (readModeOption.isDefined) {
      Some(ReadMode.withName(readModeOption.get))
    } else {
      None
    }
    val distributedReadModeTransientCacheEnabled = parameters
      .getOrElse(KustoSourceOptions.KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE, "false")
      .trim
      .toBoolean
    val queryFilterPushDown =
      parameters.get(KustoSourceOptions.KUSTO_QUERY_FILTER_PUSH_DOWN).map(s => s.trim.toBoolean)
    val partitionColumn = parameters.get(KustoDebugOptions.KUSTO_PARTITION_COLUMN)
    val partitionOptions = PartitionOptions(numPartitions, partitionColumn, partitioningMode)
    // Parse upfront and throw back an error if there is a wrongly formatted JSON
    val additionalExportOptions =
      parameters.get(KustoSourceOptions.KUSTO_EXPORT_OPTIONS_JSON) match {
        case Some(exportOptionsJsonString) =>
          Try(
            objectMapper.readValue(
              exportOptionsJsonString,
              new TypeReference[Map[String, String]] {})) match {
            case Success(exportConfigMap) => exportConfigMap
            case Failure(exception) =>
              val errorMessage =
                s"The configuration for ${KustoSourceOptions.KUSTO_EXPORT_OPTIONS_JSON} has a value " +
                  s"$exportOptionsJsonString that cannot be parsed as Map"
              logError(className, errorMessage)
              throw new IllegalArgumentException(errorMessage)
          }
        case None => Map.empty[String, String]
      }
    val userNamePrefix = additionalExportOptions.get("namePrefix")
    if (userNamePrefix.isDefined) {
      logWarn(
        className,
        "User cannot specify namePrefix for additionalExportOptions as it can lead to unexpected behavior in reading output")
    }

    KustoReadOptions(
      readMode,
      partitionOptions,
      distributedReadModeTransientCacheEnabled,
      queryFilterPushDown,
      additionalExportOptions)
  }

  private def setNumPartitions(
      sqlContext: SQLContext,
      requestedNumPartitions: Option[String],
      partitioningMode: Option[String]): Int = {
    if (requestedNumPartitions.isDefined) requestedNumPartitions.get.toInt
    else {
      partitioningMode match {
        case Some("hash") => sqlContext.getConf("spark.sql.shuffle.partitions", "10").toInt
        // In "auto" mode we don't explicitly partition the data:
        // The data is exported and split to multiple files if required by Kusto 'export' command
        // The data is then read from the base directory for parquet files and partitioned by the parquet data source
        case _ => 1
      }
    }
  }

  private val klog = Logger.getLogger("KustoConnector")

  val DefaultMicrosoftTenant = "microsoft.com"
  val NewLine: String = sys.props("line.separator")
  var ReadInitialMaxWaitTime: FiniteDuration = 4 seconds
  var ReadMaxWaitTime: FiniteDuration = 30 seconds
  var WriteInitialMaxWaitTime: FiniteDuration = 2 seconds
  var WriteMaxWaitTime: FiniteDuration = 10 seconds

  val input: InputStream = getClass.getClassLoader.getResourceAsStream("spark.kusto.properties")
  val props = new Properties()
  props.load(input)
  var Version: String = props.getProperty("application.version")
  var clientName = s"Kusto.Spark.Connector:$Version"
  val IngestPrefix: String = props.getProperty("ingestPrefix", "ingest-")
  val EnginePrefix: String = props.getProperty("enginePrefix", "https://")
  val DefaultDomainPostfix: String = props.getProperty("defaultDomainPostfix", "core.windows.net")
  val DefaultClusterSuffix: String =
    props.getProperty("defaultClusterSuffix", "kusto.windows.net")
  val AriaClustersProxy: String =
    props.getProperty("ariaClustersProxy", "https://kusto.aria.microsoft.com")
  val PlayFabClustersProxy: String =
    props.getProperty("playFabProxy", "https://insights.playfab.com")
  val AriaClustersAlias: String = "Aria proxy"
  val PlayFabClustersAlias: String = "PlayFab proxy"
  var loggingLevel: Level = Level.INFO

  def setLoggingLevel(level: String): Unit = {
    setLoggingLevel(Level.toLevel(level))
  }

  def setLoggingLevel(level: Level): Unit = {
    loggingLevel = level
    Logger.getLogger("KustoConnector").setLevel(level)
  }

  def getLoggingLevel: Level = {
    loggingLevel
  }

  private[kusto] def logInfo(reporter: String, message: String): Unit = {
    klog.info(s"$reporter: $message")
  }

  private[kusto] def logWarn(reporter: String, message: String): Unit = {
    klog.warn(s"$reporter: $message")
  }

  private[kusto] def logError(reporter: String, message: String): Unit = {
    klog.error(s"$reporter: $message")
  }

  private[kusto] def logFatal(reporter: String, message: String): Unit = {
    klog.fatal(s"$reporter: $message")
  }

  private[kusto] def logDebug(reporter: String, message: String): Unit = {
    klog.debug(s"$reporter: $message")
  }

  private[kusto] def extractSchemaFromResultTable(result: ArrayNode): String = {

    val tableSchemaBuilder = new StringJoiner(",")
    for (i <- 0 until result.size()) {
      // Each row contains {Name, CslType, Type}, converted to (Name:CslType) pairs
      tableSchemaBuilder.add(
        s"['${result.get(i).get(KustoConstants.Schema.NAME).asText()}']:${result.get(i).get(KustoConstants.Schema.CSLTYPE).asText()}")
    }

    tableSchemaBuilder.toString
  }

  private[kusto] def getSchema(
      database: String,
      query: String,
      client: ExtendedKustoClient,
      clientRequestProperties: Option[ClientRequestProperties]): KustoSchema = {
    KustoResponseDeserializer(
      client
        .executeEngine(database, query, clientRequestProperties.orNull)
        .getPrimaryResults).getSchema
  }

  private def parseAuthentication(parameters: Map[String, String], clusterUrl: String) = {
    // Parse KustoAuthentication
    val applicationId = parameters.getOrElse(KustoSourceOptions.KUSTO_AAD_APP_ID, "")
    val applicationKey = parameters.getOrElse(KustoSourceOptions.KUSTO_AAD_APP_SECRET, "")
    val applicationCertPath =
      parameters.getOrElse(KustoSourceOptions.KUSTO_AAD_APP_CERTIFICATE_PATH, "")
    val applicationCertPassword =
      parameters.getOrElse(KustoSourceOptions.KUSTO_AAD_APP_CERTIFICATE_PASSWORD, "")
    val tokenProviderCoordinates =
      parameters.getOrElse(KustoSourceOptions.KUSTO_TOKEN_PROVIDER_CALLBACK_CLASSPATH, "")
    val keyVaultAppId: String = parameters.getOrElse(KustoSourceOptions.KEY_VAULT_APP_ID, "")
    val keyVaultAppKey = parameters.getOrElse(KustoSourceOptions.KEY_VAULT_APP_KEY, "")
    val keyVaultUri: String = parameters.getOrElse(KustoSourceOptions.KEY_VAULT_URI, "")
    val keyVaultPemFile = parameters.getOrElse(KustoDebugOptions.KEY_VAULT_PEM_FILE_PATH, "")
    val keyVaultCertKey = parameters.getOrElse(KustoDebugOptions.KEY_VAULT_CERTIFICATE_KEY, "")
    val accessToken: String = parameters.getOrElse(KustoSourceOptions.KUSTO_ACCESS_TOKEN, "")
    val userPrompt: Option[String] = parameters.get(KustoSourceOptions.KUSTO_USER_PROMPT)
    var authentication: KustoAuthentication = null
    var keyVaultAuthentication: Option[KeyVaultAuthentication] = None

    val managedIdentityAuth: Boolean =
      parameters.getOrElse(KustoSourceOptions.KUSTO_MANAGED_IDENTITY_AUTH, "false").toBoolean
    val maybeManagedClientId: Option[String] =
      parameters.get(KustoSourceOptions.KUSTO_MANAGED_IDENTITY_CLIENT_ID)

    val authorityId: String =
      parameters.getOrElse(KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID, DefaultMicrosoftTenant)

    // Check KeyVault Authentication
    if (keyVaultUri != "") {
      if (keyVaultAppId.nonEmpty) {
        keyVaultAuthentication = Some(
          KeyVaultAppAuthentication(keyVaultUri, keyVaultAppId, keyVaultAppKey, authorityId))
      } else {
        keyVaultAuthentication = Some(
          KeyVaultCertificateAuthentication(
            keyVaultUri,
            keyVaultPemFile,
            keyVaultCertKey,
            authorityId))
      }
    }

    // Look for conflicts
    var numberOfAuthenticationMethods = 0
    if (applicationId.nonEmpty) numberOfAuthenticationMethods += 1
    if (accessToken.nonEmpty) numberOfAuthenticationMethods += 1
    if (tokenProviderCoordinates.nonEmpty) numberOfAuthenticationMethods += 1
    if (keyVaultUri.nonEmpty) numberOfAuthenticationMethods += 1
    if (numberOfAuthenticationMethods > 1) {
      throw new IllegalArgumentException(
        "More than one authentication methods were provided. Failing.")
    }

    // Resolve authentication
    if (applicationId.nonEmpty) {
      // Application authentication
      if (applicationKey.nonEmpty) {
        authentication = AadApplicationAuthentication(applicationId, applicationKey, authorityId)
      } else if (applicationCertPath.nonEmpty) {
        authentication = AadApplicationCertificateAuthentication(
          applicationId,
          applicationCertPath,
          applicationCertPassword,
          authorityId)
      }
    } else if (managedIdentityAuth) {
      // Authentication for managed Identity
      authentication = ManagedIdentityAuthentication(maybeManagedClientId)
    } else if (accessToken.nonEmpty) {
      // Authentication by token
      authentication = KustoAccessTokenAuthentication(accessToken)
    } else if (tokenProviderCoordinates.nonEmpty) {
      // Authentication by token provider
      val classLoader = Thread.currentThread().getContextClassLoader
      val c1 = classLoader
        .loadClass(tokenProviderCoordinates)
        .getConstructor(parameters.getClass)
        .newInstance(parameters)
      val tokenProviderCallback = c1.asInstanceOf[Callable[String]]

      authentication = KustoTokenProviderAuthentication(tokenProviderCallback)
    } else if (keyVaultUri.isEmpty) {
      if (userPrompt.isDefined) {
        // Use only for local run where you can open the browser and logged in as your user
        authentication = KustoUserPromptAuthentication(authorityId)
      } else {
        logWarn(
          "parseSourceParameters",
          "No authentication method was supplied - using device code authentication. The token should last for one hour")
        val deviceCodeProvider = new DeviceAuthentication(clusterUrl, authorityId)
        val accessToken = deviceCodeProvider.acquireToken()
        authentication = KustoAccessTokenAuthentication(accessToken)
      }
    }
    (authentication, keyVaultAuthentication)
  }

  def parseSourceParameters(
      parameters: Map[String, String],
      allowProxy: Boolean): SourceParameters = {
    // Parse KustoTableCoordinates - these are mandatory options
    val database = parameters.get(KustoSourceOptions.KUSTO_DATABASE)
    val cluster = parameters.get(KustoSourceOptions.KUSTO_CLUSTER)

    if (database.isEmpty) {
      throw new InvalidParameterException(
        "KUSTO_DATABASE parameter is missing. Must provide a destination database name")
    }

    if (cluster.isEmpty) {
      throw new InvalidParameterException(
        "KUSTO_CLUSTER parameter is missing. Must provide a destination cluster name")
    }

    var alias = cluster
    var clusterUrl = cluster
    try {
      alias = Some(getClusterNameFromUrlIfNeeded(cluster.get.toLowerCase()))
      clusterUrl = Some(getEngineUrlFromAliasIfNeeded(cluster.get.toLowerCase()))
    } catch {
      case e: Exception =>
        if (!allowProxy) {
          throw e
        }
    }
    val table = parameters.get(KustoSinkOptions.KUSTO_TABLE)
    val requestId: String =
      parameters.getOrElse(KustoSinkOptions.KUSTO_REQUEST_ID, UUID.randomUUID().toString)
    val clientRequestProperties = getClientRequestProperties(parameters, requestId)

    val (authentication, keyVaultAuthentication) = parseAuthentication(parameters, clusterUrl.get)

    val ingestionUri = parameters.get(KustoSinkOptions.KUSTO_INGESTION_URI)
    SourceParameters(
      authentication,
      KustoCoordinates(clusterUrl.get, alias.get, database.get, table, ingestionUri),
      keyVaultAuthentication,
      requestId,
      clientRequestProperties)
  }

  case class SinkParameters(writeOptions: WriteOptions, sourceParametersResults: SourceParameters)

  case class SourceParameters(
      authenticationParameters: KustoAuthentication,
      kustoCoordinates: KustoCoordinates,
      keyVaultAuth: Option[KeyVaultAuthentication],
      requestId: String,
      clientRequestProperties: ClientRequestProperties)

  def parseSinkParameters(
      parameters: Map[String, String],
      mode: SaveMode = SaveMode.Append): SinkParameters = {
    if (mode != SaveMode.Append) {
      throw new InvalidParameterException(
        s"Kusto data source supports only 'Append' mode, '$mode' directive is invalid. Please use df.write.mode(SaveMode.Append)..")
    }

    // TODO get defaults from KustoWriter()
    // Parse WriteOptions
    var tableCreation: SinkTableCreationMode = SinkTableCreationMode.FailIfNotExist
    var tableCreationParam: Option[String] = None
    var isAsync: Boolean = false
    var isTransactionalMode: Boolean = false
    var writeModeParam: Option[String] = None
    var batchLimit: Int = 0
    var minimalExtentsCountForSplitMergePerNode: Int = 0
    var maxRetriesOnMoveExtents: Int = 0
    try {
      tableCreationParam = parameters.get(KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS)
      tableCreation =
        if (tableCreationParam.isEmpty) SinkTableCreationMode.FailIfNotExist
        else SinkTableCreationMode.withName(tableCreationParam.get)
    } catch {
      case _: NoSuchElementException =>
        throw new InvalidParameterException(
          s"No such SinkTableCreationMode option: '${tableCreationParam.get}'")
    }
    try {
      writeModeParam = parameters.get(KustoSinkOptions.KUSTO_WRITE_MODE)
      isTransactionalMode =
        if (writeModeParam.isEmpty) true
        else WriteMode.withName(writeModeParam.get) == WriteMode.Transactional
    } catch {
      case _: NoSuchElementException =>
        throw new InvalidParameterException(s"No such WriteMode option: '${writeModeParam.get}'")
    }
    val userTempTableName = parameters.get(KustoSinkOptions.KUSTO_TEMP_TABLE_NAME)
    if (userTempTableName.isDefined && (tableCreation == SinkTableCreationMode.CreateIfNotExist || !isTransactionalMode)) {
      throw new InvalidParameterException(
        "tempTableName can't be used with CreateIfNotExist or Queued write mode.")
    }
    isAsync =
      parameters.getOrElse(KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC, "false").trim.toBoolean
    val pollingOnDriver =
      parameters.getOrElse(KustoSinkOptions.KUSTO_POLLING_ON_DRIVER, "false").trim.toBoolean

    batchLimit = parameters
      .getOrElse(KustoSinkOptions.KUSTO_CLIENT_BATCHING_LIMIT, DefaultBatchingLimit.toString)
      .trim
      .toInt
    minimalExtentsCountForSplitMergePerNode = parameters
      .getOrElse(
        KustoDebugOptions.KUSTO_MAXIMAL_EXTENTS_COUNT_FOR_SPLIT_MERGE_PER_NODE,
        DefaultExtentsCountForSplitMergePerNode.toString)
      .trim
      .toInt
    maxRetriesOnMoveExtents = parameters
      .getOrElse(
        KustoDebugOptions.KUSTO_MAX_RETRIES_ON_MOVE_EXTENTS,
        DefaultMaxRetriesOnMoveExtents.toString)
      .trim
      .toInt

    val adjustSchemaParam = parameters.get(KustoSinkOptions.KUSTO_ADJUST_SCHEMA)
    val adjustSchema =
      if (adjustSchemaParam.isEmpty) SchemaAdjustmentMode.NoAdjustment
      else SchemaAdjustmentMode.withName(adjustSchemaParam.get)

    val timeout = new FiniteDuration(
      parameters
        .getOrElse(KustoSinkOptions.KUSTO_TIMEOUT_LIMIT, KCONST.DefaultWaitingIntervalLongRunning)
        .toInt,
      TimeUnit.SECONDS)
    val autoCleanupTime = new FiniteDuration(
      parameters
        .getOrElse(
          KustoSinkOptions.KUSTO_STAGING_RESOURCE_AUTO_CLEANUP_TIMEOUT,
          KCONST.DefaultCleaningInterval)
        .toInt,
      TimeUnit.SECONDS)

    val disableFlushImmediately =
      parameters.getOrElse(KustoDebugOptions.KUSTO_DISABLE_FLUSH_IMMEDIATELY, "false").toBoolean
    val ensureNoDupBlobs =
      parameters.getOrElse(KustoDebugOptions.KUSTO_ENSURE_NO_DUPLICATED_BLOBS, "false").toBoolean

    val ingestionPropertiesAsJson =
      parameters.get(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON)

    val sourceParameters = parseSourceParameters(parameters, allowProxy = false)

    val writeOptions = WriteOptions(
      pollingOnDriver,
      tableCreation,
      isAsync,
      parameters.getOrElse(KustoSinkOptions.KUSTO_WRITE_RESULT_LIMIT, "1"),
      parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, "UTC"),
      timeout,
      ingestionPropertiesAsJson,
      batchLimit,
      sourceParameters.requestId,
      autoCleanupTime,
      maxRetriesOnMoveExtents,
      minimalExtentsCountForSplitMergePerNode,
      adjustSchema,
      isTransactionalMode,
      userTempTableName,
      disableFlushImmediately,
      ensureNoDupBlobs)

    if (sourceParameters.kustoCoordinates.table.isEmpty) {
      throw new InvalidParameterException(
        "KUSTO_TABLE parameter is missing. Must provide a destination table name")
    }

    logInfo(
      "parseSinkParameters",
      s"Parsed write options for sink: {'table': '${sourceParameters.kustoCoordinates.table}', 'timeout': '${writeOptions.timeout}, 'async': ${writeOptions.isAsync}, " +
        s"'tableCreationMode': ${writeOptions.tableCreateOptions}, 'writeLimit': ${writeOptions.writeResultLimit}, 'batchLimit': ${writeOptions.batchLimit}" +
        s", 'timeout': ${writeOptions.timeout}, 'timezone': ${writeOptions.timeZone}, " +
        s"'ingestionProperties': $ingestionPropertiesAsJson, 'requestId': '${sourceParameters.requestId}', 'pollingOnDriver': ${writeOptions.pollingOnDriver}," +
        s"'maxRetriesOnMoveExtents':$maxRetriesOnMoveExtents, 'minimalExtentsCountForSplitMergePerNode':$minimalExtentsCountForSplitMergePerNode, " +
        s"'adjustSchema': $adjustSchema, 'autoCleanupTime': $autoCleanupTime${if (writeOptions.userTempTableName.isDefined)
            s", userTempTableName: ${userTempTableName.get}"
          else ""}, disableFlushImmediately: $disableFlushImmediately${if (ensureNoDupBlobs) "ensureNoDupBlobs: true"
          else ""}")

    SinkParameters(writeOptions, sourceParameters)
  }

  def retryApplyFunction[T](func: () => T, retryConfig: RetryConfig, retryName: String): T = {
    val retry = Retry.of(retryName, retryConfig)
    val f: CheckedFunction0[T] = new CheckedFunction0[T]() {
      override def apply(): T = func()
    }

    retry.executeCheckedSupplier(f)
  }

  def getClientRequestProperties(
      parameters: Map[String, String],
      requestId: String): ClientRequestProperties = {
    val crpOption = parameters.get(KustoSourceOptions.KUSTO_CLIENT_REQUEST_PROPERTIES_JSON)

    val crp = if (crpOption.isDefined) {
      ClientRequestProperties.fromString(crpOption.get)
    } else {
      new ClientRequestProperties
    }

    crp.setClientRequestId(requestId)
    crp
  }

  private[kusto] def reportExceptionAndThrow(
      reporter: String,
      exception: Throwable,
      doingWhat: String = "",
      cluster: String = "",
      database: String = "",
      table: String = "",
      requestId: String = "",
      shouldNotThrow: Boolean = false): Unit = {
    val whatFailed = if (doingWhat.isEmpty) "" else s"when $doingWhat"
    val clusterDesc = if (cluster.isEmpty) "" else s", cluster: '$cluster' "
    val databaseDesc = if (database.isEmpty) "" else s", database: '$database'"
    val tableDesc = if (table.isEmpty) "" else s", table: '$table'"
    val requestIdDesc = if (requestId.isEmpty) "" else s", requestId: '$requestId'"

    if (!shouldNotThrow) {
      logError(
        reporter,
        s"caught exception $whatFailed$clusterDesc$databaseDesc$tableDesc$requestIdDesc.${NewLine}EXCEPTION: ${ExceptionUtils
            .getStackTrace(exception)}")
      throw exception
    }

    logWarn(
      reporter,
      s"caught exception $whatFailed$clusterDesc$databaseDesc$tableDesc$requestIdDesc, exception ignored.${NewLine}EXCEPTION: ${ExceptionUtils
          .getStackTrace(exception)}")
  }

  private[kusto] def getClusterNameFromUrlIfNeeded(cluster: String): String = {
    if (cluster.equals(AriaClustersProxy)) {
      AriaClustersAlias
    } else if (cluster.equals(PlayFabClustersProxy)) {
      PlayFabClustersAlias
    } else if (cluster.startsWith(EnginePrefix)) {
      if (!cluster.contains(".kusto.") && !cluster.contains(".kustodev.")) {
        throw new InvalidParameterException(
          "KUSTO_CLUSTER parameter accepts either a full url with https scheme or the cluster's" +
            "alias and tries to construct the full URL from it. Parameter given: " + cluster)
      }
      val host = new URI(cluster).getHost
      val startIdx = if (host.startsWith(IngestPrefix)) IngestPrefix.length else 0
      val endIdx =
        if (cluster.contains(".kustodev.")) host.indexOf(".kustodev.")
        else host.indexOf(".kusto.")
      host.substring(startIdx, endIdx)

    } else {
      cluster
    }
  }

  private[kusto] def getEngineUrlFromAliasIfNeeded(cluster: String): String = {
    if (cluster.startsWith(EnginePrefix)) {

      val host = new URI(cluster).getHost
      if (host.startsWith(IngestPrefix)) {
        val startIdx = IngestPrefix.length
        val uriBuilder = new URIBuilder()
        uriBuilder.setHost(
          s"${host.substring(startIdx, host.indexOf(".kusto."))}.kusto.windows.net")
        uriBuilder.setScheme("https").toString
      } else {
        cluster
      }
    } else {
      val uriBuilder = new URIBuilder()
      uriBuilder.setScheme("https").setHost(s"$cluster.kusto.windows.net").toString
    }
  }

  /**
   * A function to run sequentially async work on TimerTask using a Timer. The function passed is
   * scheduled sequentially by the timer, until last calculated returned value by func does not
   * satisfy the condition of doWhile or a given number of times has passed. After this condition
   * was satisfied, the finalWork function is called over the last returned value by func. Returns
   * a CountDownLatch object used to count down iterations and await on it synchronously if needed
   *
   * @param func
   *   \- the function to run
   * @param delayBeforeStart
   *   \- delay before first job
   * @param delayBeforeEach
   *   \- delay between jobs
   * @param doWhileCondition
   *   \- go one while the condition holds for the func.apply output
   * @param finalWork
   *   \- do final work with the last func.apply output
   */
  def doWhile[A](
      func: () => A,
      delayBeforeStart: Long,
      delayBeforeEach: Int,
      doWhileCondition: A => Boolean,
      finalWork: A => Unit,
      maxWaitTimeBetweenCallsMillis: Int,
      maxWaitTimeAfterMinute: Int): CountDownLatch = {
    val latch = new CountDownLatch(1)
    val t = new Timer()
    var currentWaitTime = delayBeforeEach
    var waitedTime = 0
    var maxWaitTime = maxWaitTimeBetweenCallsMillis

    class ExponentialBackoffTask extends TimerTask {
      def run(): Unit = {
        try {
          val res = func.apply()

          if (!doWhileCondition.apply(res)) {
            finalWork.apply(res)
            while (latch.getCount > 0) latch.countDown()
            t.cancel()
          } else {
            waitedTime += currentWaitTime
            if (waitedTime > TimeUnit.MINUTES.toMillis(1)) {
              maxWaitTime = maxWaitTimeAfterMinute
            }
            currentWaitTime =
              if (currentWaitTime + currentWaitTime > maxWaitTime) maxWaitTime
              else currentWaitTime + currentWaitTime
            t.schedule(new ExponentialBackoffTask(), currentWaitTime)
          }
        } catch {
          case exception: Exception =>
            while (latch.getCount > 0) latch.countDown()
            t.cancel()
            throw exception
        }
      }
    }

    val task: TimerTask = new ExponentialBackoffTask()
    t.schedule(task, delayBeforeStart)

    latch
  }

  // Throws on Failure or timeout
  def verifyAsyncCommandCompletion(
      client: Client,
      database: String,
      commandResult: KustoResultSetTable,
      samplePeriod: FiniteDuration = KCONST.DefaultPeriodicSamplePeriod,
      timeOut: FiniteDuration,
      doingWhat: String,
      loggerName: String,
      requestId: String): Option[KustoResultSetTable] = {
    commandResult.next()
    val operationId = commandResult.getString(0)
    val operationsShowCommand = CslCommandsGenerator.generateOperationsShowCommand(operationId)
    val sampleInMillis = samplePeriod.toMillis.toInt
    val timeoutInMillis = timeOut.toMillis
    val delayPeriodBetweenCalls = if (sampleInMillis < 1) 1 else sampleInMillis

    val stateCol = "State"
    val statusCol = "Status"
    val statusCheck: () => Option[KustoResultSetTable] = () => {
      try {
        Some(client.execute(database, operationsShowCommand).getPrimaryResults)
      } catch {
        case e: DataServiceException =>
          if (e.isPermanent) {
            val message =
              s"Couldn't monitor the progress of the $doingWhat on requestId: $requestId operation from the service, you may track" +
                s" it using the command '$operationsShowCommand'."
            logError("verifyAsyncCommandCompletion", message)
            throw new Exception(message, e)
          }
          logWarn(
            "verifyAsyncCommandCompletion",
            "Failed transiently to retrieve export status, trying again in a few seconds")
          None
        case _: DataClientException => None
      }
    }
    var lastResponse: Option[KustoResultSetTable] = None
    val task = doWhile[Option[KustoResultSetTable]](
      func = statusCheck,
      delayBeforeStart = 0,
      delayBeforeEach = delayPeriodBetweenCalls,
      doWhileCondition = (result: Option[KustoResultSetTable]) => {
        val inProgress =
          result.isEmpty || (result.get.next() && result.get.getString(stateCol) == "InProgress")
        if (inProgress) {
          logDebug(
            loggerName,
            s"Async operation $doingWhat on requestId $requestId, is in status 'InProgress'," +
              "polling status again in a few seconds")
        }
        inProgress
      },
      finalWork = (result: Option[KustoResultSetTable]) => {
        lastResponse = result
      },
      maxWaitTimeBetweenCallsMillis = ReadInitialMaxWaitTime.toMillis.toInt,
      ReadMaxWaitTime.toMillis.toInt)

    var success = true
    if (timeOut < FiniteDuration.apply(0, SECONDS)) {
      task.await()
    } else {
      if (!task.await(timeoutInMillis, TimeUnit.MILLISECONDS)) {
        // Timed out
        success = false
      }
    }

    if (lastResponse.isEmpty || lastResponse.get.getString(stateCol) != "Completed") {
      throw new FailedOperationException(
        s"Failed to execute Kusto operation with OperationId '$operationId', State: '${lastResponse.get
            .getString(stateCol)}'," +
          s" Status: '${lastResponse.get.getString(statusCol)}'",
        lastResponse)
    }

    if (!success) {
      throw new TimeoutAwaitingPendingOperationException(
        s"Timed out while waiting for operation with OperationId '$operationId'")
    }

    lastResponse
  }

  private[kusto] def mergeKeyVaultAndOptionsAuthentication(
      paramsFromKeyVault: AadApplicationAuthentication,
      authenticationParameters: Option[KustoAuthentication]): KustoAuthentication = {
    if (authenticationParameters.isEmpty) {
      // We have both keyVault and AAD application params, take from options first and throw if both are empty
      try {
        val app = authenticationParameters.asInstanceOf[AadApplicationAuthentication]
        AadApplicationAuthentication(
          ID = if (app.ID == "") {
            if (paramsFromKeyVault.ID == "") {
              throw new InvalidParameterException(
                "AADApplication ID is empty. Please pass it in keyVault or options")
            }
            paramsFromKeyVault.ID
          } else {
            app.ID
          },
          password = if (app.password == "") {
            if (paramsFromKeyVault.password == "AADApplication key is empty. Please pass it in keyVault or options") {
              throw new InvalidParameterException("")
            }
            paramsFromKeyVault.password
          } else {
            app.password
          },
          authority =
            if (app.authority == "microsoft.com") paramsFromKeyVault.authority else app.authority)
      } catch {
        case _: ClassCastException =>
          throw new UnsupportedOperationException(
            "keyVault authentication can be combined only with AADAplicationAuthentication")
      }
    } else {
      paramsFromKeyVault
    }
  }

  // Try get key vault parameters - if fails use transientStorageParameters
  private[kusto] def mergeKeyVaultAndOptionsStorageParams(
      transientStorageParameters: Option[TransientStorageParameters],
      keyVaultAuthentication: KeyVaultAuthentication): Option[TransientStorageParameters] = {

    val keyVaultCredential = KeyVaultUtils.getStorageParamsFromKeyVault(keyVaultAuthentication)
    try {
      val domainSuffix =
        if (StringUtils.isNotBlank(keyVaultCredential.domainSuffix))
          keyVaultCredential.domainSuffix
        else KustoDataSourceUtils.DefaultDomainPostfix
      Some(new TransientStorageParameters(Array(keyVaultCredential), domainSuffix))
    } catch {
      case ex: Exception =>
        if (transientStorageParameters.isDefined) {
          // If storage option defined - take it
          transientStorageParameters
        } else {
          throw ex
        }
    }
  }

  private[kusto] def countRows(
      client: Client,
      query: String,
      database: String,
      crp: ClientRequestProperties): Int = {
    val res = client.execute(database, generateCountQuery(query), crp).getPrimaryResults
    res.next()
    res.getInt(0)
  }

  // No need to retry here - if an exception is caught - fallback to distributed mode
  private[kusto] def estimateRowsCount(
      client: Client,
      query: String,
      database: String,
      crp: ClientRequestProperties): Int = {
    val estimationResult: util.List[AnyRef] = Await.result(
      Future {
        val res =
          client.execute(database, generateEstimateRowsCountQuery(query), crp).getPrimaryResults
        res.next()
        res.getCurrentRow
      },
      KustoConstants.TimeoutForCountCheck)
    val maybeEstimatedCount = Option(estimationResult.get(1))
    /*
     Check if the result is null or an empty string return a 0 , else return the numeric value
     */
    val estimatedCount = maybeEstimatedCount match {
      case Some(ecStr: String) =>
        if (StringUtils.isBlank(ecStr) || !StringUtils.isNumeric(ecStr)) /* Empty estimate */ 0
        else ecStr.toInt
      case Some(ecInt: java.lang.Number) =>
        ecInt.intValue() // Is a numeric , get the int value back
      case None => 0 // No value
    }
    // We cannot be finitely determine the count , or have a 0 count. Recheck using a 'query | count()'
    if (estimatedCount == 0) {
      Await.result(
        Future {
          val res = client.execute(database, generateCountQuery(query), crp).getPrimaryResults
          res.next()
          res.getInt(0)
        },
        KustoConstants.TimeoutForCountCheck)
    } else {
      estimatedCount
    }
  }
}

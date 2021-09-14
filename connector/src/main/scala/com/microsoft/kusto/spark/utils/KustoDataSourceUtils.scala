package com.microsoft.kusto.spark.utils

import java.io.InputStream
import java.net.URI
import java.security.InvalidParameterException
import java.util
import java.util.Properties
import java.util.concurrent.{Callable, CountDownLatch, TimeUnit, TimeoutException}
import java.util.{NoSuchElementException, StringJoiner, Timer, TimerTask, UUID}

import com.microsoft.azure.kusto.data.{Client, ClientRequestProperties, KustoResultSetTable}
import com.microsoft.azure.kusto.data.exceptions.{DataClientException, DataServiceException}
import com.microsoft.kusto.spark.authentication._
import com.microsoft.kusto.spark.common.{KustoCoordinates, KustoDebugOptions}
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SchemaAdjustmentMode, SinkTableCreationMode, WriteOptions}
import com.microsoft.kusto.spark.datasource.{KustoResponseDeserializer, KustoSchema, KustoSourceOptions, KustoStorageParameters}
import com.microsoft.kusto.spark.exceptions.{FailedOperationException, TimeoutAwaitingPendingOperationException}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoConstants => KCONST}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import com.microsoft.kusto.spark.utils.KustoConstants.{DefaultBatchingLimit, DefaultExtentsCountForSplitMergePerNode, DefaultMaxRetriesOnMoveExtents}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.matching.Regex
import org.apache.commons.lang3.StringUtils
import org.apache.http.client.utils.URIBuilder
import org.json.JSONObject

object KustoDataSourceUtils {
  private val klog = Logger.getLogger("KustoConnector")

  val SasPattern: Regex = raw"(?:https://)?([^.]+).blob.([^/]+)/([^?]+)?(.+)".r
  val DefaultMicrosoftTenant = "microsoft.com"
  val NewLine: String = sys.props("line.separator")
  val ReadMaxWaitTime: FiniteDuration = 30 seconds
  val WriteMaxWaitTime: FiniteDuration = 5 seconds

  val input: InputStream = getClass.getClassLoader.getResourceAsStream("spark.kusto.properties")
  val props = new Properties()
  props.load(input)
  var Version: String = props.getProperty("application.version")
  var clientName = s"Kusto.Spark.Connector:$Version"
  val IngestPrefix: String = props.getProperty("ingestPrefix", "ingest-")
  val EnginePrefix: String = props.getProperty("enginePrefix", "https://")
  val DefaultDomainPostfix: String = props.getProperty("defaultDomainPostfix", "core.windows.net")
  val DefaultClusterSuffix: String = props.getProperty("defaultClusterSuffix", "kusto.windows.net")
  val AriaClustersProxy: String = props.getProperty("ariaClustersProxy", "https://kusto.aria.microsoft.com")
  val PlayFabClustersProxy: String = props.getProperty("playFabProxy", "https://insights.playfab.com")
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

  private[kusto] def extractSchemaFromResultTable(result: Iterable[JSONObject]): String = {

    val tableSchemaBuilder = new StringJoiner(",")

    for (row <- result) {
      // Each row contains {Name, CslType, Type}, converted to (Name:CslType) pairs
      tableSchemaBuilder.add(s"['${row.getString("Name")}']:${row.getString("CslType")}")
    }

    tableSchemaBuilder.toString
  }

  private[kusto] def getSchema(database: String, query: String, client: Client, clientRequestProperties: Option[ClientRequestProperties]): KustoSchema = {
    KustoResponseDeserializer(client.execute(database, query, clientRequestProperties.orNull).getPrimaryResults).getSchema
  }

  def parseSourceParameters(parameters: Map[String, String]): SourceParameters = {
    // Parse KustoTableCoordinates - these are mandatory options
    val database = parameters.get(KustoSourceOptions.KUSTO_DATABASE)
    val cluster = parameters.get(KustoSourceOptions.KUSTO_CLUSTER)

    if (database.isEmpty) {
      throw new InvalidParameterException("KUSTO_DATABASE parameter is missing. Must provide a destination database name")
    }

    if (cluster.isEmpty) {
      throw new InvalidParameterException("KUSTO_CLUSTER parameter is missing. Must provide a destination cluster name")
    }
    val alias = getClusterNameFromUrlIfNeeded(cluster.get.toLowerCase())
    val clusterUrl = getEngineUrlFromAliasIfNeeded(cluster.get.toLowerCase())
    val table = parameters.get(KustoSinkOptions.KUSTO_TABLE)
    val requestId: String = parameters.getOrElse(KustoSinkOptions.KUSTO_REQUEST_ID, UUID.randomUUID().toString)
    val clientRequestProperties = getClientRequestProperties(parameters, requestId)
    // Parse KustoAuthentication
    val applicationId = parameters.getOrElse(KustoSourceOptions.KUSTO_AAD_APP_ID, "")
    val applicationKey = parameters.getOrElse(KustoSourceOptions.KUSTO_AAD_APP_SECRET, "")
    val applicationCertPath = parameters.getOrElse(KustoSourceOptions.KUSTO_AAD_APP_CERTIFICATE_PATH, "")
    val applicationCertPassword = parameters.getOrElse(KustoSourceOptions.KUSTO_AAD_APP_CERTIFICATE_PASSWORD, "")
    val tokenProviderCoordinates = parameters.getOrElse(KustoSourceOptions.KUSTO_TOKEN_PROVIDER_CALLBACK_CLASSPATH, "")
    val keyVaultAppId: String = parameters.getOrElse(KustoSourceOptions.KEY_VAULT_APP_ID, "")
    val keyVaultAppKey = parameters.getOrElse(KustoSourceOptions.KEY_VAULT_APP_KEY, "")
    val keyVaultUri: String = parameters.getOrElse(KustoSourceOptions.KEY_VAULT_URI, "")
    val keyVaultPemFile = parameters.getOrElse(KustoDebugOptions.KEY_VAULT_PEM_FILE_PATH, "")
    val keyVaultCertKey = parameters.getOrElse(KustoDebugOptions.KEY_VAULT_CERTIFICATE_KEY, "")
    val accessToken: String = parameters.getOrElse(KustoSourceOptions.KUSTO_ACCESS_TOKEN, "")
    val userPrompt: Option[String] = parameters.get(KustoSourceOptions.KUSTO_USER_PROMPT)
    var authentication: KustoAuthentication = null
    var keyVaultAuthentication: Option[KeyVaultAuthentication] = None

    val authorityId: String = parameters.getOrElse(KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID, DefaultMicrosoftTenant)

    // Check KeyVault Authentication
    if (keyVaultUri != "") {
      if (keyVaultAppId.nonEmpty) {
        keyVaultAuthentication = Some(KeyVaultAppAuthentication(keyVaultUri,
          keyVaultAppId,
          keyVaultAppKey,
          authorityId))
      } else {
        keyVaultAuthentication = Some(KeyVaultCertificateAuthentication(keyVaultUri,
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
      throw new IllegalArgumentException("More than one authentication methods were provided. Failing.")
    }

    // Resolve authentication
    if (applicationId.nonEmpty) {
      // Application authentication
      if (applicationKey.nonEmpty) {
        authentication = AadApplicationAuthentication(applicationId, applicationKey, authorityId)
      } else if (applicationCertPath.nonEmpty) {
        authentication = AadApplicationCertificateAuthentication(applicationId, applicationCertPath, applicationCertPassword, authorityId)
      }
    } else if (accessToken.nonEmpty) {
      // Authentication by token
      authentication = KustoAccessTokenAuthentication(accessToken)
    } else if (tokenProviderCoordinates.nonEmpty) {
      // Authentication by token provider
      val classLoader = Thread.currentThread().getContextClassLoader
      val c1 = classLoader.loadClass(tokenProviderCoordinates).getConstructor(parameters.getClass).newInstance(parameters)
      val tokenProviderCallback = c1.asInstanceOf[Callable[String]]

      authentication = KustoTokenProviderAuthentication(tokenProviderCallback)
    } else if (keyVaultUri.isEmpty) {
      if (userPrompt.isDefined) {
        // Use only for local run where you can open the browser and logged in as your user
        authentication = KustoUserPromptAuthentication(authorityId)
      } else {
        logWarn("parseSourceParameters", "No authentication method was supplied - using device code authentication. The token should last for one hour")
        val deviceCodeProvider = new DeviceAuthentication(clusterUrl, authorityId)
        val accessToken = deviceCodeProvider.acquireToken()
        authentication = KustoAccessTokenAuthentication(accessToken)
      }
    }

    SourceParameters(authentication, KustoCoordinates(clusterUrl, alias, database.get, table),
      keyVaultAuthentication, requestId, clientRequestProperties)
  }

  case class SinkParameters(writeOptions: WriteOptions, sourceParametersResults: SourceParameters)

  case class SourceParameters(authenticationParameters: KustoAuthentication, kustoCoordinates: KustoCoordinates,
                              keyVaultAuth: Option[KeyVaultAuthentication], requestId:String,
                              clientRequestProperties: ClientRequestProperties)

  def parseSinkParameters(parameters: Map[String, String], mode: SaveMode = SaveMode.Append): SinkParameters = {
    if (mode != SaveMode.Append) {
      throw new InvalidParameterException(s"Kusto data source supports only 'Append' mode, '$mode' directive is invalid. Please use df.write.mode(SaveMode.Append)..")
    }

    // Parse WriteOptions
    var tableCreation: SinkTableCreationMode = SinkTableCreationMode.FailIfNotExist
    var tableCreationParam: Option[String] = None
    var isAsync: Boolean = false
    var isAsyncParam: String = ""
    var batchLimit: Int = 0
    var minimalExtentsCountForSplitMergePerNode: Int = 0
    var maxRetriesOnMoveExtents: Int = 0
    try {
      isAsyncParam = parameters.getOrElse(KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC, "false")
      isAsync = parameters.getOrElse(KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC, "false").trim.toBoolean
      tableCreationParam = parameters.get(KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS)
      tableCreation = if (tableCreationParam.isEmpty) SinkTableCreationMode.FailIfNotExist else SinkTableCreationMode.withName(tableCreationParam.get)
      batchLimit = parameters.getOrElse(KustoSinkOptions.KUSTO_CLIENT_BATCHING_LIMIT, DefaultBatchingLimit.toString)
        .trim.toInt
      minimalExtentsCountForSplitMergePerNode = parameters.getOrElse(KustoDebugOptions
        .KUSTO_MAXIMAL_EXTENTS_COUNT_FOR_SPLIT_MERGE_PER_NODE, DefaultExtentsCountForSplitMergePerNode.toString).trim.toInt
      maxRetriesOnMoveExtents = parameters.getOrElse(KustoDebugOptions.KUSTO_MAX_RETRIES_ON_MOVR_EXTENTS,
        DefaultMaxRetriesOnMoveExtents.toString).trim.toInt
    } catch {
      case _: NoSuchElementException => throw new InvalidParameterException(s"No such SinkTableCreationMode option: '${tableCreationParam.get}'")
      case _: java.lang.IllegalArgumentException => throw new InvalidParameterException(s"KUSTO_WRITE_ENABLE_ASYNC is expecting either 'true' or 'false', got: '$isAsyncParam'")
    }

    val adjustSchemaParam = parameters.get(KustoSinkOptions.KUSTO_ADJUST_SCHEMA)
    val adjustSchema = if (adjustSchemaParam.isEmpty) SchemaAdjustmentMode.NoAdjustment else SchemaAdjustmentMode.withName(adjustSchemaParam.get)

    val timeout = new FiniteDuration(parameters.getOrElse(KustoSinkOptions.KUSTO_TIMEOUT_LIMIT, KCONST
      .DefaultWaitingIntervalLongRunning).toInt, TimeUnit.SECONDS)
    val autoCleanupTime = new FiniteDuration(parameters.getOrElse(KustoSinkOptions.KUSTO_STAGING_RESOURCE_AUTO_CLEANUP_TIMEOUT, KCONST
      .DefaultCleaningInterval).toInt, TimeUnit.SECONDS)

    val ingestionPropertiesAsJson = parameters.get(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON)

    val sourceParameters = parseSourceParameters(parameters)

    val writeOptions = WriteOptions(
      tableCreation,
      isAsync,
      parameters.getOrElse(KustoSinkOptions.KUSTO_WRITE_RESULT_LIMIT, "1"),
      parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, "UTC"),
      timeout,
      ingestionPropertiesAsJson,
      batchLimit,
      sourceParameters.requestId,
      autoCleanupTime,
      minimalExtentsCountForSplitMergePerNode,
      maxRetriesOnMoveExtents,
      adjustSchema
    )

    if (sourceParameters.kustoCoordinates.table.isEmpty) {
      throw new InvalidParameterException("KUSTO_TABLE parameter is missing. Must provide a destination table name")
    }

    logInfo("parseSinkParameters", s"Parsed write options for sink: {'timeout': ${writeOptions.timeout}, 'async': ${writeOptions.isAsync}, " +
      s"'tableCreationMode': ${writeOptions.tableCreateOptions}, 'writeLimit': ${writeOptions.writeResultLimit}, 'batchLimit': ${writeOptions.batchLimit}" +
      s", 'timeout': ${writeOptions.timeout}, 'timezone': ${writeOptions.timeZone}, " +
      s"'ingestionProperties': $ingestionPropertiesAsJson, requestId: ${sourceParameters.requestId}}")

    SinkParameters(writeOptions, sourceParameters)
  }

  def getClientRequestProperties(parameters: Map[String, String], requestId: String): ClientRequestProperties = {
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
                                              exception: Exception,
                                              doingWhat: String = "",
                                              cluster: String = "",
                                              database: String = "",
                                              table: String = "",
                                              shouldNotThrow: Boolean = false): Unit = {
    val whatFailed = if (doingWhat.isEmpty) "" else s"when $doingWhat"
    val clusterDesc = if (cluster.isEmpty) "" else s", cluster: '$cluster' "
    val databaseDesc = if (database.isEmpty) "" else s", database: '$database'"
    val tableDesc = if (table.isEmpty) "" else s", table: '$table'"

    if (!shouldNotThrow) {
      logError(reporter, s"caught exception $whatFailed$clusterDesc$databaseDesc$tableDesc.${NewLine}EXCEPTION: ${ExceptionUtils.getStackTrace(exception)}")
      throw exception
    }

    logWarn(reporter, s"caught exception $whatFailed$clusterDesc$databaseDesc$tableDesc, exception ignored.${NewLine}EXCEPTION: ${ExceptionUtils.getStackTrace(exception)}")
  }

  private[kusto] def getClusterNameFromUrlIfNeeded(cluster: String): String = {
    if (cluster.equals(AriaClustersProxy)) {
      AriaClustersAlias
    } else if(cluster.equals(PlayFabClustersProxy)) {
      PlayFabClustersAlias
    }
    else if (cluster.startsWith(EnginePrefix)) {
      if (!cluster.contains(".kusto.") && !cluster.contains(".kustodev.")) {
        throw new InvalidParameterException("KUSTO_CLUSTER parameter accepts either a full url with https scheme or the cluster's" +
          "alias and tries to construct the full URL from it. Parameter given: " + cluster)
      }
      val host = new URI(cluster).getHost
      val startIdx = if (host.startsWith(IngestPrefix)) IngestPrefix.length else 0
      val endIdx = if(cluster.contains(".kustodev.")) host.indexOf(".kustodev.") else host.indexOf(".kusto.")
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
        uriBuilder.setHost(s"${host.substring(startIdx, host.indexOf(".kusto."))}.kusto.windows.net")
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
   * A function to run sequentially async work on TimerTask using a Timer.
   * The function passed is scheduled sequentially by the timer, until last calculated returned value by func does not
   * satisfy the condition of doWhile or a given number of times has passed.
   * After either this condition was satisfied or the 'numberOfTimesToRun' has passed (This can be avoided by setting
   * numberOfTimesToRun to a value less than 0), the finalWork function is called over the last returned value by func.
   * Returns a CountDownLatch object used to count down iterations and await on it synchronously if needed
   *
   * @param func             - the function to run
   * @param delayBeforeStart - delay before first job
   * @param delayBeforeEach  - delay between jobs
   * @param timesToRun       - stop jobs after numberOfTimesToRun.
   *                         set negative value to run infinitely
   * @param stopCondition    - stop jobs if condition holds for the func.apply output
   * @param finalWork        - do final work with the last func.apply output
   */
  def doWhile[A](func: () => A, delayBeforeStart: Long, delayBeforeEach: Int, timesToRun: Int, stopCondition: A => Boolean, finalWork: A => Unit, maxWaitTimeBetweenCalls: Int): CountDownLatch = {
    val latch = new CountDownLatch(if (timesToRun > 0) timesToRun else 1)
    val t = new Timer()
    var currentWaitTime = delayBeforeEach

    class ExponentialBackoffTask extends TimerTask {
      def run(): Unit = {
        try {
          val res = func.apply()
          if (timesToRun > 0) {
            latch.countDown()
          }

          if (latch.getCount == 0) {
            throw new TimeoutException(s"runSequentially: timed out based on maximal allowed repetitions ($timesToRun), aborting")
          }

          if (!stopCondition.apply(res)) {
            finalWork.apply(res)
            while (latch.getCount > 0) latch.countDown()
          } else {
            currentWaitTime = if (currentWaitTime + currentWaitTime > maxWaitTimeBetweenCalls) maxWaitTimeBetweenCalls else currentWaitTime + currentWaitTime
            t.schedule(new ExponentialBackoffTask(), currentWaitTime)
          }
        } catch {
          case exception: Exception =>
            while (latch.getCount > 0) latch.countDown()
            throw exception
        }
      }
    }

    val task: TimerTask = new ExponentialBackoffTask()
    t.schedule(task, delayBeforeStart)
    latch
  }

  def verifyAsyncCommandCompletion(client: Client,
                                   database: String,
                                   commandResult: KustoResultSetTable,
                                   samplePeriod: FiniteDuration = KCONST.DefaultPeriodicSamplePeriod,
                                   timeOut: FiniteDuration,
                                   doingWhat: String): Option[KustoResultSetTable] = {
    commandResult.next()
    val operationId = commandResult.getString(0)
    val operationsShowCommand = CslCommandsGenerator.generateOperationsShowCommand(operationId)
    val sampleInMillis = samplePeriod.toMillis.toInt
    val timeoutInMillis = timeOut.toMillis
    val delayPeriodBetweenCalls = if (sampleInMillis < 1) 1 else sampleInMillis
    val timesToRun = if (timeOut < FiniteDuration.apply(0, SECONDS)) -1 else (timeoutInMillis / delayPeriodBetweenCalls + 5).toInt

    val stateCol = "State"
    val statusCol = "Status"
    val statusCheck: () => Option[KustoResultSetTable] = () => {
      try {
        Some(client.execute(database, operationsShowCommand).getPrimaryResults)
      }
      catch {
        case e: DataServiceException => {
          if (e.isPermanent) {
            val message = s"Couldn't monitor the progress of the $doingWhat operation from the service, you may track" +
              s" it using the command '$operationsShowCommand'."
            logError("verifyAsyncCommandCompletion", message)
            throw new Exception(message, e)
          }
          logWarn("verifyAsyncCommandCompletion", "Failed transiently to retrieve export status, trying again in a few seconds")
          None
        }
        case _: DataClientException => None
      }
    }
    var lastResponse: Option[KustoResultSetTable] = None
    val task = doWhile[Option[KustoResultSetTable]](
      func = statusCheck,
      delayBeforeStart = 0, delayBeforeEach = delayPeriodBetweenCalls, timesToRun = timesToRun,
      stopCondition = (result: Option[KustoResultSetTable]) =>
        result.isEmpty || (result.get.next() && result.get.getString(stateCol) == "InProgress"),
      finalWork = (result: Option[KustoResultSetTable]) => {
        lastResponse = result
      }, maxWaitTimeBetweenCalls = ReadMaxWaitTime.toMillis.toInt)

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
        s"Failed to execute Kusto operation with OperationId '$operationId', State: '${lastResponse.get.getString(stateCol)}'," +
          s" Status: '${lastResponse.get.getString(statusCol)}'",
        lastResponse
      )
    }

    if (!success) {
      throw new TimeoutAwaitingPendingOperationException(s"Timed out while waiting for operation with OperationId '$operationId'")
    }

    lastResponse
  }

  private[kusto] def parseSas(url: String): KustoStorageParameters = {
    url match {
      case SasPattern(storageAccountId, cloud, container, sasKey) => KustoStorageParameters(storageAccountId, sasKey, container, secretIsAccountKey = false, cloud)
      case _ => throw new InvalidParameterException(
        "SAS url couldn't be parsed. Should be https://<storage-account>.blob.<domainEndpointSuffix>/<container>?<SAS-Token>"
      )
    }
  }

  private[kusto] def mergeKeyVaultAndOptionsAuthentication(paramsFromKeyVault: AadApplicationAuthentication,
                                                           authenticationParameters: Option[KustoAuthentication]): KustoAuthentication = {
    if (authenticationParameters.isEmpty) {
      // We have both keyVault and AAD application params, take from options first and throw if both are empty
      try {
        val app = authenticationParameters.asInstanceOf[AadApplicationAuthentication]
        AadApplicationAuthentication(
          ID = if (app.ID == "") {
            if (paramsFromKeyVault.ID == "") {
              throw new InvalidParameterException("AADApplication ID is empty. Please pass it in keyVault or options")
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
          authority = if (app.authority == "microsoft.com") paramsFromKeyVault.authority else app.authority
        )
      } catch {
        case _: ClassCastException => throw new UnsupportedOperationException("keyVault authentication can be combined only with AADAplicationAuthentication")
      }
    } else {
      paramsFromKeyVault
    }
  }

  private[kusto] def mergeKeyVaultAndOptionsStorageParams(storageAccount: Option[String],
                                                          storageContainer: Option[String],
                                                          storageSecret: Option[String],
                                                          storageSecretIsAccountKey: Boolean,
                                                          keyVaultAuthentication: KeyVaultAuthentication): Option[KustoStorageParameters] = {
    if (!storageSecretIsAccountKey) {
      // If SAS option defined - take sas
      Some(KustoDataSourceUtils.parseSas(storageSecret.get))
    } else {
      if (storageAccount.isEmpty || storageContainer.isEmpty || storageSecret.isEmpty) {
        val keyVaultParameters = KeyVaultUtils.getStorageParamsFromKeyVault(keyVaultAuthentication)
        // If KeyVault contains sas take it
        if (!keyVaultParameters.secretIsAccountKey) {
          Some(keyVaultParameters)
        } else {
          if ((storageAccount.isEmpty && keyVaultParameters.account.isEmpty) ||
            (storageContainer.isEmpty && keyVaultParameters.container.isEmpty) ||
            (storageSecret.isEmpty && keyVaultParameters.secret.isEmpty)) {

            // We don't have enough information to access blob storage
            None
          }
          else {
            // Try combine
            val account = if (storageAccount.isEmpty) Some(keyVaultParameters.account) else storageAccount
            val secret = if (storageSecret.isEmpty) Some(keyVaultParameters.secret) else storageSecret
            val container = if (storageContainer.isEmpty) Some(keyVaultParameters.container) else storageContainer

            getAndValidateTransientStorageParametersIfExist(account, container, secret, storageSecretIsAccountKey = true, None)
          }
        }
      } else {
        Some(KustoStorageParameters(storageAccount.get, storageSecret.get, storageContainer.get, storageSecretIsAccountKey))
      }
    }
  }

  private[kusto] def getAndValidateTransientStorageParametersIfExist(storageAccount: Option[String],
                                                                     storageContainer: Option[String],
                                                                     storageAccountSecret: Option[String],
                                                                     storageSecretIsAccountKey: Boolean,
                                                                     domainPostfix: Option[String])
  : Option[KustoStorageParameters] = {

    val paramsFromSas = if (!storageSecretIsAccountKey && storageAccountSecret.isDefined) {
      if (storageAccountSecret.get == null) {
        throw new InvalidParameterException("storage secret from parameters is null")
      }
      Some(parseSas(storageAccountSecret.get))
    } else None

    if (paramsFromSas.isDefined) {
      if (storageAccount.isDefined && !storageAccount.get.equals(paramsFromSas.get.account)) {
        throw new InvalidParameterException("Storage account name does not match the name in storage access SAS key.")
      }

      if (storageContainer.isDefined && !storageContainer.get.equals(paramsFromSas.get.container)) {
        throw new InvalidParameterException("Storage container name does not match the name in storage access SAS key.")
      }

      paramsFromSas
    }
    else if (storageAccount.isDefined && storageContainer.isDefined && storageAccountSecret.isDefined) {
      if (storageAccount.get == null || storageAccountSecret.get == null || storageContainer.get == null) {
        throw new InvalidParameterException("storageAccount key from parameters is null")
      }
      val postfix = domainPostfix.getOrElse(DefaultDomainPostfix)
      Some(KustoStorageParameters(storageAccount.get, storageAccountSecret.get, storageContainer.get, storageSecretIsAccountKey, postfix))
    }
    else None
  }

  private[kusto] def countRows(client: Client, query: String, database: String, crp: ClientRequestProperties): Int = {
    val res = client.execute(database, generateCountQuery(query), crp).getPrimaryResults
    res.next()
    res.getInt(0)
  }

  private[kusto] def estimateRowsCount(client: Client, query: String, database: String, crp: ClientRequestProperties): Int = {
    var count = 0
    val estimationResult: util.List[AnyRef] = Await.result(Future {
      val res = client.execute(database, generateEstimateRowsCountQuery(query), crp).getPrimaryResults
      res.next()
      res.getCurrentRow
    }, KustoConstants.TimeoutForCountCheck)
    if (StringUtils.isBlank(estimationResult.get(1).toString)) {
      // Estimation can be empty for certain cases
      Await.result(Future {
        val res = client.execute(database, generateCountQuery(query), crp).getPrimaryResults
        res.next()
        res.getInt(0)
      }, KustoConstants.TimeoutForCountCheck)
    } else {
      // Zero estimation count does not indicate zero results, therefore we add 1 here so that we won't return an empty RDD
      count = estimationResult.get(1).asInstanceOf[Int] + 1
    }

    count
  }

}

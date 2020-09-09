package com.microsoft.kusto.spark.utils

import java.io.InputStream
import java.net.URI
import java.security.InvalidParameterException
import java.util
import java.util.concurrent.{Callable, CountDownLatch, TimeUnit, TimeoutException}
import java.util.{NoSuchElementException, StringJoiner, Timer, TimerTask, UUID}

import com.microsoft.azure.kusto.data.{Client, ClientRequestProperties, KustoResultSetTable}
import com.microsoft.azure.kusto.data.exceptions.{DataClientException, DataServiceException}
import com.microsoft.kusto.spark.authentication._
import com.microsoft.kusto.spark.common.{KustoCoordinates, KustoDebugOptions}
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SinkTableCreationMode, WriteOptions}
import com.microsoft.kusto.spark.datasource.{KustoResponseDeserializer, KustoSchema, KustoSourceOptions, KustoStorageParameters}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoConstants => KCONST}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import java.util.Properties

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.matching.Regex
import org.apache.commons.lang3.StringUtils
import org.apache.http.client.utils.URIBuilder
import org.json.JSONObject

object KustoDataSourceUtils {
  private val klog = Logger.getLogger("KustoConnector")

  val sasPattern: Regex = raw"(?:https://)?([^.]+).blob.([^/]+)/([^?]+)?(.+)".r

  val NewLine = sys.props("line.separator")
  val MaxWaitTime: FiniteDuration = 1 minute

  val input: InputStream = getClass.getClassLoader.getResourceAsStream("spark.kusto.properties")
  val props = new Properties( )
  props.load(input)
  var version: String = props.getProperty("application.version")
  var clientName = s"Kusto.Spark.Connector:$version"
  val ingestPrefix: String = props.getProperty("ingestPrefix","ingest-")
  val enginePrefix: String = props.getProperty("enginePrefix","https://")
  val defaultDomainPostfix: String = props.getProperty("defaultDomainPostfix","core.windows.net")
  val defaultClusterSuffix: String = props.getProperty("defaultClusterSuffix","kusto.windows.net")
  val ariaClustersProxy: String = props.getProperty("ariaClustersProxy", "https://kusto.aria.microsoft.com")
  val ariaClustersAlias: String = "Aria proxy"

  def setLoggingLevel(level: String): Unit = {
    setLoggingLevel(Level.toLevel(level))
  }

  def setLoggingLevel(level: Level): Unit = {
    Logger.getLogger("KustoConnector").setLevel(level)
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

  private[kusto] def extractSchemaFromResultTable(resultRows: util.ArrayList[util.ArrayList[String]]): String = {

    val tableSchemaBuilder = new StringJoiner(",")

    for (row <- resultRows) {
      // Each row contains {Name, CslType, Type}, converted to (Name:CslType) pairs
      tableSchemaBuilder.add(s"['${row.get(0)}']:${row.get(1)}")
    }

    tableSchemaBuilder.toString
  }

  private[kusto] def getSchema(database: String, query: String, client: Client): KustoSchema = {
    KustoResponseDeserializer(client.execute(database, query).getPrimaryResults).getSchema
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
    var authentication: KustoAuthentication = null
    var keyVaultAuthentication: Option[KeyVaultAuthentication] = None

    // Check KeyVault Authentication
    if (keyVaultUri != "") {
      if (!keyVaultAppId.isEmpty) {
        keyVaultAuthentication = Some(KeyVaultAppAuthentication(keyVaultUri,
          keyVaultAppId,
          keyVaultAppKey))
      } else {
        keyVaultAuthentication = Some(KeyVaultCertificateAuthentication(keyVaultUri,
          keyVaultPemFile,
          keyVaultCertKey))
      }
    }

    // Look for conflicts
    var numberOfAuthenticationMethods = 0
    if (!applicationId.isEmpty) numberOfAuthenticationMethods += 1
    if (!accessToken.isEmpty) numberOfAuthenticationMethods += 1
    if (!tokenProviderCoordinates.isEmpty) numberOfAuthenticationMethods += 1
    if (!keyVaultUri.isEmpty) numberOfAuthenticationMethods += 1
    if(numberOfAuthenticationMethods > 1){
      throw new IllegalArgumentException("More than one authentication methods were provided. Failing.")
    }

    // Resolve authentication
    if (!applicationId.isEmpty) {
      // Application authentication
      if(!applicationKey.isEmpty){
        authentication = AadApplicationAuthentication(applicationId, applicationKey, parameters.getOrElse(KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com"))
      } else if(!applicationCertPath.isEmpty){
        authentication = AadApplicationCertificateAuthentication(applicationId, applicationCertPath, applicationCertPassword)
      }
    } else if (!accessToken.isEmpty) {
      // Authentication by token
      authentication = KustoAccessTokenAuthentication(accessToken)
    }  else if (!tokenProviderCoordinates.isEmpty) {
      // Authentication by token provider
      val classLoader = Thread.currentThread().getContextClassLoader
      val c1 = classLoader.loadClass(tokenProviderCoordinates).getConstructor(parameters.getClass).newInstance(parameters)
      val tokenProviderCallback = c1.asInstanceOf[Callable[String]]

      authentication = KustoTokenProviderAuthentication(tokenProviderCallback)
    } else if (keyVaultUri.isEmpty) {
      logWarn("parseSourceParameters", "No authentication method was not supplied - using device authentication")
      val token = DeviceAuthentication.acquireAccessTokenUsingDeviceCodeFlow(clusterUrl)
      authentication = KustoAccessTokenAuthentication(token)
    }

    SourceParameters(authentication, KustoCoordinates(clusterUrl, alias, database.get, table), keyVaultAuthentication)
  }

  case class SinkParameters(writeOptions: WriteOptions, sourceParametersResults: SourceParameters)

  case class SourceParameters(authenticationParameters: KustoAuthentication, kustoCoordinates: KustoCoordinates, keyVaultAuth: Option[KeyVaultAuthentication])

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
    try {
      isAsyncParam = parameters.getOrElse(KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC, "false")
      isAsync = parameters.getOrElse(KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC, "false").trim.toBoolean
      tableCreationParam = parameters.get(KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS)
      tableCreation = if (tableCreationParam.isEmpty) SinkTableCreationMode.FailIfNotExist else SinkTableCreationMode.withName(tableCreationParam.get)
      batchLimit = parameters.getOrElse(KustoSinkOptions.KUSTO_CLIENT_BATCHING_LIMIT, "100").trim.toInt
    } catch {
      case _: NoSuchElementException => throw new InvalidParameterException(s"No such SinkTableCreationMode option: '${tableCreationParam.get}'")
      case _: java.lang.IllegalArgumentException => throw new InvalidParameterException(s"KUSTO_WRITE_ENABLE_ASYNC is expecting either 'true' or 'false', got: '$isAsyncParam'")
    }

    val timeout = new FiniteDuration(parameters.getOrElse(KustoSinkOptions.KUSTO_TIMEOUT_LIMIT, KCONST.defaultWaitingIntervalLongRunning).toLong, TimeUnit.SECONDS)
    val operationId = parameters.getOrElse(KustoSinkOptions.KUSTO_OPERATION_ID, UUID.randomUUID().toString)

    val ingestionPropertiesAsJson = parameters.get(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON)

    val writeOptions = WriteOptions(
      tableCreation,
      isAsync,
      parameters.getOrElse(KustoSinkOptions.KUSTO_WRITE_RESULT_LIMIT, "1"),
      parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, "UTC"),
      timeout,
      ingestionPropertiesAsJson,
      batchLimit,
      operationId
    )

    val sourceParameters = parseSourceParameters(parameters)

    if (sourceParameters.kustoCoordinates.table.isEmpty) {
      throw new InvalidParameterException("KUSTO_TABLE parameter is missing. Must provide a destination table name")
    }

    logInfo("parseSinkParameters", s"Parsed write options for sink: {'timeout': ${writeOptions.timeout}, 'async': ${writeOptions.isAsync}, " +
      s"'tableCreationMode': ${writeOptions.tableCreateOptions}, 'writeLimit': ${writeOptions.writeResultLimit}, 'batchLimit': ${writeOptions.batchLimit}" +
      s", 'timeout': ${writeOptions.timeout}, 'timezone': ${writeOptions.timeZone}, 'ingestionProperties': $ingestionPropertiesAsJson, operationId: $operationId}")

    SinkParameters(writeOptions, sourceParameters)
  }

  def getClientRequestProperties(parameters: Map[String, String]): ClientRequestProperties = {
    val crpOption = parameters.get(KustoSourceOptions.KUSTO_CLIENT_REQUEST_PROPERTIES_JSON)

    if (crpOption.isDefined) {
      val crp = ClientRequestProperties.fromString(crpOption.get)
      crp
    } else {
      new ClientRequestProperties
    }
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
    if (cluster.equals(ariaClustersProxy)){
      ariaClustersAlias
    }
    else if (cluster.startsWith(enginePrefix) ){
      val host = new URI(cluster).getHost
      val startIdx = if (host.startsWith(ingestPrefix)) ingestPrefix.length else 0
      host.substring(startIdx,host.indexOf(".kusto."))
    } else {
      cluster
    }
  }

  private[kusto] def getEngineUrlFromAliasIfNeeded(cluster: String): String = {
    if(cluster.startsWith(enginePrefix)){
      val host = new URI(cluster).getHost
      if(host.startsWith(ingestPrefix)){
        val uriBuilder = new URIBuilder()
        uriBuilder.setHost(ingestPrefix + host).toString
      } else{
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
    * @param func               - the function to run
    * @param delayBeforeStart              - delay before first job
    * @param delayBeforeEach           - delay between jobs
    * @param timesToRun - stop jobs after numberOfTimesToRun.
    *                           set negative value to run infinitely
    * @param stopCondition            - stop jobs if condition holds for the func.apply output
    * @param finalWork          - do final work with the last func.apply output
    */
    def doWhile[A](func: () => A, delayBeforeStart: Long, delayBeforeEach: Int, timesToRun: Int, stopCondition: A => Boolean, finalWork: A => Unit): CountDownLatch = {
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
            currentWaitTime = if (currentWaitTime > MaxWaitTime.toMillis) currentWaitTime else currentWaitTime + currentWaitTime
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
                                   directory: String,
                                   samplePeriod: FiniteDuration = KCONST.defaultPeriodicSamplePeriod,
                                   timeOut: FiniteDuration): Unit = {
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
      catch{
        case e: DataServiceException => {
          val error = new JSONObject(e.getCause.getMessage).getJSONObject("error")
          val isPermanent = error.getBoolean("@permanent")
          if (isPermanent) {
            val message = s"Couldn't monitor the progress of the export command from the service, operationId:$operationId" +
              s"and read from the blob directory: ('$directory'), once it completes."
            logError("verifyAsyncCommandCompletion", message)
            throw new Exception(message, e)
          }
          logWarn("verifyAsyncCommandCompletion", "Failed transiently to retrieve export status, trying again in a few seconds")
          None
        }
        case _: DataClientException => None
      }}
    var lastResponse: Option[KustoResultSetTable] = None
    val task = doWhile[Option[KustoResultSetTable]](
      func = statusCheck,
      delayBeforeStart = 0, delayBeforeEach = delayPeriodBetweenCalls, timesToRun = timesToRun,
      stopCondition = (result: Option[KustoResultSetTable]) =>
        result.isEmpty || (result.get.next() && result.get.getString(stateCol) == "InProgress"),
      finalWork = (result: Option[KustoResultSetTable]) => {
        lastResponse = result
      })
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
      throw new RuntimeException(
        s"Failed to execute Kusto operation with OperationId '$operationId', State: '${lastResponse.get.getString(stateCol)}'," +
          s" Status: '${lastResponse.get.getString(statusCol)}'"
      )
    }

    if (!success) {
      throw new RuntimeException(s"Timed out while waiting for operation with OperationId '$operationId'")
    }
  }

  private[kusto] def parseSas(url: String): KustoStorageParameters = {
    url match {
      case sasPattern(storageAccountId, cloud, container, sasKey) => KustoStorageParameters(storageAccountId, sasKey, container, secretIsAccountKey = false, cloud)
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
      val postfix = domainPostfix.getOrElse(defaultDomainPostfix)
      Some(KustoStorageParameters(storageAccount.get, storageAccountSecret.get, storageContainer.get, storageSecretIsAccountKey, postfix))
    }
    else None
  }

  private[kusto] def countRows(client: Client, query: String, database: String): Int = {
    val res =client.execute(database, generateCountQuery(query)).getPrimaryResults
    res.next()
    res.getInt(0)
  }

  private[kusto] def estimateRowsCount(client: Client, query: String, database: String): Int = {
    var count = 0
    val estimationResult: util.ArrayList[AnyRef] = Await.result(Future {
      val res = client.execute(database, generateEstimateRowsCountQuery(query)).getPrimaryResults
      res.next()
      res.getCurrentRow
    }, KustoConstants.timeoutForCountCheck)
    if(StringUtils.isBlank(estimationResult.get(1).toString)){
      // Estimation can be empty for certain cases
      Await.result(Future{
        val res = client.execute(database, generateCountQuery(query)).getPrimaryResults
          res.next()
          res.getInt(0)
      }, KustoConstants.timeoutForCountCheck)
    } else {
      // Zero estimation count does not indicate zero results, therefore we add 1 here so that we won't return an empty RDD
      count = estimationResult.get(1).asInstanceOf[Int] + 1
    }

    count
  }

}

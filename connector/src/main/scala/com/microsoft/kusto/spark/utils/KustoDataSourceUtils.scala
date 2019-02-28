package com.microsoft.kusto.spark.utils

import java.security.InvalidParameterException
import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}
import java.util.{NoSuchElementException, StringJoiner, Timer, TimerTask}

import com.microsoft.azure.kusto.data.{Client, Results}
import com.microsoft.kusto.spark.datasource.{KeyVaultAuthentication, KustoAuthentication, KustoCoordinates, WriteOptions, _}
import com.microsoft.kusto.spark.datasource.KustoOptions.SinkTableCreationMode
import com.microsoft.kusto.spark.datasource.KustoOptions.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.StructType
import org.json4s.jackson.JsonMethods.parse

import scala.util.matching.Regex
import scala.concurrent.duration._

object KustoDataSourceUtils{
  private val klog = Logger.getLogger("KustoConnector")

  val ClientName = "Kusto.Spark.Connector"
  val DefaultTimeoutLongRunning: FiniteDuration = 90 minutes
  val DefaultPeriodicSamplePeriod: FiniteDuration = 2 seconds

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

  def createTmpTableWithSameSchema(kustoAdminClient: Client,
                                   tableCoordinates: KustoCoordinates,
                                   tmpTableName: String,
                                   tableCreation: SinkTableCreationMode = SinkTableCreationMode.FailIfNotExist,
                                   schema: StructType): Unit = {
    val schemaShowCommandResult = kustoAdminClient.execute(tableCoordinates.database, generateTableShowSchemaCommand(tableCoordinates.table.get)).getValues
    var tmpTableSchema: String = ""
    val tableSchemaBuilder = new StringJoiner(",")

    if (schemaShowCommandResult.size() == 0){
      // Table Does not exist
      if(tableCreation == SinkTableCreationMode.FailIfNotExist){
        throw new RuntimeException(s"Table '${tableCoordinates.table}' doesn't exist in database '$tableCoordinates.database', in cluster '$tableCoordinates.cluster'")
      } else {
        // Parse dataframe schema and create a destination table with that schema
        for(field <- schema){
          val fieldType =  DataTypeMapping.getSparkTypeToKustoTypeMap(field.dataType)
          tableSchemaBuilder.add(s"${field.name}:$fieldType")
        }
        tmpTableSchema = tableSchemaBuilder.toString
        kustoAdminClient.execute(tableCoordinates.database, generateTableCreateCommand(tableCoordinates.table.get, tmpTableSchema))
      }
    } else {
      // Table exists. Parse kusto table schema and check if it matches the dataframes schema
      val orderedColumns = parse(schemaShowCommandResult.get(0).get(1)) \ "OrderedColumns"
      for (col <- orderedColumns.children) {
        tableSchemaBuilder.add(s"${(col \ "Name").values}:${(col \ "CslType").values}")
      }
      tmpTableSchema = tableSchemaBuilder.toString
    }

    //  Create a temporary table with the kusto or dataframe parsed schema
    kustoAdminClient.execute(tableCoordinates.database, generateTableCreateCommand(tmpTableName, tmpTableSchema))
  }

  def parseSourceParameters(parameters: Map[String,String]): SourceParameters = {
    // Parse KustoTableCoordinates - these are mandatory options
    val database  = parameters.get(KustoOptions.KUSTO_DATABASE)
    val cluster = parameters.get(KustoOptions.KUSTO_CLUSTER)

    if (database.isEmpty){
      throw new InvalidParameterException("KUSTO_DATABASE parameter is missing. Must provide a destination database name")
    }

    if (cluster.isEmpty){
      throw new InvalidParameterException("KUSTO_CLUSTER parameter is missing. Must provide a destination cluster name")
    }

    val table = parameters.get(KustoOptions.KUSTO_TABLE)

    // Parse KustoAuthentication
    val applicationId = parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_ID, "")
    val applicationKey = parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, "")
    var authentication: KustoAuthentication = null
    val keyVaultUri: String = parameters.getOrElse(KustoOptions.KEY_VAULT_URI, "")
    var accessToken: String = ""
    var keyVaultAuthentication: Option[KeyVaultAuthentication] = None
    if(keyVaultUri != ""){
      // KeyVault Authentication
      val keyVaultAppId: String = parameters.getOrElse(KustoOptions.KEY_VAULT_APP_ID, "")

      if(!keyVaultAppId.isEmpty){
        keyVaultAuthentication = Some(KeyVaultAppAuthentication(keyVaultUri,
          keyVaultAppId,
          parameters.getOrElse(KustoOptions.KEY_VAULT_APP_KEY, "")))
      } else {
        keyVaultAuthentication = Some(KeyVaultCertificateAuthentication(keyVaultUri,
          parameters.getOrElse(KustoOptions.KEY_VAULT_PEM_FILE_PATH, ""),
          parameters.getOrElse(KustoOptions.KEY_VAULT_CERTIFICATE_KEY, "")))
      }
    }

    if(!applicationId.isEmpty || !applicationKey.isEmpty){
      authentication = AadApplicationAuthentication(applicationId, applicationKey, parameters.getOrElse(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com"))
    }
    else if ({accessToken = parameters.getOrElse(KustoOptions.KUSTO_ACCESS_TOKEN, "")
      !accessToken.isEmpty}){
      authentication = KustoAccessTokenAuthentication(accessToken)
    } else if (keyVaultUri.isEmpty){
      authentication = KustoAccessTokenAuthentication(DeviceAuthentication.acquireAccessTokenUsingDeviceCodeFlow(cluster.get))
    }

    SourceParameters(authentication, KustoCoordinates(getClusterNameFromUrlIfNeeded(cluster.get), database.get, table), keyVaultAuthentication)
  }

  case class SinkParameters(writeOptions: WriteOptions, sourceParametersResults: SourceParameters)

  case class SourceParameters(authenticationParameters: KustoAuthentication, kustoCoordinates: KustoCoordinates, keyVaultAuth: Option[KeyVaultAuthentication])

  def parseSinkParameters(parameters: Map[String,String], mode: SaveMode = SaveMode.Append): SinkParameters = {
    if(mode != SaveMode.Append)
    {
      if (mode == SaveMode.ErrorIfExists){
        logInfo(ClientName, s"Kusto data source supports only append mode. Ignoring 'ErrorIfExists' directive")
      } else {
        throw new InvalidParameterException(s"Kusto data source supports only append mode. '$mode' directive is invalid")
      }
    }

    // Parse WriteOptions
    var tableCreation: SinkTableCreationMode = SinkTableCreationMode.FailIfNotExist
    var tableCreationParam: Option[String] = None
    var isAsync: Boolean = false
    var isAsyncParam : String = ""

    try {
      isAsyncParam = parameters.getOrElse(KustoOptions.KUSTO_WRITE_ENABLE_ASYNC, "false")
      isAsync =  parameters.getOrElse(KustoOptions.KUSTO_WRITE_ENABLE_ASYNC, "false").trim.toBoolean
      tableCreationParam = parameters.get(KustoOptions.KUSTO_TABLE_CREATE_OPTIONS)
      tableCreation = if(tableCreationParam.isEmpty) SinkTableCreationMode.FailIfNotExist else SinkTableCreationMode.withName(tableCreationParam.get)
    } catch {
      case _ : NoSuchElementException => throw new InvalidParameterException(s"No such SinkTableCreationMode option: '${tableCreationParam.get}'")
      case _ : java.lang.IllegalArgumentException  => throw new InvalidParameterException(s"KUSTO_WRITE_ENABLE_ASYNC is expecting either 'true' or 'false', got: '$isAsyncParam'")
    }
    val writeOptions = WriteOptions(tableCreation, isAsync, parameters.getOrElse(KustoOptions.KUSTO_WRITE_RESULT_LIMIT, "1"), parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, "UTC"))
    val sourceParameters = parseSourceParameters(parameters)

    if (sourceParameters.kustoCoordinates.table.isEmpty){
      throw new InvalidParameterException("KUSTO_TABLE parameter is missing. Must provide a destination table name")
    }
    SinkParameters(writeOptions, sourceParameters)
  }

  private [kusto] def reportExceptionAndThrow(
    reporter: String,
    exception: Exception,
    doingWhat: String = "",
    cluster: String = "",
    database: String = "",
    table: String = "",
    isLogDontThrow: Boolean = false) : Unit = {
    val whatFailed = if (doingWhat.isEmpty) "" else s"when $doingWhat"
    val clusterDesc = if (cluster.isEmpty) "" else s", cluster: '$cluster' "
    val databaseDesc = if (database.isEmpty) "" else s", database: '$database'"
    val tableDesc = if (table.isEmpty) "" else s", table: '$table'"
    logError(reporter,s"caught exception $whatFailed$clusterDesc$databaseDesc$tableDesc. Exception: ${exception.getMessage}")

    if (!isLogDontThrow) throw exception
  }

  private def getClusterNameFromUrlIfNeeded(url: String): String = {
    val urlPattern: Regex = raw"https://(?:ingest-)?([^.]+).kusto.windows.net(?::443)?".r
    url match {
      case urlPattern(clusterAlias) => clusterAlias
      case _ => url
    }
  }

  /**
    * A function to run sequentially async work on TimerTask using a Timer.
    * The function passed is scheduled sequentially by the timer, until last calculated returned value by func does not
    * satisfy the condition of doWhile or a given number of times has passed.
    * After one of these conditions was held true the finalWork function is called over the last returned value by func.
    * Returns a CountDownLatch object use to countdown times and await on it synchronously if needed
    *
    * @param func - the function to run
    * @param delay - delay before first job
    * @param runEvery - delay between jobs
    * @param numberOfTimesToRun - stop jobs after numberOfTimesToRun.
    *                            set negative value to run infinitely
    * @param doWhile - stop jobs if condition holds for the func.apply output
    * @param finalWork - do final work with the last func.apply output
    */
  def runSequentially[A](func: () => A, delay: Int, runEvery: Int, numberOfTimesToRun: Int, doWhile: A => Boolean, finalWork: A => Unit): CountDownLatch = {
    val latch = new CountDownLatch(if (numberOfTimesToRun > 0) numberOfTimesToRun else 1)
    val t = new Timer()
    val task = new TimerTask() {
      def run(): Unit = {
        val res = func.apply()
        if(numberOfTimesToRun > 0){
          latch.countDown()
        }

        if (latch.getCount == 0)
        {
          throw new TimeoutException(s"runSequentially: Reached maximal allowed repetitions ($numberOfTimesToRun), aborting")
        }

        if (!doWhile.apply(res)){
          t.cancel()
          finalWork.apply(res)
          while (latch.getCount > 0){
            latch.countDown()
          }
        }
      }
    }
    t.schedule(task, delay, runEvery)
    latch
  }

  def verifyAsyncCommandCompletion(client: Client, database: String, commandResult: Results, samplePeriod: FiniteDuration = DefaultPeriodicSamplePeriod, timeOut: FiniteDuration = DefaultTimeoutLongRunning): Unit = {
    val operationId = commandResult.getValues.get(0).get(0)
    val operationsShowCommand = CslCommandsGenerator.generateOperationsShowCommand(operationId)
    val sampleInMillis = samplePeriod.toMillis.toInt
    val timeoutInMillis = timeOut.toMillis
    val delayPeriodBetweenCalls = if (sampleInMillis < 1) 1 else sampleInMillis
    val timesToRun = (timeoutInMillis/ delayPeriodBetweenCalls + 5).toInt

    val stateCol = "State"
    val statusCol = "Status"
    val showCommandResult = client.execute(database, operationsShowCommand)
    val stateIdx = showCommandResult.getColumnNameToIndex.get(stateCol)
    val statusIdx = showCommandResult.getColumnNameToIndex.get(statusCol)

    val success = runSequentially[util.ArrayList[String]](
      func = () => client.execute(database, operationsShowCommand).getValues.get(0), delay = 0, runEvery = delayPeriodBetweenCalls, numberOfTimesToRun = timesToRun,
      doWhile = result => {
        result.get(stateIdx) == "InProgress"
      },
      finalWork = result => {
        if (result.get(stateIdx) != "Completed") {
          throw new RuntimeException(s"Failed to execute Kusto operation with OperationId '$operationId', State: '${result.get(stateIdx)}', Status: '${result.get(statusIdx)}'")
        }
      }).await(timeoutInMillis, TimeUnit.MILLISECONDS)

    if (!success) {
      throw new RuntimeException(s"Timed out while waiting for operation with OperationId '$operationId'")
    }
  }

  private [kusto] def parseSas(url: String): KustoStorageParameters  = {
    val urlPattern: Regex = raw"(?:https://)?([^.]+).blob.core.windows.net/([^?]+)?(.+)".r
    url match {
      case urlPattern(storageAccountId, container, sasKey) => KustoStorageParameters(storageAccountId, sasKey, container, storageSecretIsAccountKey = false)
      case _ => throw new InvalidParameterException("SAS url couldn't be parsed. Should be https://<storage-account>.blob.core.windows.net/<container>?<SAS-Token>")
    }
  }

  private [kusto] def mergeKeyVaultAndOptionsAuthentication(paramsFromKeyVault: AadApplicationAuthentication, authenticationParameters: Option[KustoAuthentication]): KustoAuthentication = {
    if(authenticationParameters.isEmpty){
      // We have both keyVault and AAD application params, take from options first and throw if both are empty
      try{
        val app = authenticationParameters.asInstanceOf[AadApplicationAuthentication]
        AadApplicationAuthentication(
          ID = if (app.ID == "") {
            if (paramsFromKeyVault.ID == "")
              throw new InvalidParameterException("AADApplication ID is empty. Please pass it in keyVault or options")
            paramsFromKeyVault.ID
          } else app.ID,
          password = if (app.password == "") {
            if (paramsFromKeyVault.password == "AADApplication key is empty. Please pass it in keyVault or options")
              throw new InvalidParameterException("")
            paramsFromKeyVault.password
          } else app.password,
          authority = if (app.authority == "microsoft.com") paramsFromKeyVault.authority else app.authority
        )
      } catch {
        case _: ClassCastException => throw new UnsupportedOperationException("keyVault authentication can be combined only with AADAplicationAuthentication")
      }
    } else {
      paramsFromKeyVault
    }
  }

  private [kusto] def mergeKeyVaultAndOptionsStorageParams(storageAccount: Option[String],
                                                           storageContainer: Option[String],
                                                           storageSecret: Option[String],
                                                           storageSecretIsAccountKey: Boolean,
                                                           keyVaultAuthentication: KeyVaultAuthentication): KustoStorageParameters = {
    if(!storageSecretIsAccountKey){
      // If SAS option defined - take sas
      KustoDataSourceUtils.parseSas(storageSecret.get)
    } else {
      if (storageAccount.isEmpty || storageContainer.isEmpty || storageSecret.isEmpty){
        val keyVaultParameters = KeyVaultUtils.getStorageParamsFromKeyVault(keyVaultAuthentication)
        // If KeyVault contains sas take it
        if(!keyVaultParameters.storageSecretIsAccountKey) {
          keyVaultParameters
        } else {
          // Try combine

          val account = if(storageAccount.isEmpty){
            Some(keyVaultParameters.account)
          } else storageAccount
          val secret = if(storageSecret.isEmpty){
            Some(keyVaultParameters.secret)
          } else storageSecret
          val container = if(storageContainer.isEmpty){
            Some(keyVaultParameters.container)
          } else storageContainer

          getAndValidateTransientStorageParameters(account, secret, container, storageSecretIsAccountKey = true)
        }
      } else {
        KustoStorageParameters(storageAccount.get, storageSecret.get, storageContainer.get, storageSecretIsAccountKey)
      }
    }
  }

  private[kusto] def getAndValidateTransientStorageParameters(storageAccount: Option[String],
                                                       storageContainer: Option[String],
                                                       storageAccountSecret: Option[String],
                                                       storageSecretIsAccountKey: Boolean): KustoStorageParameters = {
    if (storageAccount.isEmpty) {
      throw new InvalidParameterException("Storage account name is empty. Reading in 'Scale' mode requires a transient blob storage")
    }

    if (storageContainer.isEmpty) {
      throw new InvalidParameterException("Storage container name is empty.")
    }

    if (storageAccountSecret.isEmpty) {
      throw new InvalidParameterException("Storage account secret is empty. Please provide a storage account key or a SAS key")
    }

    KustoStorageParameters(storageAccount.get, storageAccountSecret.get, storageContainer.get, storageSecretIsAccountKey)
  }

  private[kusto] def countRows(client: Client, query: String, database: String): Int = {
    client.execute(database, generateCountQuery(query)).getValues.get(0).get(0).toInt
  }
}

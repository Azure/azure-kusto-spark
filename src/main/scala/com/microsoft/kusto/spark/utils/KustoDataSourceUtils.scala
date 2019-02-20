package com.microsoft.kusto.spark.utils

import java.security.InvalidParameterException
import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}
import java.util.{NoSuchElementException, StringJoiner, Timer, TimerTask}

import com.microsoft.azure.kusto.data.{Client, Results}
import com.microsoft.kusto.spark.datasource._
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
    val schemaShowCommandResult = kustoAdminClient.execute(tableCoordinates.database, generateTableShowSchemaCommand(tableCoordinates.table)).getValues
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
        kustoAdminClient.execute(tableCoordinates.database, generateTableCreateCommand(tableCoordinates.table, tmpTableSchema))
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

  def parseSinkParameters(parameters: Map[String,String], mode : SaveMode = SaveMode.Append): (WriteOptions, KustoAuthentication, KustoCoordinates) = {
    if(mode != SaveMode.Append)
    {
      if (mode == SaveMode.ErrorIfExists){
        logInfo(ClientName, s"Kusto data source supports only append mode. Ignoring 'ErrorIfExists' directive")
      } else {
        throw new InvalidParameterException(s"Kusto data source supports only append mode. '$mode' directive is invalid")
      }
    }

    var tableCreation: SinkTableCreationMode = SinkTableCreationMode.FailIfNotExist
    var tableCreationParam: Option[String] = None
    var isAsync: Boolean = false
    var isAsyncParam : String = ""

    // Parse KustoTableCoordinates - these are mandatory options
    val table = parameters.get(KustoOptions.KUSTO_TABLE)
    val database  = parameters.get(KustoOptions.KUSTO_DATABASE)
    var cluster = parameters.get(KustoOptions.KUSTO_CLUSTER)

    if (table.isEmpty){
        throw new InvalidParameterException("KUSTO_TABLE parameter is missing. Must provide a destination table name")
    }

    if (database.isEmpty){
      throw new InvalidParameterException("KUSTO_DATABASE parameter is missing. Must provide a destination database name")
    }

    if (cluster.isEmpty){
      throw new InvalidParameterException("KUSTO_CLUSTER parameter is missing. Must provide a destination cluster name")
    }

    // Parse WriteOptions
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

    // Parse KustoAuthentication
    val applicationId = parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_ID, "")
    var authentication: KustoAuthentication = null
    var keyVaultUri: String = null
    var userToken: String = null
    var keyVaultCertFilePath: String = null

    if(applicationId != ""){
      authentication = AadApplicationAuthentication(applicationId, parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, ""), parameters.getOrElse(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com"))
    }
    else if({
      keyVaultUri = parameters.getOrElse(KustoOptions.KEY_VAULT_URI, "")
      keyVaultUri != ""}){
      // KeyVault Authentication
      var keyVaultAppId: String = null

      if({keyVaultAppId = parameters.getOrElse(KustoOptions.KEY_VAULT_APP_ID, "")
          keyVaultAppId != ""}){
          authentication = KeyVaultAppAuthentication(keyVaultUri,
           keyVaultAppId,
           parameters.getOrElse(KustoOptions.KEY_VAULT_APP_KEY, ""))
      }
      else {
        authentication = KeyVaultCertificateAuthentication(keyVaultUri,
          parameters.getOrElse(KustoOptions.KEY_VAULT_PEM_FILE_PATH, ""),
          parameters.getOrElse(KustoOptions.KEY_VAULT_CERTIFICATE_KEY, ""))
        }
      }
    else if ({userToken = parameters.getOrElse(KustoOptions.KUSTO_USER_TOKEN, "")
        userToken != ""}){
      authentication = KustoUserTokenAuthentication(userToken)
    }
    else {
      authentication = KustoUserTokenAuthentication(DeviceAuthentication.acquireAccessTokenUsingDeviceCodeFlow(cluster.get))
    }

    (writeOptions, authentication, KustoCoordinates(getClusterNameFromUrlIfNeeded(cluster.get), database.get, table.get))
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
}


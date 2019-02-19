package com.microsoft.kusto.spark.utils

import java.security.InvalidParameterException
import java.util.{NoSuchElementException, StringJoiner}

import com.microsoft.azure.kusto.data.Client
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

object KustoDataSourceUtils{
  private val klog = Logger.getLogger("KustoConnector")

  val ClientName = "Kusto.Spark.Connector"

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
                                   tableCoordinates: KustoTableCoordinates,
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

  def parseSinkParameters(parameters: Map[String,String], mode : SaveMode = SaveMode.Append): (WriteOptions, KustoAuthentication, KustoTableCoordinates) = {
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
          authentication = KeyVaultAppAuthentiaction(keyVaultUri,
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

    (writeOptions, authentication, KustoTableCoordinates(getClusterNameFromUrlIfNeeded(cluster.get), database.get, table.get))
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
}


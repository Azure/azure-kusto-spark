package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.{NoSuchElementException, StringJoiner}

import com.microsoft.azure.kusto.data.Client
import com.microsoft.kusto.spark.datasink.KustoWriter.TempIngestionTablePrefix
import com.microsoft.kusto.spark.datasource.KustoOptions.SinkTableCreationMode
import com.microsoft.kusto.spark.datasource.KustoOptions.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.utils.DataTypeMapping
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.StructType
import org.json4s.native.JsonMethods.parse

object KustoDataSourceUtils{
  private val klog = Logger.getLogger("KustoConnector")

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


  def createTmpTableWithSameSchema(kustoAdminClient: Client, table: String, database:String, tmpTableName: String, cluster: String,
                                   tableCreation: SinkTableCreationMode = SinkTableCreationMode.FailIfNotExist, schema: StructType
                                  ): Unit = {
    val schemaShowCommandResult = kustoAdminClient.execute(database,generateTableShowSchemaCommand(table)).getValues
    var tmpTableSchema: String = ""
    val tableSchemaBuilder = new StringJoiner(",")

    if (schemaShowCommandResult.size() == 0){
      // Table Does not exist
      if(tableCreation == SinkTableCreationMode.FailIfNotExist){
        throw new RuntimeException(s"Table '$table' doesn't exist in database '$database', in cluster '$cluster'")
      } else {
        // Parse dataframe schema and create a destination table with that schema
        for(field <- schema){
          tableSchemaBuilder.add(s"${field.name}:${DataTypeMapping.sparkTypeToKustoTypeMap.getOrElse(field.dataType, StringType)}")
        }
        tmpTableSchema = tableSchemaBuilder.toString
        kustoAdminClient.execute(database, generateTableCreateCommand(table, tmpTableSchema))
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
    kustoAdminClient.execute(database, generateTableCreateCommand(tmpTableName, tmpTableSchema))
  }

  def validateSinkParameters(parameters: Map[String,String]): (Boolean, SinkTableCreationMode) = {
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
    (isAsync, tableCreation)
  }

  // Not used. Here in case we prefer this approach
  def generateFindOldTemporaryTablesCommand2(database: String): String = {
    s""".show database $database extents metadata | where TableName startswith '$TempIngestionTablePrefix' | project TableName, maxDate = todynamic(ExtentMetadata).MaxDateTime | where maxDate > ago(1h)"""
  }

  def generateFindOldTempTablesCommand(database: String): String = {
    s""".show journal | where Event == 'CREATE-TABLE' | where Database == '$database' | where EntityName startswith '$TempIngestionTablePrefix' | where EventTimestamp < ago(1h) and EventTimestamp > ago(3d) | project EntityName """
  }

  def generateFindCurrentTempTablesCommand(prefix: String): String = {
    s""".show tables | where TableName startswith '$prefix' | project TableName """
  }

  def generateDropTablesCommand(tables: String): String = {
    s".drop tables ($tables) ifexists"
  }

  def generateShowLatestTableExtents(table: String) : String = {
    s".show table BestTable extents | summarize max(MaxCreatedOn)"
  }

  // Table name must be normalized
  def generateTableCreateCommand(tableName: String, columnsTypesAndNames: String): String = {
    s".create table $tableName ($columnsTypesAndNames)"
  }

  def generateTableShowSchemaCommand(table: String): String = {
    s".show table $table schema as json"
  }

  def generateOperationsShowCommand(operationId: String): String = {
    s".show operations $operationId"
  }

  def generateTableDropCommand(table: String): String = {
    s".drop table $table ifexists"
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

  def generateCreateTmpStorageCommand(): String = {
    s".create tempstorage"
  }

  def generateTableMoveExtentsCommand(sourceTableName:String, destinationTableName: String): String ={
    s".move extents all from table $sourceTableName to table $destinationTableName"
  }

  def generateTableAlterMergePolicyCommand(table: String, allowMerge: Boolean, allowRebuild: Boolean): String ={
    s""".alter table $table policy merge @'{"AllowMerge":"$allowMerge", "AllowRebuild":"$allowRebuild"}'"""
  }
}
package com.microsoft.kusto.spark.utils

import java.util

import com.microsoft.kusto.spark.datasink.KustoWriter.tempIngestionTablePrefix
import com.microsoft.kusto.spark.datasource.KustoStorageParameters
import scala.collection.JavaConverters._

private[kusto] object CslCommandsGenerator {
  def generateFetchTableIngestByTagsCommand(table: String): String = {
    s""".show table $table  extents;
       $$command_results
       | mv-apply split(Tags, "\\r\\n") on (
         where Tags startswith "ingest-by" | project Tags = substring(Tags,10)
         )
       | summarize make_set(Tags)"""
  }


  // Not used. Here in case we prefer this approach
//  def generateFindOldTemporaryTablesCommand2(database: String): String = {
//    s""".show database $database extents metadata | where TableName startswith '$TempIngestionTablePrefix' | project TableName, maxDate = todynamic(ExtentMetadata).MaxDateTime | where maxDate > ago(1h)"""
//  }

  def generateFindOldTempTablesCommand(database: String,tempTablePrefixes: Array[String]): String = {
    val whereClause = tempTablePrefixes.zipWithIndex.foldLeft[String](""){(res: String,cur:(String,Int))=>{
      val (c,i) = cur
      val state= s"${if(i==0) "" else "or"} EntityName startswith '$c'"
      res+state
    } }
    s""".show journal | where Event == 'CREATE-TABLE' | where Database == '$database' | where $whereClause | where EventTimestamp < ago(3d) and EventTimestamp >ago(14d) | project EntityName """
  }

  def generateFindCurrentTempTablesCommand(tempTablePrefixes: Array[String]): String = {
    val whereClause = tempTablePrefixes.zipWithIndex.foldLeft[String](""){(res: String,cur:(String,Int))=>{
      val (c,i) = cur
      val state= s"${if(i==0) "" else "or"} TableName startswith '$c'"
      res+state
    } }
    s""".show tables with (IncludeHiddenTables=true) |where $whereClause | project TableName """
  }

  def generateDropTablesCommand(tables: String): String = {
    s".drop tables ($tables) ifexists"
  }

  // Table name must be normalized
  def generateTableCreateCommand(tableName: String, columnsTypesAndNames: String): String = {
    s".create table $tableName ($columnsTypesAndNames)"
  }

  // Table name must be normalized
  def generateTempTableCreateCommand(tableName: String, columnsTypesAndNames: String, hidden: Boolean = true): String = {
    s".create table $tableName ($columnsTypesAndNames)${if (hidden) " with(hidden=true)" else ""}"
  }

  // Note: we could project-away Type, but this would result in an exception for non-existing tables,
  // and we rely on getting an empty result in this case
  def generateTableGetSchemaAsRowsCommand(table: String): String = {
    ".show table " + KustoQueryUtils.normalizeTableName(table) + " schema as json | project ColumnsJson=todynamic(Schema).OrderedColumns" +
      "| mv-expand ColumnsJson "
  }

  def generateTableDropCommand(table: String): String = {
    s".drop table ${KustoQueryUtils.normalizeTableName(table)} ifexists"
  }

  def generateCreateTmpStorageCommand(): String = {
    s".create tempstorage"
  }

  def generateGetExportContainersCommand(): String = {
    s".show export containers"
  }

  def generateTableMoveExtentsCommand(sourceTableName: String, destinationTableName: String, ingestIfNotExistsTags: util.ArrayList[String]): String = {
    val quotedTags = ingestIfNotExistsTags.asScala.map((tag: String) => s""""$tag"""").asJava
    s""".move extents to table $destinationTableName <|
       .show tables ( $sourceTableName,  $destinationTableName) extents;
        $$command_results
       | where TableName ==  '$sourceTableName'
       | extend extentsSource = todynamic('$quotedTags')
       |extend extentsDest = toscalar(
              $$command_results
               | where TableName == '$destinationTableName'
               | mv-apply Tags=split(Tags, '\\r\\n') on
                (
                 extend Tags = substring(Tags, ${KustoConstants.ingestByPrefix.length})
                )
                |summarize make_set(Tags)
        )
      | where array_length(set_intersect(extentsSource,extentsDest))==0
      |  distinct ExtentId"""
  }

  def generateTableAlterMergePolicyCommand(table: String, allowMerge: Boolean, allowRebuild: Boolean): String = {
    s""".alter table ${KustoQueryUtils.normalizeTableName(table)} policy merge @'{"AllowMerge":"$allowMerge", "AllowRebuild":"$allowRebuild"}'"""
  }

  def generateOperationsShowCommand(operationId: String): String = {
    s".show operations $operationId"
  }

  // Export data to blob
  def generateExportDataCommand(
                                 query: String,
                                 directory: String,
                                 partitionId: Int,
                                 storageParameters: Seq[KustoStorageParameters],
                                 partitionPredicate: Option[String] = None,
                                 sizeLimit: Option[Long],
                                 isAsync: Boolean = true,
                                 isCompressed: Boolean = false): String = {
    val getFullUrlFromParams = (storage: KustoStorageParameters) => {
      val secret = storage.secret
      val secretString = if (storage.secretIsAccountKey) s""";" h@"$secret"""" else if (secret(0) == '?') s"""" h@"$secret"""" else s"""?" h@"$secret""""
      val blobUri = s"https://${storage.account}.blob.${storage.endpointSuffix}"
      s"$blobUri/${storage.container}$secretString"
    }

    val async = if (isAsync) "async " else ""
    val compress = if (isCompressed) "compressed " else ""
    val sizeLimitIfDefined = if (sizeLimit.isDefined) s"sizeLimit=${sizeLimit.get * 1024 * 1024}, " else ""

    var command =
      s""".export $async${compress}to parquet ("${storageParameters.map(getFullUrlFromParams).reduce((s,s1)=>s+",\"" + s1)})""" +
        s""" with (${sizeLimitIfDefined}namePrefix="${directory}part$partitionId", compressionType=snappy) <| $query"""

    if (partitionPredicate.nonEmpty) {
      command += s" | where ${partitionPredicate.get}"
    }
    command
  }

  def generateCountQuery(query: String): String = {
    query + "| count"
  }

  def generateEstimateRowsCountQuery(query: String): String = {
    query + "| evaluate estimate_rows_count()"
  }

  def generateTableCount(table: String): String = {
    s".show tables | where TableName == '$table' | count"
  }

  def generateTableAlterRetentionPolicy(tmpTableName: String, period: String, recoverable: Boolean): String = {
    s""".alter table ${KustoQueryUtils.normalizeTableName(tmpTableName)} policy retention '{ "SoftDeletePeriod": "$period", "Recoverability":"${if (recoverable) "Enabled" else "Disabled"}" }' """
  }

  def generateShowTableMappingsCommand(tableName: String, kind: String): String = {
    s""".show table ${KustoQueryUtils.normalizeTableName(tableName)} ingestion ${kind.toLowerCase()} mappings"""
  }

  def generateCreateTableMappingCommand(tableName: String, kind: String, name:String, mappingAsJson: String): String = {
    s""".create table ${KustoQueryUtils.normalizeTableName(tableName)} ingestion ${kind.toLowerCase} mapping "$name" @"$mappingAsJson""""
  }
}
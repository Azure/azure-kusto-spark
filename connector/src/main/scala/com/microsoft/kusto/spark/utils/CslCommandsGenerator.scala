package com.microsoft.kusto.spark.utils

import java.time.Instant
import com.microsoft.kusto.spark.datasource.{TransientStorageCredentials, TransientStorageParameters}

private[kusto] object CslCommandsGenerator {
  private final val defaultKeySet = Set("compressionType","namePrefix","sizeLimit")
  def generateFetchTableIngestByTagsCommand(table: String): String = {
    s""".show table $table  extents;
       $$command_results
       | mv-apply split(Tags, "\\r\\n") on (
         where Tags startswith "ingest-by" | project Tags = substring(Tags,10)
         )
       | summarize make_set(Tags)"""
  }

  def generateFindCurrentTempTablesCommand(tempTablePrefixes: Array[String]): String = {
    val whereClause = tempTablePrefixes.zipWithIndex.foldLeft[String]("") { (res: String, cur: (String, Int)) => {
      val (c, i) = cur
      val state = s"${if (i == 0) "" else "or"} TableName startswith '$c'"
      res + state
    }
    }
    s""".show tables with (IncludeHiddenTables=true) |where $whereClause | project TableName """
  }

  def generateDropTablesCommand(tables: String): String = {
    s".drop tables ($tables) ifexists"
  }

  def generateTableAlterIngestionBatchingPolicyCommand(tableName: String, targetTableBatchingPolicy: String): String = {
    s""".alter table ${KustoQueryUtils.normalizeTableName(tableName)} policy ingestionbatching @"$targetTableBatchingPolicy""""
  }

  def generateTableShowIngestionBatchingPolicyCommand(tableName: String): String = {
    s".show table ${KustoQueryUtils.normalizeTableName(tableName)} policy ingestionbatching"
  }

  def generateRefreshBatchingPolicyCommand(databaseName:String, tableName: String): String = {
    s""".refresh database '$databaseName' table '$tableName' cache ingestionbatchingpolicy"""
  }

  def generateAlterIngestionBatchingPolicyCommand(entityType: String, entityName: String, targetBatchingPolicy: String): String = {
    s""".alter $entityType ${KustoQueryUtils.normalizeTableName(entityName)} policy ingestionbatching @"$targetBatchingPolicy""""
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

  def generateIsTableMaterializedViewSourceCommand(destinationTableName: String): String = {
    s""".show materialized-views | where SourceTable == '$destinationTableName' | count"""
  }

  def generateIsTableEngineV3(tableName: String): String = {
    s""".show table ${tableName} details | project todynamic(ShardingPolicy).UseShardEngine"""
  }

  def generateTableMoveExtentsCommand(sourceTableName: String, destinationTableName: String, batchSize: Int,
                                      isDestinationTableMaterializedViewSource: Boolean = false): String = {
    val setNewIngestionTime: String = if (isDestinationTableMaterializedViewSource) "with(SetNewIngestionTime=true)" else ""
    s""".move extents to table $destinationTableName $setNewIngestionTime <|
       .show table $sourceTableName extents with(extentsShowFilteringRuntimePolicy='{"MaximumResultsCount":$batchSize}');
        $$command_results
       |  distinct ExtentId"""
  }

  def generateTableMoveExtentsAsyncCommand(sourceTableName: String, destinationTableName: String, batchSize: Option[Int],
                                           isDestinationTableMaterializedViewSource: Boolean = false): String
  = {
    val withClause = if (batchSize.isDefined) s"""with(extentsShowFilteringRuntimePolicy='{"MaximumResultsCount":${batchSize.get}}')""" else ""
    val setNewIngestionTime: String = if (isDestinationTableMaterializedViewSource) "with(SetNewIngestionTime=true)" else ""
    s""".move async extents to table $destinationTableName $setNewIngestionTime <|
       .show table $sourceTableName extents $withClause;
       """
  }

  def generateShowOperationDetails(operataionId: String): String = {
    s".show operation $operataionId details"
  }

  def generateNodesCountCommand(): String = {
    ".show cluster | count"
  }

  def generateExtentsCountCommand(table: String): String = {
    s".show table $table extents | count"
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
                                 storageParameters: TransientStorageParameters,
                                 partitionPredicate: Option[String] = None,
                                 isAsync: Boolean = true,
                                 isCompressed: Boolean = false,
                                 additionalExportOptions: Map[String,String] = Map.empty
                               ): String = {
    val getFullUrlFromParams = (storage: TransientStorageCredentials) => {
      val secretString = if (!storage.sasDefined) s""";" h@"${storage.storageAccountKey}"""" else if
      (storage.sasKey(0) == '?') s"""" h@"${storage.sasKey}"""" else s"""?" h@"${storage.sasKey}""""
      val blobUri = s"https://${storage.storageAccountName}.blob.${storageParameters.endpointSuffix}"
      s"$blobUri/${storage.blobContainer}$secretString"
    }
    val async = if (isAsync) "async " else ""
    val compress = if (isCompressed) "compressed " else ""
    // if (sizeLimit.isDefined) s"sizeLimit=${sizeLimit.get * 1024 * 1024}, " else ""
    val additionalOptionsString = additionalExportOptions.filterKeys(key=> !defaultKeySet.contains(key)).map {
      case (k,v) => s"""$k="$v""""
    }.mkString(",")
    // Values in the map will override,We could have chosen sizeLimit option as the default.
    // Chosen the one in the map for consistency
    val compressionFormat = additionalExportOptions.getOrElse("compressionType","snappy")
    val namePrefix = additionalExportOptions.getOrElse("namePrefix",s"${directory}part$partitionId")
    val sizeLimitOverride = additionalExportOptions.get("sizeLimit").map(size => s"sizeLimit=${size.toLong * 1024 * 1024} ,").getOrElse("")
    var command =
      s""".export $async${compress}to parquet ("${storageParameters.storageCredentials.map(getFullUrlFromParams).reduce((s, s1) => s + ",\"" + s1)})""" +
        s""" with ($sizeLimitOverride namePrefix="$namePrefix", compressionType="$compressionFormat",$additionalOptionsString) <| $query"""
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

  def generateTableAlterAutoDeletePolicy(tmpTableName: String, expiryDate: Instant): String = {
    s""".alter table ${KustoQueryUtils.normalizeTableName(tmpTableName)} policy auto_delete @'{"ExpiryDate": "${expiryDate.toString}","DeleteIfNotEmpty": true }' """
  }

  def generateShowTableMappingsCommand(tableName: String, kind: String): String = {
    s""".show table ${KustoQueryUtils.normalizeTableName(tableName)} ingestion ${kind.toLowerCase()} mappings"""
  }

  def generateCreateTableMappingCommand(tableName: String, kind: String, name: String, mappingAsJson: String): String = {
    s""".create table ${KustoQueryUtils.normalizeTableName(tableName)} ingestion ${kind.toLowerCase} mapping "$name" @"$mappingAsJson""""
  }

  def generateExtentTagsDropByPrefixCommand(tableName:String , prefix: String): String = {
    s""".drop extent tags <|
         .show table $tableName extents
         | where isnotempty(Tags)
         | extend Tags = split(Tags, '\\r\\n')
         | mv-expand Tags to typeof(string)
         | where Tags startswith 'ingest-by:$prefix'
       """
  }
}
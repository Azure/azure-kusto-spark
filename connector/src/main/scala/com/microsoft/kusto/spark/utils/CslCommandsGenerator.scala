package com.microsoft.kusto.spark.utils

import com.microsoft.kusto.spark.datasink.KustoWriter.TempIngestionTablePrefix

private[kusto] object CslCommandsGenerator {

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

  // Table name must be normalized
  def generateTableCreateCommand(tableName: String, columnsTypesAndNames: String): String = {
    s".create table $tableName ($columnsTypesAndNames)"
  }

  // Note: we could project-away Type, but this would result in an exception for non-existing tables,
  // and we rely on getting an empty result in this case
  def generateTableGetSchemaAsRowsCommand(table: String): String = {
    ".show table " + KustoQueryUtils.normalizeTableName(table) + " schema as json | project ColumnsJson=todynamic(Schema).OrderedColumns" +
      "| mv-expand ColumnsJson | evaluate bag_unpack(ColumnsJson)"
  }

  def generateTableDropCommand(table: String): String = {
    s".drop table ${KustoQueryUtils.normalizeTableName(table)} ifexists"
  }

  def generateCreateTmpStorageCommand(): String = {
    s".create tempstorage"
  }

  def generateTableMoveExtentsCommand(sourceTableName: String, destinationTableName: String): String = {
    s".move extents all from table $sourceTableName to table $destinationTableName"
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
                                 storageAccountName: String,
                                 container: String,
                                 directory: String,
                                 secret: String,
                                 useKeyNotSas: Boolean = true,
                                 partitionId: Int,
                                 partitionPredicate: Option[String] = None,
                                 sizeLimit: Option[Long],
                                 isAsync: Boolean = true,
                                 isCompressed: Boolean = false): String = {

    val secretString = if (useKeyNotSas) s""";" h@"$secret"""" else if (secret(0) == '?') s"""" h@"$secret"""" else s"""?" h@"$secret""""
    val blobUri = s"https://$storageAccountName.blob.core.windows.net"
    val async = if (isAsync) "async " else ""
    val compress = if (isCompressed) "compressed " else ""
    val sizeLimitIfDefined = if (sizeLimit.isDefined) s"sizeLimit=${sizeLimit.get * 1024 * 1024}, " else ""

    var command =
      s""".export $async${compress}to parquet ("$blobUri/$container$secretString)""" +
        s""" with (${sizeLimitIfDefined}namePrefix="${directory}part$partitionId", compressionType=snappy) <| $query"""

    if (partitionPredicate.nonEmpty) {
      command += s" | where ${partitionPredicate.get}"
    }
    command
  }

  def generateCountQuery(query: String): String = {
    query + "| count"
  }

  def generateTableCount(table: String): String = {
    s".show tables | where TableName == '$table' | count"
  }

  def generateTableAlterRetentionPolicy(tmpTableName: String, period: String, recoverable: Boolean): String = {
    s""".alter table ${KustoQueryUtils.normalizeTableName(tmpTableName)} policy retention '{ "SoftDeletePeriod": "$period", "Recoverability":"${if (recoverable) "Enabled" else "Disabled"}" }' """
  }

  def generateShowTableMappingsCommand(tableName: String, kind: String): String = {
    s""".show table ${KustoQueryUtils.normalizeTableName(tableName)} ingestion $kind mappings"""
  }

  def generateCreateTableMappingCommand(tableName: String, kind: String, name:String, mappingAsJson: String): String = {
    s""".create table ${KustoQueryUtils.normalizeTableName(tableName)} ingestion $kind mapping "$name" @"$mappingAsJson""""
  }
}
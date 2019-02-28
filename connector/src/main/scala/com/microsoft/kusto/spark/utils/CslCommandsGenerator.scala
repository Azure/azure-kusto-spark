package com.microsoft.kusto.spark.utils

import com.microsoft.kusto.spark.datasink.KustoWriter.TempIngestionTablePrefix

object CslCommandsGenerator{

  // Not used. Here in case we prefer this approach
  private[kusto] def generateFindOldTemporaryTablesCommand2(database: String): String = {
    s""".show database $database extents metadata | where TableName startswith '$TempIngestionTablePrefix' | project TableName, maxDate = todynamic(ExtentMetadata).MaxDateTime | where maxDate > ago(1h)"""
  }

  private[kusto] def generateFindOldTempTablesCommand(database: String): String = {
    s""".show journal | where Event == 'CREATE-TABLE' | where Database == '$database' | where EntityName startswith '$TempIngestionTablePrefix' | where EventTimestamp < ago(1h) and EventTimestamp > ago(3d) | project EntityName """
  }

  private[kusto] def generateFindCurrentTempTablesCommand(prefix: String): String = {
    s""".show tables | where TableName startswith '$prefix' | project TableName """
  }

  private[kusto] def generateDropTablesCommand(tables: String): String = {
    s".drop tables ($tables) ifexists"
  }

  // Table name must be normalized
  def generateTableCreateCommand(tableName: String, columnsTypesAndNames: String): String = {
    s".create table $tableName ($columnsTypesAndNames)"
  }

  private[kusto] def generateTableShowSchemaCommand(table: String): String = {
    s".show table $table schema as json"
  }

  def generateTableDropCommand(table: String): String = {
    s".drop table $table ifexists"
  }

  private[kusto] def generateCreateTmpStorageCommand(): String = {
    s".create tempstorage"
  }

  private[kusto] def generateTableMoveExtentsCommand(sourceTableName:String, destinationTableName: String): String ={
    s".move extents all from table $sourceTableName to table $destinationTableName"
  }

  private[kusto] def generateTableAlterMergePolicyCommand(table: String, allowMerge: Boolean, allowRebuild: Boolean): String ={
    s""".alter table $table policy merge @'{"AllowMerge":"$allowMerge", "AllowRebuild":"$allowRebuild"}'"""
  }

  private[kusto] def generateOperationsShowCommand(operationId: String): String = {
    s".show operations $operationId"
  }

  // Export data to blob
  private[kusto] def generateExportDataCommand(
     query: String,
     storageAccountName: String,
     container: String,
     directory: String,
     secret: String,
     useKeyNotSas: Boolean = true,
     partitionId: Int,
     partitionPredicate: Option[String] = None,
     isAsync: Boolean): String = {

    val secretString = if (useKeyNotSas) s""";" h@"$secret"""" else if (secret(0) == '?') s"""" h@"$secret"""" else s"""?" h@"$secret""""
    val blobUri = s"https://$storageAccountName.blob.core.windows.net"
    val async = if (isAsync) "async " else ""

    var command = s""".export ${async}to parquet ("$blobUri/$container$secretString)""" +
      s""" with (namePrefix="${directory}part$partitionId", fileExtension=parquet) <| $query"""

    if (partitionPredicate.nonEmpty)
    {
      command += s" | where ${partitionPredicate.get}"
    }
    command
  }

  private[kusto]  def generateCountQuery(query: String): String = {
    query + "| count"
  }
}
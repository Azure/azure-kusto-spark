package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException

import com.microsoft.kusto.spark.datasink.KustoWriter
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils, KustoQueryUtils}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val (writeOptions, authentication, tableCoordinates) = KustoDataSourceUtils.parseSinkParameters(parameters, mode)

    KustoWriter.write(
      None,
      data,
      tableCoordinates,
      authentication,
      writeOptions)

    val limit = if (writeOptions.writeResultLimit.equalsIgnoreCase(KustoOptions.NONE_RESULT_LIMIT)) None else {
      try {
        Some(writeOptions.writeResultLimit.toInt)
      }
      catch {
        case _: Exception => throw new InvalidParameterException(s"KustoOptions.KUSTO_WRITE_RESULT_LIMIT is set to '${writeOptions.writeResultLimit}'. Must be either 'none' or an integer value")
      }
    }

    createRelation(sqlContext, adjustParametersForBaseRelation(parameters, limit))
  }

  def adjustParametersForBaseRelation(parameters: Map[String, String], limit: Option[Int]): Map[String, String] = {
    val readMode = parameters.get(KustoOptions.KUSTO_READ_MODE)
    val limitIsSmall = limit.isDefined && limit.get <= 200
    var adjustedParams = parameters

    if (readMode.isEmpty && limitIsSmall) {
      adjustedParams = parameters + (KustoOptions.KUSTO_READ_MODE -> "lean") + (KustoOptions.KUSTO_NUM_PARTITIONS -> "1")
    }
    else if (parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME).isEmpty ||
      parameters.get(KustoOptions.KUSTO_BLOB_CONTAINER).isEmpty ||
      parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY).isEmpty && parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_SAS_KEY).isEmpty
    ) {
      if (readMode.isDefined && readMode.get != "lean") {
        throw new InvalidParameterException(s"Read mode is set to '${readMode.get}', but transient storage parameters are not provided")
      }
      adjustedParams = parameters + (KustoOptions.KUSTO_READ_MODE -> "lean") + (KustoOptions.KUSTO_NUM_PARTITIONS -> "1")
    }

    if (limit.isDefined) {
      adjustedParams + (KustoOptions.KUSTO_QUERY -> KustoQueryUtils.limitQuery(parameters(KustoOptions.KUSTO_TABLE), limit.get))
    } else {
      adjustedParams
    }
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val requestedPartitions = parameters.get(KustoOptions.KUSTO_NUM_PARTITIONS)
    val readMode = parameters.getOrElse(KustoOptions.KUSTO_READ_MODE, "scale")
    val partitioningMode = parameters.get(KustoOptions.KUSTO_READ_PARTITION_MODE)
    val numPartitions = setNumPartitionsPerMode(sqlContext, requestedPartitions, readMode, partitioningMode)

    if (!KustoOptions.supportedReadModes.contains(readMode)) {
      throw new InvalidParameterException(s"Kusto read mode must be one of ${KustoOptions.supportedReadModes.mkString(", ")}")
    }

    if (numPartitions != 1 && readMode.equals("lean")) {
      throw new InvalidParameterException(s"Reading in lean mode cannot be done on multiple partitions. Requested number of partitions: $numPartitions")
    }

    var storageSecreteIsAccountKey = true
    var storageSecrete = parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY)
    if (storageSecrete.isEmpty) {
      storageSecrete = parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_SAS_KEY)
      if (storageSecrete.isDefined) storageSecreteIsAccountKey = false
    }

    KustoRelation(
      KustoCoordinates(parameters.getOrElse(KustoOptions.KUSTO_CLUSTER, ""), parameters.getOrElse(KustoOptions.KUSTO_DATABASE, "")),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_ID, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com"),
      parameters.getOrElse(KustoOptions.KUSTO_QUERY, ""),
      readMode.equalsIgnoreCase("lean"),
      numPartitions,
      parameters.get(KustoOptions.KUSTO_PARTITION_COLUMN),
      partitioningMode,
      parameters.get(KustoOptions.KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES),
      parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME),
      parameters.get(KustoOptions.KUSTO_BLOB_CONTAINER),
      storageSecrete,
      storageSecreteIsAccountKey
    )(sqlContext.sparkSession)
  }

  private def setNumPartitionsPerMode(sqlContext: SQLContext, requestedNumPartitions: Option[String], readMode: String, partitioningMode: Option[String]): Int = {
    if (requestedNumPartitions.isDefined) requestedNumPartitions.get.toInt else {
      if (readMode.equals("lean")) 1 else {
        partitioningMode match {
          case Some("hash") => sqlContext.getConf("spark.sql.shuffle.partitions", "10").toInt
          // In "auto" mode we don't explicitly partition the data:
          // The data is exported and split to multiple files if required by Kusto 'export' command
          // The data is then read from the base directory for parquet files and partitioned by the parquet data source
          case _ => 1
        }
      }
    }
  }

  override def shortName(): String = "kusto"
}
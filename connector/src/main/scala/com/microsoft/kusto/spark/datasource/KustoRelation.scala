package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.Locale

import com.microsoft.kusto.spark.utils.{KustoClient, KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.concurrent.duration.FiniteDuration

private[kusto] case class KustoRelation(kustoCoordinates: KustoCoordinates,
   authentication: KustoAuthentication,
   query: String,
   readOptions: KustoReadOptions,
   timeout: FiniteDuration,
   numPartitions: Int,
   partitioningColumn: Option[String],
   partitioningMode: Option[String],
   customSchema: Option[String] = None,
   storageParameters: Option[KustoStorageParameters])
  (@transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan with PrunedFilteredScan with Serializable {

  private val normalizedQuery = KustoQueryUtils.normalizeQuery(query)
  var cachedSchema: StructType = _
  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = {
    if(cachedSchema == null){
      cachedSchema =  if (customSchema.isDefined) {
        StructType.fromDDL(customSchema.get)
      }
      else getSchema
    }
    cachedSchema
  }

  override def buildScan(): RDD[Row] = {
    val kustoClient = KustoClient.getAdmin(authentication, kustoCoordinates.cluster)
    val count = KDSU.countRows(kustoClient, query, kustoCoordinates.database)
    if (count == 0)  {
      sparkSession.emptyDataFrame.rdd
    }
    else {
      val useLeanMode = KDSU.isLeanReadMode(count, storageParameters.isDefined, readOptions.forcedReadMode)
      if (useLeanMode) {
        KustoReader.leanBuildScan(
          kustoClient,
          KustoReadRequest(sparkSession, schema, kustoCoordinates, query, authentication, timeout)
        )
      } else {
        KustoReader.scaleBuildScan(
          kustoClient,
          KustoReadRequest(sparkSession, schema, kustoCoordinates, query, authentication, timeout),
          storageParameters.get,
          KustoPartitionParameters(numPartitions, getPartitioningColumn, getPartitioningMode)
        )
      }
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val kustoClient = KustoClient.getAdmin(authentication, kustoCoordinates.cluster)
    val count = KDSU.countRows(kustoClient, query, kustoCoordinates.database)
    if (count == 0) {
      sparkSession.emptyDataFrame.rdd
    }
    else {
      val useLeanMode = KDSU.isLeanReadMode(count, storageParameters.isDefined, readOptions.forcedReadMode)
      if (useLeanMode) {
        KustoReader.leanBuildScan(
          kustoClient,
          KustoReadRequest(sparkSession, schema, kustoCoordinates, query, authentication, timeout),
          KustoFiltering(requiredColumns, filters)
        )
      } else {
        KustoReader.scaleBuildScan(
          kustoClient,
          KustoReadRequest(sparkSession, schema, kustoCoordinates, query, authentication, timeout),
          storageParameters.get,
          KustoPartitionParameters(numPartitions, getPartitioningColumn, getPartitioningMode),
          readOptions,
          KustoFiltering(requiredColumns, filters)
        )
      }
    }
  }

  private def getSchema: StructType = {
    if (query.isEmpty) {
      throw new InvalidParameterException("Query is empty")
    }

    val getSchemaQuery = if (KustoQueryUtils.isQuery(query)) KustoQueryUtils.getQuerySchemaQuery(normalizedQuery) else ""
    if (getSchemaQuery.isEmpty) {
      throw new RuntimeException("Spark connector cannot run Kusto commands. Please provide a valid query")
    }

    KDSU.getSchema(kustoCoordinates.database, getSchemaQuery, KustoClient.getAdmin(authentication, kustoCoordinates.cluster))
  }

  private def getPartitioningColumn: String = {
    if (partitioningColumn.isDefined) {
      val requestedColumn = partitioningColumn.get.toLowerCase(Locale.ROOT)
      if (!schema.contains(requestedColumn)) {
        throw new InvalidParameterException(
          s"Cannot partition by column '$requestedColumn' since it is not part of the query schema: ${KDSU.NewLine}${schema.mkString(", ")}")
      }
      requestedColumn
    } else schema.head.name
  }

  private def getPartitioningMode: String = {
    if (partitioningMode.isDefined) {
      val mode = partitioningMode.get.toLowerCase(Locale.ROOT)
      if (!KustoOptions.supportedPartitioningModes.contains(mode)) {
        throw new InvalidParameterException(
          s"Specified partitioning mode '$mode' : ${KDSU.NewLine}${KustoOptions.supportedPartitioningModes.mkString(", ")}")
      }
      mode
    } else "hash"
  }
}
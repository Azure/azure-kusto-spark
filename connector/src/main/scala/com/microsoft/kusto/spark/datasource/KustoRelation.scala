package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.Locale

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.{KustoCoordinates, KustoDebugOptions}
import com.microsoft.kusto.spark.utils.{KustoClientCache, KustoConstants, KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import java.util.concurrent.{TimeUnit, TimeoutException}

import com.microsoft.kusto.spark.datasink.{KustoWriter, WriteOptions}

private[kusto] case class KustoRelation(kustoCoordinates: KustoCoordinates,
                                        authentication: KustoAuthentication,
                                        query: String,
                                        readOptions: KustoReadOptions,
                                        timeout: FiniteDuration,
                                        numPartitions: Int,
                                        partitioningColumn: Option[String],
                                        partitioningMode: Option[String],
                                        customSchema: Option[String] = None,
                                        storageParameters: Option[KustoStorageParameters],
                                        clientRequestProperties: Option[ClientRequestProperties])
                                       (@transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan with PrunedFilteredScan with Serializable with InsertableRelation  {

  private val normalizedQuery = KustoQueryUtils.normalizeQuery(query)
  var cachedSchema: StructType = _

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = {
    if (cachedSchema == null) {
      cachedSchema = if (customSchema.isDefined) {
        StructType.fromDDL(customSchema.get)
      }
      else getSchema
    }
    cachedSchema
  }

  override def buildScan(): RDD[Row] = {
    buildScan(Array.empty, Array.empty)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val kustoClient = KustoClientCache.getClient(kustoCoordinates.cluster, authentication).engineClient
    val (count, timedOut) = KDSU.tryEstimateRowsCount(kustoClient, query, kustoCoordinates.database)
    if (count == 0 && !timedOut) {
      sparkSession.emptyDataFrame.rdd
    }
    else {
      val useLeanMode = KDSU.shouldUseLeanReadMode(count, storageParameters.isDefined, readOptions.forcedReadMode, timedOut)
      if (useLeanMode) {
        KustoReader.leanBuildScan(
          kustoClient,
          KustoReadRequest(sparkSession, schema, kustoCoordinates, query, authentication, timeout, clientRequestProperties),
          KustoFiltering(requiredColumns, filters)
        )
      } else {
        KustoReader.scaleBuildScan(
          kustoClient,
          KustoReadRequest(sparkSession, schema, kustoCoordinates, query, authentication, timeout, clientRequestProperties),
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

    KDSU.getSchema(kustoCoordinates.database, getSchemaQuery, KustoClientCache.getClient(kustoCoordinates.cluster, authentication).engineClient)
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
      if (!KustoDebugOptions.supportedPartitioningModes.contains(mode)) {
        throw new InvalidParameterException(
          s"Specified partitioning mode '$mode' : ${KDSU.NewLine}${KustoDebugOptions.supportedPartitioningModes.mkString(", ")}")
      }
      mode
    } else "hash"
  }

  // Used for cached results
  override def equals(other: Any): Boolean = other match  {
    case that: KustoRelation => kustoCoordinates == kustoCoordinates && query == that.query
    case _ => false
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    KustoWriter.write(Some(0), data, kustoCoordinates, authentication, writeOptions =
      WriteOptions.apply(timeout = new FiniteDuration(KustoConstants.defaultWaitingIntervalLongRunning.toLong, TimeUnit.MINUTES)))
  }
}
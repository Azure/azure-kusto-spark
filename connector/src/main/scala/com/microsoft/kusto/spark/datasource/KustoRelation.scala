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

import scala.concurrent.duration._
import java.util.concurrent.TimeUnit

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
                                        clientRequestProperties: Option[ClientRequestProperties],
                                        requestId: String)
                                       (@transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan with PrunedFilteredScan with Serializable with InsertableRelation  {

  private val normalizedQuery = KustoQueryUtils.normalizeQuery(query)
  var cachedSchema: KustoSchema = _

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = {
    if (cachedSchema == null) {
      cachedSchema = if (customSchema.isDefined) {
        val schema = StructType.fromDDL(customSchema.get)
        KustoSchema(schema, Set())
      }
      else getSchema
    }
    cachedSchema.sparkSchema
  }

  override def buildScan(): RDD[Row] = {
    buildScan(Array.empty, Array.empty)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val kustoClient = KustoClientCache.getClient(kustoCoordinates.clusterAlias, kustoCoordinates.clusterUrl, authentication).engineClient
    var timedOutCounting = false
    val forceSingleMode = readOptions.readMode.isDefined && readOptions.readMode.get == ReadMode.ForceSingleMode
    var useSingleMode = forceSingleMode
    var res: Option[RDD[Row]] = None
    if (readOptions.readMode.isEmpty){
      var count = 0
      try {
        count = KDSU.
          estimateRowsCount(kustoClient, query, kustoCoordinates.database, clientRequestProperties.orNull)
      }catch {
        // Assume count is high if estimation got timed out
        case e: Exception =>
          if (forceSingleMode) {
            // Throw in case user forced LeanMode
            throw e
          }
          // By default we fallback to distributed mode
          timedOutCounting = true
      }
      if (count == 0 && !timedOutCounting) {
        res = Some(sparkSession.emptyDataFrame.rdd)
      } else {
        // Use distributed mode if count is high or in case of a time out
        useSingleMode =  !(timedOutCounting || count > KustoConstants.DirectQueryUpperBoundRows)
      }
    }

    var exception: Option[Exception] = None
    if(res.isEmpty) {
      if (useSingleMode) {
        try {
          res = Some(KustoReader.singleBuildScan(
            kustoClient,
            KustoReadRequest(sparkSession, cachedSchema, kustoCoordinates, query, authentication, timeout, clientRequestProperties, requestId),
            KustoFiltering(requiredColumns, filters)))
        } catch {
          case ex: Exception => exception = Some(ex)
        }
      }

      if (!useSingleMode || exception.isDefined) {
        if(exception.isDefined){
            KDSU.logError("KustoRelation",s"Failed with lean mode, falling back to distributed mode, requestId: $requestId. Exception : ${exception.get.getMessage}")
        }
        res = Some(KustoReader.distributedBuildScan(
          kustoClient,
          KustoReadRequest(sparkSession, cachedSchema, kustoCoordinates, query, authentication, timeout, clientRequestProperties, requestId),
          if (storageParameters.isDefined) Seq(storageParameters.get) else
            KustoClientCache.getClient(kustoCoordinates.clusterAlias, kustoCoordinates.clusterUrl, authentication).getTempBlobsForExport,
          KustoPartitionParameters(numPartitions, getPartitioningColumn, getPartitioningMode),
          readOptions,
          KustoFiltering(requiredColumns, filters))

        )
      }

      if(res.isEmpty && exception.isDefined){
        throw exception.get
      }
    }

    KDSU.logInfo("KustoRelation", s"Finished reading. OperationId: $requestId")
    res.get
  }

  private def getSchema: KustoSchema = {
    if (query.isEmpty) {
      throw new InvalidParameterException("Query is empty")
    }

    val getSchemaQuery = if (KustoQueryUtils.isQuery(query)) KustoQueryUtils.getQuerySchemaQuery(normalizedQuery) else ""
    if (getSchemaQuery.isEmpty) {
      throw new RuntimeException("Spark connector cannot run Kusto commands. Please provide a valid query")
    }

    KDSU.getSchema(kustoCoordinates.database, getSchemaQuery, KustoClientCache.getClient(kustoCoordinates.clusterAlias, kustoCoordinates.clusterUrl, authentication).engineClient, clientRequestProperties)
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
    case that: KustoRelation => kustoCoordinates == that.kustoCoordinates && query == that.query && authentication == that.authentication
    case _ => false
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    KustoWriter.write(None, data, kustoCoordinates, authentication, writeOptions =
      WriteOptions.apply(timeout = new FiniteDuration(KustoConstants.DefaultWaitingIntervalLongRunning.toInt,
        TimeUnit.SECONDS), autoCleanupTime = new FiniteDuration(KustoConstants.DefaultCleaningInterval.toInt,
        TimeUnit.SECONDS)),
      clientRequestProperties.get)
  }
}
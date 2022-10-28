package com.microsoft.kusto.spark.datasource

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.{KustoCoordinates, KustoDebugOptions}
import com.microsoft.kusto.spark.datasink.{KustoParquetWriter, WriteOptions}
import com.microsoft.kusto.spark.utils.{KustoClientCache, KustoConstants, KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import java.security.InvalidParameterException
import java.util.Locale
import scala.concurrent.duration._

private[kusto] case class KustoRelation(kustoCoordinates: KustoCoordinates,
                                        authentication: KustoAuthentication,
                                        query: String,
                                        readOptions: KustoReadOptions,
                                        timeout: FiniteDuration,
                                        customSchema: Option[String] = None,
                                        storageParameters: Option[TransientStorageParameters],
                                        clientRequestProperties: Option[ClientRequestProperties],
                                        requestId: String)
                                       (@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan with Serializable with InsertableRelation {

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

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val kustoClient = KustoClientCache.getClient(kustoCoordinates.clusterUrl, authentication, kustoCoordinates.ingestionUrl, kustoCoordinates.clusterAlias)
    var timedOutCounting = false
    val forceSingleMode = readOptions.readMode.isDefined && readOptions.readMode.get == ReadMode.ForceSingleMode
    var useSingleMode = forceSingleMode
    var res: Option[RDD[Row]] = None
    if (readOptions.readMode.isEmpty) {
      var count = 0
      try {
        count = KDSU.
          estimateRowsCount(kustoClient.engineClient, query, kustoCoordinates.database, clientRequestProperties.orNull)
      } catch {
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
        useSingleMode = !(timedOutCounting || count > KustoConstants.DirectQueryUpperBoundRows)
      }
    }

    var exception: Option[Exception] = None
    if (res.isEmpty) {
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
        if (exception.isDefined) {
          KDSU.logError("KustoRelation", s"Failed with Single mode, falling back to Distributed mode, requestId: $requestId. Exception : ${exception.get.getMessage}")
        }

        readOptions.partitionOptions.column = Some(getPartitioningColumn)
        readOptions.partitionOptions.mode = Some(getPartitioningMode)

        res = Some(KustoReader.distributedBuildScan(
          kustoClient,
          KustoReadRequest(sparkSession, cachedSchema, kustoCoordinates, query, authentication, timeout, clientRequestProperties, requestId),
          if (storageParameters.isDefined) storageParameters.get else
            KustoClientCache.getClient(kustoCoordinates.clusterUrl, authentication, kustoCoordinates.ingestionUrl, kustoCoordinates.clusterAlias).getTempBlobsForExport,
          readOptions,
          KustoFiltering(requiredColumns, filters))

        )
      }

      if (res.isEmpty && exception.isDefined) {
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

    KDSU.getSchema(kustoCoordinates.database,
      getSchemaQuery,
      KustoClientCache.getClient(
        kustoCoordinates.clusterUrl, authentication, kustoCoordinates.ingestionUrl, kustoCoordinates.clusterAlias), clientRequestProperties)
  }

  private def getPartitioningColumn: String = {
    if (readOptions.partitionOptions.column.isDefined) {
      val requestedColumn = readOptions.partitionOptions.column.get
      if (!schema.fields.exists(p => p.name equals requestedColumn)) {
        throw new InvalidParameterException(
          s"Cannot partition by column '$requestedColumn' since it is not part of the query schema: ${KDSU.NewLine}${schema.mkString(", ")}")
      }
      requestedColumn
    } else schema.head.name
  }

  private def getPartitioningMode: String = {
    if (readOptions.partitionOptions.mode.isDefined) {
      val mode = readOptions.partitionOptions.mode.get.toLowerCase(Locale.ROOT)
      if (!KustoDebugOptions.supportedPartitioningModes.contains(mode)) {
        throw new InvalidParameterException(
          s"Specified partitioning mode '$mode' : ${KDSU.NewLine}${KustoDebugOptions.supportedPartitioningModes.mkString(", ")}")
      }
      mode
    } else "hash"
  }

  // Used for cached results
  override def equals(other: Any): Boolean = other match {
    case that: KustoRelation => kustoCoordinates == that.kustoCoordinates && query == that.query && authentication == that.authentication
    case _ => false
  }

  override def hashCode(): Int = kustoCoordinates.hashCode() ^ query.hashCode ^ authentication.hashCode()

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    new KustoParquetWriter().write(None, data, kustoCoordinates, authentication, writeOptions =
      WriteOptions.apply(),
      clientRequestProperties.get)
  }
}
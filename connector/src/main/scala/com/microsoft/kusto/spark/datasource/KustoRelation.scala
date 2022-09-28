package com.microsoft.kusto.spark.datasource

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.{KustoCoordinates, KustoDebugOptions}
import com.microsoft.kusto.spark.datasink.{KustoWriter, WriteOptions}
import com.microsoft.kusto.spark.utils.{KustoClientCache, KustoConstants, KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import java.security.InvalidParameterException
import java.util.Locale
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

private[kusto] case class KustoRelation(kustoCoordinates: KustoCoordinates,
                                        authentication: KustoAuthentication,
                                        query: String,
                                        readOptions: KustoReadOptions,
                                        timeout: FiniteDuration,
                                        customSchema: Option[String] = None,
                                        maybeStorageParameters: Option[TransientStorageParameters],
                                        clientRequestProperties: Option[ClientRequestProperties],
                                        requestId: String)
                                       (@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan with Serializable with InsertableRelation  {

  private val normalizedQuery = KustoQueryUtils.normalizeQuery(query)
  var cachedSchema: KustoSchema = _

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = {
    if (cachedSchema == null) {
      cachedSchema = if (customSchema.isDefined) {
        val schema = StructType.fromDDL(customSchema.get)
        KustoSchema(schema, Set())
      }
      else {
        getSchema
      }
    }
    cachedSchema.sparkSchema
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val kustoClient = KustoClientCache.getClient(kustoCoordinates.clusterUrl, authentication,
      kustoCoordinates.ingestionUrl, kustoCoordinates.clusterAlias)
    val isUserOptionForceSingleMode = readOptions.readMode.contains(ReadMode.ForceSingleMode)
    val storageParameters = maybeStorageParameters.getOrElse(kustoClient.getTempBlobsForExport)
    val (useSingleMode, estimatedRecordCount) = readOptions.readMode match {
      // if the user provides a specific option , this is to be used no matter what
      case Some(_) => (isUserOptionForceSingleMode, -1)
      // If there is no option mentioned , then estimate which option to use
      // Count records and see if we wat a distributed or single mode
      case None => val estimatedRecordCountResult =  Try(KDSU.
        estimateRowsCount(kustoClient.engineClient, query, kustoCoordinates.database, clientRequestProperties.orNull))
        estimatedRecordCountResult match {
          // if the count is lss than the 5k threshold,use single mode.
          case Success(recordCount) => (recordCount <= KustoConstants.DirectQueryUpperBoundRows, recordCount)
          // A case of query timing out. ForceDistributedMode will be used here
          case Failure(_) => (false, -1)
        }
    }
    // if the selection yields 0 rows there are no records to scan and process
    if(estimatedRecordCount == 0){
      sparkSession.emptyDataFrame.rdd
    } else {
      // either a case of non-zero records or a case of timed-out.`
      if(useSingleMode){
        // there are less than 5000 (KustoConstants.DirectQueryUpperBoundRows) records, perform a single scan
        Try(KustoReader.singleBuildScan(
          kustoClient,
          KustoReadRequest(sparkSession, cachedSchema, kustoCoordinates, query, authentication, timeout, clientRequestProperties, requestId),
          KustoFiltering(requiredColumns, filters))) match {
          case Success(rdd) => rdd
          case Failure(exception) =>
            KDSU.logError("KustoRelation","Failed with Single mode, falling back to Distributed mode," +
              s"requestId: $requestId. Exception : ${exception.getMessage}")
            // If the user specified forceSingleMode explicitly and that cannot be honored , throw an exception back
            if(isUserOptionForceSingleMode){
              // Expected behavior for Issue#261
              throw exception
            } else {
              // The case where used did not provide an option and we estimated to be a single scan.
              // Our approximate estimate failed here , fallback to distributed
              readOptions.partitionOptions.column = Some(getPartitioningColumn)
              readOptions.partitionOptions.mode = Some(getPartitioningMode)
              KustoReader.distributedBuildScan(
                kustoClient,
                KustoReadRequest(sparkSession, cachedSchema, kustoCoordinates, query, authentication, timeout, clientRequestProperties, requestId),
                storageParameters,
                readOptions,
                KustoFiltering(requiredColumns, filters))
            }
        }
      }
      else {
        // determined to be distributed mode , through user property or by record count
        readOptions.partitionOptions.column = Some(getPartitioningColumn)
        readOptions.partitionOptions.mode = Some(getPartitioningMode)
        KustoReader.distributedBuildScan(
          kustoClient,
          KustoReadRequest(sparkSession, cachedSchema, kustoCoordinates, query, authentication, timeout, clientRequestProperties, requestId),
          storageParameters,
          readOptions,
          KustoFiltering(requiredColumns, filters))
      }
    }
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
    readOptions.partitionOptions.column match {
      case Some(requestedColumn) =>
        if (!schema.fields.exists(p => p.name equals requestedColumn)) {
          throw new InvalidParameterException(
            s"Cannot partition by column '$requestedColumn' since it is not part of the query schema: ${KDSU.NewLine}${schema.mkString(", ")}")
        }
        requestedColumn
      case None => schema.head.name
    }
  }

  private def getPartitioningMode: String = {
    readOptions.partitionOptions.mode match {
      case Some(mode) =>
        val normalizedMode = mode.toLowerCase(Locale.ROOT)
        if (!KustoDebugOptions.supportedPartitioningModes.contains(normalizedMode)){
          throw new InvalidParameterException(
            s"Specified partitioning mode '$mode' : ${KDSU.NewLine}${KustoDebugOptions.supportedPartitioningModes.mkString(", ")}")
        }
        normalizedMode
      case None => "hash"
    }
  }

  // Used for cached results
  override def equals(other: Any): Boolean = other match  {
    case that: KustoRelation => kustoCoordinates == that.kustoCoordinates && query == that.query && authentication == that.authentication
    case _ => false
  }

  override def hashCode(): Int = kustoCoordinates.hashCode() ^ query.hashCode ^ authentication.hashCode()

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    KustoWriter.write(None, data, kustoCoordinates, authentication, writeOptions =
      WriteOptions.apply(),
      clientRequestProperties.get)
  }
}
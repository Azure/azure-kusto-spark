package com.microsoft.kusto.spark.datasource

import com.microsoft.azure.kusto.data.{Client, ClientRequestProperties, KustoResultSetTable}
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey
import com.microsoft.azure.storage.blob.CloudBlobContainer
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasource.ReadMode.ReadMode
import com.microsoft.kusto.spark.utils.{CslCommandsGenerator, KustoAzureFsSetupCache, KustoBlobStorageUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.{DateTime, DateTimeZone}

import java.net.URI
import java.security.InvalidParameterException
import java.util.UUID

import scala.collection.concurrent
import scala.concurrent.duration.FiniteDuration

private[kusto] case class KustoPartition(predicate: Option[String], idx: Int) extends Partition {
  override def index: Int = idx
}

private[kusto] case class KustoPartitionParameters(num: Int, column: String, mode: String)

private[kusto] case class KustoStorageParameters(account: String,
                                                 secret: String,
                                                 container: String,
                                                 secretIsAccountKey: Boolean,
                                                 endpointSuffix: String = KDSU.DefaultDomainPostfix)

private[kusto] case class KustoFiltering(columns: Array[String] = Array.empty, filters: Array[Filter] = Array.empty)

private[kusto] case class KustoReadRequest(sparkSession: SparkSession,
                                           schema: KustoSchema,
                                           kustoCoordinates: KustoCoordinates,
                                           query: String,
                                           authentication: KustoAuthentication,
                                           timeout: FiniteDuration,
                                           clientRequestProperties: Option[ClientRequestProperties],
                                           requestId: String)

private[kusto] case class KustoReadOptions(readMode: Option[ReadMode] = None,
                                           shouldCompressOnExport: Boolean = true,
                                           exportSplitLimitMb: Long = 1024,
                                           distributedReadModeTransientCacheEnabled: Boolean = false,
                                           queryFilterPushDown: Option[Boolean])

private[kusto] case class DistributedReadModeTransientCacheKey(query: String,
                                                               kustoCoordinates: KustoCoordinates,
                                                               authentication: KustoAuthentication)
private[kusto] object KustoReader {
  private val myName = this.getClass.getSimpleName
  private val distributedReadModeTransientCache: concurrent.Map[DistributedReadModeTransientCacheKey,Seq[String]] =
    new concurrent.TrieMap()

  private[kusto] def singleBuildScan(kustoClient: Client,
                                     request: KustoReadRequest,
                                     filtering: KustoFiltering): RDD[Row] = {

    KDSU.logInfo(myName, s"Executing query. requestId: ${request.requestId}")
    val filteredQuery = KustoFilter.pruneAndFilter(KustoSchema(request.schema.sparkSchema, Set()), request.query, filtering)
    val kustoResult: KustoResultSetTable = kustoClient.execute(request.kustoCoordinates.database,
      filteredQuery,
      request.clientRequestProperties.orNull).getPrimaryResults

    val serializer = KustoResponseDeserializer(kustoResult)
    request.sparkSession.createDataFrame(serializer.toRows, serializer.getSchema.sparkSchema).rdd
  }

  private[kusto] def distributedBuildScan(kustoClient: Client,
                                          request: KustoReadRequest,
                                          storage: Seq[KustoStorageParameters],
                                          partitionInfo: KustoPartitionParameters,
                                          options: KustoReadOptions,
                                          filtering: KustoFiltering): RDD[Row] = {
    var paths: Seq[String] = Seq()
    // if distributedReadModeTransientCacheEnabled is set to true, then check if path is cached and use it
    // if not export and cache the path for reuse
    if (options.distributedReadModeTransientCacheEnabled) {
      val key = DistributedReadModeTransientCacheKey(request.query, request.kustoCoordinates, request.authentication)
      if (distributedReadModeTransientCache.contains(key)) {
        KDSU.logInfo(myName, "Fetching from distributedReadModeTransientCache: hit, reusing cached export paths")
        paths = distributedReadModeTransientCache(key)
      } else {
        KDSU.logInfo(myName, "distributedReadModeTransientCache: miss, exporting to cache paths")
        val filter = determineFilterPushDown(options.queryFilterPushDown, false, filtering)
        paths = exportToStorage(kustoClient, request, storage, partitionInfo, options, filter)
        distributedReadModeTransientCache(key) = paths
      }
    } else{
      val filter = determineFilterPushDown(options.queryFilterPushDown, true, filtering)
      paths = exportToStorage(kustoClient, request, storage, partitionInfo, options, filter)
    }

    def determineFilterPushDown(queryFilterPushDown: Option[Boolean], queryFilterPushDownDefault: Boolean, inputFilter: KustoFiltering): KustoFiltering = {
      if (queryFilterPushDown.getOrElse(queryFilterPushDownDefault)) {
        KDSU.logInfo(myName, s"using ${KustoSourceOptions.KUSTO_QUERY_FILTER_PUSH_DOWN}")
        inputFilter
      } else{
        KDSU.logInfo(myName, s"not using ${KustoSourceOptions.KUSTO_QUERY_FILTER_PUSH_DOWN}")
        KustoFiltering()
      }
    }

    val rdd = try {
      request.sparkSession.read.parquet(paths: _*).rdd
    } catch {
      case ex: Exception => {
        // Check whether the result is empty, causing an IO exception on reading empty parquet file
        // We don't mind generating the filtered query again - it only happens upon exception
        val filteredQuery = KustoFilter.pruneAndFilter(request.schema, request.query, filtering)
        val count = KDSU.countRows(kustoClient, filteredQuery, request.kustoCoordinates.database, request
          .clientRequestProperties.orNull)

        if (count == 0) {
          request.sparkSession.emptyDataFrame.rdd
        } else {
          throw ex
        }
      }
    }

    KDSU.logInfo(myName, "Transaction data read from blob storage, paths:" + paths)
    rdd
  }

  private def exportToStorage(kustoClient: Client,
                              request: KustoReadRequest,
                              storage: Seq[KustoStorageParameters],
                              partitionInfo: KustoPartitionParameters,
                              options: KustoReadOptions,
                              filtering: KustoFiltering) = {

    KDSU.logInfo(myName, s"Starting exporting data from Kusto to blob storage. requestId: ${request.requestId}")

    setupBlobAccess(request, storage)
    val partitions = calculatePartitions(partitionInfo)
    val reader = new KustoReader(kustoClient, request, storage)
    val directory = s"${request.kustoCoordinates.database}/dir${UUID.randomUUID()}/".replaceAll("[^0-9a-zA-Z/]","_")

    for (partition <- partitions) {
      reader.exportPartitionToBlob(
        partition.asInstanceOf[KustoPartition],
        request,
        storage,
        directory,
        options,
        filtering)
    }

    val directoryExists = (params: KustoStorageParameters) => {
      val container = if (params.secretIsAccountKey) {
        new CloudBlobContainer(new URI(s"https://${params.account}.blob.${params.endpointSuffix}/${params.container}"), new StorageCredentialsAccountAndKey(params.account, params.secret))
      } else {
        new CloudBlobContainer(new URI(s"https://${params.account}.blob.${params.endpointSuffix}/${params.container}${params.secret}"))
      }
      container.getDirectoryReference(directory).listBlobsSegmented().getLength > 0
    }
    val paths = storage.filter(directoryExists).map(params => s"wasbs://${params.container}@${params.account}.blob.${params.endpointSuffix}/$directory")
    KDSU.logInfo(myName, s"Finished exporting from Kusto to ${paths.mkString(",")}" +
      s", on requestId: ${request.requestId}, will start parquet reading now")
    paths
  }

  private[kusto] def deleteTransactionBlobsSafe(storage: KustoStorageParameters, directory: String): Unit = {
    try {
      KustoBlobStorageUtils.deleteFromBlob(storage.account, directory, storage.container, storage.secret, !storage.secretIsAccountKey)
    }
    catch {
      case ex: Exception =>
        KDSU.reportExceptionAndThrow(myName, ex, "trying to delete transient blobs from azure storage", shouldNotThrow = true)
    }
  }

  private[kusto] def setupBlobAccess(request: KustoReadRequest, storageParameters: Seq[KustoStorageParameters]): Unit = {
    val config = request.sparkSession.sparkContext.hadoopConfiguration
    val now = new DateTime(DateTimeZone.UTC)
    for(storage <- storageParameters) {
      if (storage.secretIsAccountKey) {
        if (!KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(storage.account, storage.secret, now)) {
          config.set(s"fs.azure.account.key.${storage.account}.blob.${storage.endpointSuffix}", s"${storage.secret}")
        }
      }
      else {
        if (!KustoAzureFsSetupCache.updateAndGetPrevSas(storage.container, storage.account, storage.secret, now)) {
          config.set(s"fs.azure.sas.${storage.container}.${storage.account}.blob.${storage.endpointSuffix}", s"${storage.secret}")
        }
      }

      if (!KustoAzureFsSetupCache.updateAndGetPrevNativeAzureFs(now)) {
        config.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      }
    }
  }

  private def calculatePartitions(partitionInfo: KustoPartitionParameters): Array[Partition] = {
    partitionInfo.mode match {
      case "hash" => calculateHashPartitions(partitionInfo)
      case _ => throw new InvalidParameterException(s"Partitioning mode '${partitionInfo.mode}' is not valid")
    }
  }

  private def calculateHashPartitions(partitionInfo: KustoPartitionParameters): Array[Partition] = {
    // Single partition
    if (partitionInfo.num <= 1) return Array[Partition](KustoPartition(None, 0))

    val partitions = new Array[Partition](partitionInfo.num)
    for (partitionId <- 0 until partitionInfo.num) {
      val partitionPredicate = s" hash(${partitionInfo.column}, ${partitionInfo.num}) == $partitionId"
      partitions(partitionId) = KustoPartition(Some(partitionPredicate), partitionId)
    }
    partitions
  }
}

private[kusto] class KustoReader(client: Client, request: KustoReadRequest, storage: Seq[KustoStorageParameters]) {
  private val myName = this.getClass.getSimpleName

  // Export a single partition from Kusto to transient Blob storage.
  // Returns the directory path for these blobs
  private[kusto] def exportPartitionToBlob(partition: KustoPartition,
                                           request: KustoReadRequest,
                                           storage: Seq[KustoStorageParameters],
                                           directory: String,
                                           options: KustoReadOptions,
                                           filtering: KustoFiltering): Unit = {

    val limit = if (options.exportSplitLimitMb <= 0) None else Some(options.exportSplitLimitMb)

    val exportCommand = CslCommandsGenerator.generateExportDataCommand(
      KustoFilter.pruneAndFilter(request.schema, request.query, filtering),
      directory,
      partition.idx,
      storage,
      partition.predicate,
      limit,
      isCompressed = options.shouldCompressOnExport
    )

    val commandResult: KustoResultSetTable = client.execute(request.kustoCoordinates.database,
      exportCommand,
      request.clientRequestProperties.orNull).getPrimaryResults
    KDSU.verifyAsyncCommandCompletion(client, request.kustoCoordinates.database, commandResult, timeOut = request
      .timeout, doingWhat = s"export data to  blob directory: ('$directory') preparing it for reading.")
  }
}

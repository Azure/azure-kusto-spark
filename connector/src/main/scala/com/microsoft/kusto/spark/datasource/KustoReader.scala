package com.microsoft.kusto.spark.datasource

import com.microsoft.azure.kusto.data.{Client, ClientRequestProperties, KustoResultSetTable}
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey
import com.microsoft.azure.storage.blob.CloudBlobContainer
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasource.ReadMode.ReadMode
import com.microsoft.kusto.spark.utils.{CslCommandsGenerator, KustoAzureFsSetupCache, KustoBlobStorageUtils, ExtendedKustoClient, KustoDataSourceUtils => KDSU}
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
                                           partitionOptions: PartitionOptions,
                                           distributedReadModeTransientCacheEnabled: Boolean = false,
                                           queryFilterPushDown: Option[Boolean],
                                           additionalExportOptions:Map[String,String] = Map.empty
                                          )

private[kusto] case class PartitionOptions(amount: Int, var column: Option[String], var mode: Option[String])

private[kusto] case class DistributedReadModeTransientCacheKey(query: String,
                                                               kustoCoordinates: KustoCoordinates,
                                                               authentication: KustoAuthentication)
private[kusto] object KustoReader {
  private val myName = this.getClass.getSimpleName
  private val distributedReadModeTransientCache: concurrent.Map[DistributedReadModeTransientCacheKey,Seq[String]] =
    new concurrent.TrieMap()

  private[kusto] def singleBuildScan(kustoClient: ExtendedKustoClient,
                                     request: KustoReadRequest,
                                     filtering: KustoFiltering): RDD[Row] = {

    KDSU.logInfo(myName, s"Executing query in Single mode. requestId: ${request.requestId}")
    val filteredQuery = KustoFilter.pruneAndFilter(KustoSchema(request.schema.sparkSchema, Set()), request.query, filtering)
    val kustoResult: KustoResultSetTable = kustoClient.executeEngine(request.kustoCoordinates.database,
      filteredQuery,
      request.clientRequestProperties.orNull).getPrimaryResults

    val serializer = KustoResponseDeserializer(kustoResult)
    request.sparkSession.createDataFrame(serializer.toRows, serializer.getSchema.sparkSchema).rdd
  }

  private[kusto] def distributedBuildScan(kustoClient: ExtendedKustoClient,
                                          request: KustoReadRequest,
                                          storage: TransientStorageParameters,
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
        paths = exportToStorage(kustoClient, request, storage, options, filter)
        distributedReadModeTransientCache(key) = paths
      }
    } else{
      val filter = determineFilterPushDown(options.queryFilterPushDown, true, filtering)
      paths = exportToStorage(kustoClient, request, storage, options, filter)
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
        val count = KDSU.countRows(kustoClient.engineClient, filteredQuery, request.kustoCoordinates.database, request
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

  private def exportToStorage(kustoClient: ExtendedKustoClient,
                              request: KustoReadRequest,
                              storage: TransientStorageParameters,
                              options: KustoReadOptions,
                              filtering: KustoFiltering) = {

    KDSU.logInfo(myName, s"Starting exporting data from Kusto to blob storage in Distributed mode. requestId: ${request.requestId}")

    setupBlobAccess(request, storage)
    val partitions = calculatePartitions(options.partitionOptions)
    val reader = new KustoReader(kustoClient)
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

    val directoryExists = (params: TransientStorageCredentials) => {
      val container = if (params.sasDefined) {
        new CloudBlobContainer(new URI(s"https://${params.storageAccountName}.blob.${storage.endpointSuffix}/${params.blobContainer}${params.sasKey}"))
      } else {
        new CloudBlobContainer(new URI(s"https://${params.storageAccountName}.blob.${storage.endpointSuffix}/${params.blobContainer}"),
          new StorageCredentialsAccountAndKey(params.storageAccountName, params.storageAccountKey))
      }
      container.getDirectoryReference(directory).listBlobsSegmented().getLength > 0
    }
    val paths = storage.storageCredentials.filter(directoryExists).map(params => s"wasbs://${params.blobContainer}@${params.storageAccountName}.blob.${storage.endpointSuffix}/$directory")
    KDSU.logInfo(myName, s"Finished exporting from Kusto to ${paths.mkString(",")}" +
      s", on requestId: ${request.requestId}, will start parquet reading now")
    paths
  }

  private[kusto] def deleteTransactionBlobsSafe(storage: TransientStorageCredentials, directory: String): Unit = {
    try {
      KustoBlobStorageUtils.deleteFromBlob(storage.storageAccountName, directory, storage.blobContainer,
        if (storage.sasDefined) storage.sasKey else storage.storageAccountKey , !storage.sasDefined)
    }
    catch {
      case ex: Exception =>
        KDSU.reportExceptionAndThrow(myName, ex, "trying to delete transient blobs from azure storage", shouldNotThrow = true)
    }
  }

  private[kusto] def setupBlobAccess(request: KustoReadRequest, storageParameters: TransientStorageParameters): Unit = {
    val config = request.sparkSession.sparkContext.hadoopConfiguration
    val now = new DateTime(DateTimeZone.UTC)
    for(storage <- storageParameters.storageCredentials) {
      if (!storage.sasDefined) {
        if (!KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(storage.storageAccountName
          , storage.storageAccountKey, now)) {
          config.set(s"fs.azure.account.key.${storage.storageAccountName}.blob.${storageParameters.endpointSuffix}", s"${storage.storageAccountKey}")
        }
      }
      else {
        if (!KustoAzureFsSetupCache.updateAndGetPrevSas(storage.blobContainer,
          storage.storageAccountName, storage.sasKey, now)) {
          config.set(s"fs.azure.sas.${storage.blobContainer}.${storage.storageAccountName}.blob.${storageParameters.endpointSuffix}", s"${storage.sasKey}")
        }
      }

      if (!KustoAzureFsSetupCache.updateAndGetPrevNativeAzureFs(now)) {
        config.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      }
    }
  }

  private def calculatePartitions(partitionInfo: PartitionOptions): Array[Partition] = {
    partitionInfo.mode.get match {
      case "hash" => calculateHashPartitions(partitionInfo)
      case _ => throw new InvalidParameterException(s"Partitioning mode '${partitionInfo.mode}' is not valid")
    }
  }

  private def calculateHashPartitions(partitionInfo: PartitionOptions): Array[Partition] = {
    // Single partition
    if (partitionInfo.amount <= 1)  Array[Partition](KustoPartition(None, 0))

    val partitions = new Array[Partition](partitionInfo.amount)
    for (partitionId <- 0 until partitionInfo.amount) {
      val partitionPredicate = s" hash(${partitionInfo.column}, ${partitionInfo.amount}) == $partitionId"
      partitions(partitionId) = KustoPartition(Some(partitionPredicate), partitionId)
    }
    partitions
  }
}

private[kusto] class KustoReader(client: ExtendedKustoClient) {
  private val myName = this.getClass.getSimpleName

  // Export a single partition from Kusto to transient Blob storage.
  // Returns the directory path for these blobs
  private[kusto] def exportPartitionToBlob(partition: KustoPartition,
                                           request: KustoReadRequest,
                                           storage: TransientStorageParameters,
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

    val commandResult: KustoResultSetTable = client.executeEngine(request.kustoCoordinates.database,
      exportCommand,
      request.clientRequestProperties.orNull).getPrimaryResults
    KDSU.verifyAsyncCommandCompletion(client.engineClient,
      request.kustoCoordinates.database,
      commandResult,
      timeOut = request.timeout,
      doingWhat = s"export data to  blob directory: ('$directory') preparing it for reading.",
      loggerName = myName,
      requestId = request.requestId)
  }
}

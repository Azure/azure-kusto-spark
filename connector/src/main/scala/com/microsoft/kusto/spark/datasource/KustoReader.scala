//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark.datasource

import com.azure.core.credential.AzureSasCredential
import com.azure.storage.blob.BlobContainerClientBuilder
import com.azure.storage.common.StorageSharedKeyCredential
import com.microsoft.azure.kusto.data.{Client, ClientRequestProperties, KustoResultSetTable}
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasource.ReadMode.ReadMode
import com.microsoft.kusto.spark.utils.{
  CslCommandsGenerator,
  ExtendedKustoClient,
  KustoAzureFsSetupCache,
  KustoBlobStorageUtils,
  KustoDataSourceUtils => KDSU
}
import org.apache.hadoop.util.ComparableVersion
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SparkSession}

import java.net.URI
import java.security.InvalidParameterException
import java.time.{Clock, Instant}
import java.util.UUID
import scala.collection.concurrent
import scala.concurrent.duration.FiniteDuration

private[kusto] case class KustoPartition(predicate: Option[String], idx: Int) extends Partition {
  override def index: Int = idx
}

private[kusto] case class KustoFiltering(
    columns: Array[String] = Array.empty,
    filters: Array[Filter] = Array.empty)

private[kusto] case class KustoReadRequest(
    sparkSession: SparkSession,
    schema: KustoSchema,
    kustoCoordinates: KustoCoordinates,
    query: String,
    authentication: KustoAuthentication,
    timeout: FiniteDuration,
    clientRequestProperties: Option[ClientRequestProperties],
    requestId: String)

private[kusto] case class KustoReadOptions(
    readMode: Option[ReadMode] = None,
    partitionOptions: PartitionOptions,
    distributedReadModeTransientCacheEnabled: Boolean = false,
    queryFilterPushDown: Option[Boolean],
    additionalExportOptions: Map[String, String] = Map.empty)

private[kusto] case class PartitionOptions(
    amount: Int,
    var column: Option[String],
    var mode: Option[String])

private[kusto] case class DistributedReadModeTransientCacheKey(
    query: String,
    kustoCoordinates: KustoCoordinates,
    authentication: KustoAuthentication)
private[kusto] object KustoReader {
  private val className = this.getClass.getSimpleName
  private val distributedReadModeTransientCache
      : concurrent.Map[DistributedReadModeTransientCacheKey, Seq[String]] =
    new concurrent.TrieMap()
  /*
  A new native implementation of Parquet writer that uses new encoding schemes was rolled out on the ADX side. This uses delta byte array for strings and other byte array-based Parquet types (default in Parquet V2 which most modern parquet readers support by default).
  To avoid breaking changes for applications, if the runtime is on a lower version than 3.3.0 of spark runtime we explicitly set the ADX export to not use the useNativeIngestion
  TODO - add test
   */
  private val minimalParquetWriterVersion = "3.3.0"
  private[kusto] def singleBuildScan(
      kustoClient: ExtendedKustoClient,
      request: KustoReadRequest,
      filtering: KustoFiltering): RDD[Row] = {

    KDSU.logInfo(className, s"Executing query in Single mode. requestId: ${request.requestId}")
    val filteredQuery = KustoFilter.pruneAndFilter(
      KustoSchema(request.schema.sparkSchema, Set()),
      request.query,
      filtering)
    val kustoResult: KustoResultSetTable = kustoClient
      .executeEngine(
        request.kustoCoordinates.database,
        filteredQuery,
        request.clientRequestProperties.orNull)
      .getPrimaryResults

    val serializer = KustoResponseDeserializer(kustoResult)
    request.sparkSession.createDataFrame(serializer.toRows, serializer.getSchema.sparkSchema).rdd
  }

  private[kusto] def distributedBuildScan(
      kustoClient: ExtendedKustoClient,
      request: KustoReadRequest,
      storage: TransientStorageParameters,
      options: KustoReadOptions,
      filtering: KustoFiltering): RDD[Row] = {
    var paths: Seq[String] = Seq()
    // if distributedReadModeTransientCacheEnabled is set to true, then check if path is cached and use it
    // if not export and cache the path for reuse
    if (options.distributedReadModeTransientCacheEnabled) {
      val key = DistributedReadModeTransientCacheKey(
        request.query,
        request.kustoCoordinates,
        request.authentication)
      if (distributedReadModeTransientCache.contains(key)) {
        KDSU.logInfo(
          className,
          "Fetching from distributedReadModeTransientCache: hit, reusing cached export paths")
        paths = distributedReadModeTransientCache(key)
      } else {
        KDSU.logInfo(
          className,
          "distributedReadModeTransientCache: miss, exporting to cache paths")
        val filter = determineFilterPushDown(
          options.queryFilterPushDown,
          queryFilterPushDownDefault = false,
          filtering)
        paths = exportToStorage(kustoClient, request, storage, options, filter)
        distributedReadModeTransientCache(key) = paths
      }
    } else {
      val filter = determineFilterPushDown(
        options.queryFilterPushDown,
        queryFilterPushDownDefault = true,
        filtering)
      paths = exportToStorage(kustoClient, request, storage, options, filter)
    }

    def determineFilterPushDown(
        queryFilterPushDown: Option[Boolean],
        queryFilterPushDownDefault: Boolean,
        inputFilter: KustoFiltering): KustoFiltering = {
      if (queryFilterPushDown.getOrElse(queryFilterPushDownDefault)) {
        KDSU.logInfo(className, s"using ${KustoSourceOptions.KUSTO_QUERY_FILTER_PUSH_DOWN}")
        inputFilter
      } else {
        KDSU.logInfo(className, s"not using ${KustoSourceOptions.KUSTO_QUERY_FILTER_PUSH_DOWN}")
        KustoFiltering()
      }
    }

    val rdd =
      try {
        request.sparkSession.read.parquet(paths: _*).rdd
      } catch {
        case ex: Exception =>
          // Check whether the result is empty, causing an IO exception on reading empty parquet file
          // We don't mind generating the filtered query again - it only happens upon exception
          val filteredQuery = KustoFilter.pruneAndFilter(request.schema, request.query, filtering)
          val count = KDSU.countRows(
            kustoClient.engineClient,
            filteredQuery,
            request.kustoCoordinates.database,
            request.clientRequestProperties.orNull)

          if (count == 0) {
            request.sparkSession.emptyDataFrame.rdd
          } else {
            throw ex
          }
      }

    KDSU.logInfo(className, "Transaction data read from blob storage, paths:" + paths)
    rdd
  }

  private def exportToStorage(
      kustoClient: ExtendedKustoClient,
      request: KustoReadRequest,
      storage: TransientStorageParameters,
      options: KustoReadOptions,
      filtering: KustoFiltering) = {

    KDSU.logInfo(
      className,
      s"Starting exporting data from Kusto to blob storage in Distributed mode. requestId: ${request.requestId}")

    setupBlobAccess(request, storage)
    val partitions = calculatePartitions(options.partitionOptions)
    val reader = new KustoReader(kustoClient)
    val directory = s"${request.kustoCoordinates.database}/dir${UUID.randomUUID()}/"
      .replaceAll("[^0-9a-zA-Z/]", "_")

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
      val endpoint = s"https://${params.storageAccountName}.blob.${storage.endpointSuffix}"
      val container = if (params.sasDefined) {
        val sas = if (params.sasKey(0) == '?') params.sasKey else s"?${params.sasKey}"
        new BlobContainerClientBuilder()
          .endpoint(endpoint)
          .containerName(params.blobContainer)
          .credential(new AzureSasCredential(sas))
          .buildClient()
      } else {
        new BlobContainerClientBuilder()
          .endpoint(endpoint)
          .containerName(params.blobContainer)
          .credential(
            new StorageSharedKeyCredential(params.storageAccountName, params.storageAccountKey))
          .buildClient()
      }
      //
      val exists = container.listBlobsByHierarchy(directory).stream().count() > 0
// Existing logic container.exists() && container.getDirectoryReference(directory).listBlobsSegmented().getLength > 0
      exists
    }
    val paths = storage.storageCredentials
      .filter(directoryExists)
      .map(params =>
        s"wasbs://${params.blobContainer}" +
          s"@${params.storageAccountName}.blob.${storage.endpointSuffix}/$directory")
    KDSU.logInfo(
      className,
      s"Finished exporting from Kusto to ${paths.mkString(",")}" +
        s", on requestId: ${request.requestId}, will start parquet reading now")
    paths
  }

  private[kusto] def setupBlobAccess(
      request: KustoReadRequest,
      storageParameters: TransientStorageParameters): Unit = {
    val config = request.sparkSession.sparkContext.hadoopConfiguration
    val now = Instant.now(Clock.systemUTC())
    for (storage <- storageParameters.storageCredentials) {
      if (!storage.sasDefined) {
        if (!KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(
            storage.storageAccountName,
            storage.storageAccountKey,
            now)) {
          config.set(
            s"fs.azure.account.key.${storage.storageAccountName}.blob.${storageParameters.endpointSuffix}",
            s"${storage.storageAccountKey}")
        }
      } else {
        if (!KustoAzureFsSetupCache.updateAndGetPrevSas(
            storage.blobContainer,
            storage.storageAccountName,
            storage.sasKey,
            now)) {
          config.set(
            s"fs.azure.sas.${storage.blobContainer}.${storage.storageAccountName}.blob.${storageParameters.endpointSuffix}",
            s"${storage.sasKey}")
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
      case _ =>
        throw new InvalidParameterException(
          s"Partitioning mode '${partitionInfo.mode}' is not valid")
    }
  }

  private def calculateHashPartitions(partitionInfo: PartitionOptions): Array[Partition] = {
    // Single partition
    if (partitionInfo.amount <= 1) Array[Partition](KustoPartition(None, 0))

    val partitions = new Array[Partition](partitionInfo.amount)
    for (partitionId <- 0 until partitionInfo.amount) {
      partitionInfo.column match {
        case Some(columnName) =>
          val partitionPredicate = s" hash($columnName, ${partitionInfo.amount}) == $partitionId"
          partitions(partitionId) = KustoPartition(Some(partitionPredicate), partitionId)
        case None => KDSU.logWarn(className, "Column name is empty when requesting for export")
      }
    }
    partitions
  }
}

private[kusto] class KustoReader(client: ExtendedKustoClient) {
  private val myName = this.getClass.getSimpleName

  // Export a single partition from Kusto to transient Blob storage.
  // Returns the directory path for these blobs
  private[kusto] def exportPartitionToBlob(
      partition: KustoPartition,
      request: KustoReadRequest,
      storage: TransientStorageParameters,
      directory: String,
      options: KustoReadOptions,
      filtering: KustoFiltering): Unit = {
    val supportNewParquetWriter = new ComparableVersion(request.sparkSession.version)
      .compareTo(new ComparableVersion(KustoReader.minimalParquetWriterVersion)) > 0
    if (!supportNewParquetWriter) {
      KDSU.logWarn(
        myName,
        "Setting useNativeParquetWriter=false. Users are advised to move to Spark versions >= 3.3.0 to leverage the performance and cost improvements of" +
          "new encoding schemes introduced in both Kusto parquet files write and Spark parquet read")
    }
    val exportCommand = CslCommandsGenerator.generateExportDataCommand(
      query = KustoFilter.pruneAndFilter(request.schema, request.query, filtering),
      directory = directory,
      partitionId = partition.idx,
      storageParameters = storage,
      partitionPredicate = partition.predicate,
      additionalExportOptions = options.additionalExportOptions,
      supportNewParquetWriter = supportNewParquetWriter)

    val commandResult: KustoResultSetTable = client
      .executeEngine(
        request.kustoCoordinates.database,
        exportCommand,
        request.clientRequestProperties.orNull)
      .getPrimaryResults
    KDSU.verifyAsyncCommandCompletion(
      client.engineClient,
      request.kustoCoordinates.database,
      commandResult,
      timeOut = request.timeout,
      doingWhat = s"export data to  blob directory: ('$directory') preparing it for reading.",
      loggerName = myName,
      requestId = request.requestId)
  }
}

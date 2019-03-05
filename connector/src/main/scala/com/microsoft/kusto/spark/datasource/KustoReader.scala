package com.microsoft.kusto.spark.datasource
import java.security.InvalidParameterException
import java.util.UUID

import com.microsoft.azure.kusto.data.Client
import com.microsoft.kusto.spark.utils.{CslCommandsGenerator, KustoClient, KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import scala.concurrent.duration.FiniteDuration

private[kusto] case class KustoPartition(predicate: Option[String], idx: Int) extends Partition {
  override def index: Int = idx
}

private[kusto] case class KustoPartitionParameters(num: Int, column: String, mode: String)

private[kusto] case class KustoStorageParameters(account: String,
                                            secret: String,
                                            container: String,
                                            storageSecretIsAccountKey: Boolean)

private[kusto] case class KustoFiltering(columns: Array[String] = Array.empty, filters: Array[Filter] = Array.empty)

private[kusto] case class KustoReadRequest(sparkSession: SparkSession,
                                           schema: StructType,
                                           kustoCoordinates: KustoCoordinates,
                                           query: String,
                                           authentication: KustoAuthentication,
                                           timeout: FiniteDuration)

private[kusto] case class KustoReadOptions(isLeanMode: Boolean, isConfigureFileSystem: Boolean = true, isCompressOnExport: Boolean = true)

private[kusto] object KustoReader {
  private val myName = this.getClass.getSimpleName

  private[kusto] def leanBuildScan(
    request: KustoReadRequest,
    filtering: KustoFiltering = KustoFiltering.apply()): RDD[Row] = {

    val kustoClient = KustoClient.getAdmin(request.authentication, request.kustoCoordinates.cluster)
    val filteredQuery = KustoFilter.pruneAndFilter(request.schema, request.query, filtering)
    val kustoResult = kustoClient.execute(request.kustoCoordinates.database, filteredQuery)
    val serializer = KustoResponseDeserializer(kustoResult)
    request.sparkSession.createDataFrame(serializer.toRows, serializer.getSchema).rdd
  }

  private[kusto] def scaleBuildScan(
     request: KustoReadRequest,
     storage: KustoStorageParameters,
     partitionInfo: KustoPartitionParameters,
     isFsSetupNeeded: Boolean = false,
     isCompressOnExport: Boolean = true,
     filtering: KustoFiltering = KustoFiltering.apply()): RDD[Row] = {

    if (isFsSetupNeeded) setupBlobAccess(request, storage)
    val partitions = calculatePartitions(partitionInfo)
    val reader = new KustoReader(request, storage)
    val directory = KustoQueryUtils.simplifyName(s"${request.kustoCoordinates.database}/dir${UUID.randomUUID()}/")

    for (partition <- partitions) {
      reader.exportPartitionToBlob(partition.asInstanceOf[KustoPartition], request, storage, directory, isCompressOnExport, filtering)
    }

    val path = s"wasbs://${storage.container}@${storage.account}.blob.core.windows.net/$directory"
    try {
      request.sparkSession.read.parquet(s"$path").rdd
    }
    catch {
      case ex: Exception =>
        // Check whether the result is empty, causing an IO exception on reading empty parquet file
        // We don't mind generating the filtered query again - it only happens upon exception
        val filteredQuery = KustoFilter.pruneAndFilter(request.schema, request.query, filtering)
        val count = KDSU.countRows(reader.client, filteredQuery, request.kustoCoordinates.database)

        if (count == 0) {
          request.sparkSession.emptyDataFrame.rdd
        } else { throw ex }
    }
  }

  private[kusto] def setupBlobAccess(request: KustoReadRequest, storage: KustoStorageParameters): Unit = {
    val config = request.sparkSession.conf
    if (storage.storageSecretIsAccountKey) {
      config.set(s"fs.azure.account.key.${storage.account}.blob.core.windows.net", s"${storage.secret}")
    }
    else {
      config.set(s"fs.azure.sas.${storage.container}.${storage.account}.blob.core.windows.net", s"${storage.secret}")
    }
    config.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
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

private[kusto] class KustoReader(request: KustoReadRequest, storage: KustoStorageParameters) {
  private val myName = this.getClass.getSimpleName
  val client: Client = KustoClient.getAdmin(request.authentication, request.kustoCoordinates.cluster)
  val blobFileSizeSplitLimit = 1 * 1024 * 1024 * 1024 // 1GB, maximal allowed for 'export' command

  // Export a single partition from Kusto to transient Blob storage.
  // Returns the directory path for these blobs
  private[kusto] def exportPartitionToBlob(
    partition: KustoPartition,
    request: KustoReadRequest,
    storage: KustoStorageParameters,
    directory: String,
    isCompressed: Boolean,
    filtering: KustoFiltering): Unit = {

    val exportCommand = CslCommandsGenerator.generateExportDataCommand(
      KustoFilter.pruneAndFilter(request.schema, request.query, filtering),
      storage.account,
      storage.container,
      directory,
      storage.secret,
      storage.storageSecretIsAccountKey,
      partition.idx,
      partition.predicate,
      Some(blobFileSizeSplitLimit),
      isCompressed = isCompressed
    )

    val commandResult = client.execute(request.kustoCoordinates.database, exportCommand)
    KDSU.verifyAsyncCommandCompletion(client, request.kustoCoordinates.database, commandResult, timeOut = request.timeout)
  }
}

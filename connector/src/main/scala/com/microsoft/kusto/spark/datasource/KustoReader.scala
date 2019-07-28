package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.UUID

import com.microsoft.azure.kusto.data.{Client, ClientRequestProperties}
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.utils.{CslCommandsGenerator, KustoAzureFsSetupCache, KustoBlobStorageUtils, KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.duration.FiniteDuration

private[kusto] case class KustoPartition(predicate: Option[String], idx: Int) extends Partition {
  override def index: Int = idx
}

private[kusto] case class KustoPartitionParameters(num: Int, column: String, mode: String)

private[kusto] case class KustoStorageParameters(account: String,
                                                 secret: String,
                                                 container: String,
                                                 secretIsAccountKey: Boolean)

private[kusto] case class KustoFiltering(columns: Array[String] = Array.empty, filters: Array[Filter] = Array.empty)

private[kusto] case class KustoReadRequest(sparkSession: SparkSession,
                                           schema: StructType,
                                           kustoCoordinates: KustoCoordinates,
                                           query: String,
                                           authentication: KustoAuthentication,
                                           timeout: FiniteDuration,
                                           clientRequestProperties: Option[ClientRequestProperties])

private[kusto] case class KustoReadOptions(forcedReadMode: String = "",
                                           shouldCompressOnExport: Boolean = true,
                                           exportSplitLimitMb: Long = 1024)

private[kusto] object KustoReader {
  private val myName = this.getClass.getSimpleName

  private[kusto] def leanBuildScan(
                                    kustoClient: Client,
                                    request: KustoReadRequest,
                                    filtering: KustoFiltering): RDD[Row] = {

    val filteredQuery = KustoFilter.pruneAndFilter(request.schema, request.query, filtering)
    val kustoResult = kustoClient.execute(request.kustoCoordinates.database,
      filteredQuery,
      request.clientRequestProperties.orNull)

    val serializer = KustoResponseDeserializer(kustoResult)
    request.sparkSession.createDataFrame(serializer.toRows, serializer.getSchema).rdd
  }

  private[kusto] def scaleBuildScan(
                                     kustoClient: Client,
                                     request: KustoReadRequest,
                                     storage: KustoStorageParameters,
                                     partitionInfo: KustoPartitionParameters,
                                     options: KustoReadOptions,
                                     filtering: KustoFiltering): RDD[Row] = {

    setupBlobAccess(request, storage)
    val partitions = calculatePartitions(partitionInfo)
    val reader = new KustoReader(kustoClient, request, storage)
    val directory = KustoQueryUtils.simplifyName(s"${request.kustoCoordinates.database}/dir${UUID.randomUUID()}/")

    for (partition <- partitions) {
      reader.exportPartitionToBlob(
        partition.asInstanceOf[KustoPartition],
        request,
        storage,
        directory,
        options,
        filtering,
        request.clientRequestProperties)
    }

    KDSU.logInfo(myName, s"Finished exporting from kusto to '${storage.account}/${storage.container}/$directory'" +
      s", will start parquet reading now")

    val path = s"wasbs://${storage.container}@${storage.account}.blob.core.windows.net/$directory"
    val rdd = try {
      request.sparkSession.read.parquet(s"$path").queryExecution.executedPlan.execute().asInstanceOf[RDD[Row]]
    } catch {
      case ex: Exception =>
        // Check whether the result is empty, causing an IO exception on reading empty parquet file
        // We don't mind generating the filtered query again - it only happens upon exception
        val filteredQuery = KustoFilter.pruneAndFilter(request.schema, request.query, filtering)
        val count = KDSU.countRows(kustoClient, filteredQuery, request.kustoCoordinates.database)

        if (count == 0) {
          request.sparkSession.emptyDataFrame.rdd
        } else {
          throw ex
        }
    }

    KDSU.logInfo(myName, "Transaction data written to blob storage account " +
      storage.account + ", container " + storage.container + ", directory " + directory)

    rdd
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

  private[kusto] def setupBlobAccess(request: KustoReadRequest, storage: KustoStorageParameters): Unit = {
    val config = request.sparkSession.conf
    val now = new DateTime(DateTimeZone.UTC)
    if (storage.secretIsAccountKey) {
      if (!KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(storage.account, storage.secret, now)) {
        config.set(s"fs.azure.account.key.${storage.account}.blob.core.windows.net", s"${storage.secret}")
      }
    }
    else {
      if (!KustoAzureFsSetupCache.updateAndGetPrevSas(storage.container, storage.account, storage.secret, now)) {
        config.set(s"fs.azure.sas.${storage.container}.${storage.account}.blob.core.windows.net", s"${storage.secret}")
      }
    }

    if (!KustoAzureFsSetupCache.updateAndGetPrevNativeAzureFs(now)) {
      config.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
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

private[kusto] class KustoReader(client: Client, request: KustoReadRequest, storage: KustoStorageParameters) {
  private val myName = this.getClass.getSimpleName

  // Export a single partition from Kusto to transient Blob storage.
  // Returns the directory path for these blobs
  private[kusto] def exportPartitionToBlob(
                                            partition: KustoPartition,
                                            request: KustoReadRequest,
                                            storage: KustoStorageParameters,
                                            directory: String,
                                            options: KustoReadOptions,
                                            filtering: KustoFiltering,
                                            clientRequestProperties: Option[ClientRequestProperties]): Unit = {

    val limit = if (options.exportSplitLimitMb <= 0) None else Some(options.exportSplitLimitMb)

    val exportCommand = CslCommandsGenerator.generateExportDataCommand(
      KustoFilter.pruneAndFilter(request.schema, request.query, filtering),
      storage.account,
      storage.container,
      directory,
      storage.secret,
      storage.secretIsAccountKey,
      partition.idx,
      partition.predicate,
      limit,
      isCompressed = options.shouldCompressOnExport
    )

    val commandResult = client.execute(request.kustoCoordinates.database,
      exportCommand,
      clientRequestProperties.orNull)
    KDSU.verifyAsyncCommandCompletion(client, request.kustoCoordinates.database, commandResult, timeOut = request.timeout)
  }
}

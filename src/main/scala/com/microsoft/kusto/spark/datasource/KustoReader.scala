package com.microsoft.kusto.spark.datasource
import java.security.InvalidParameterException
import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import com.microsoft.azure.kusto.data.{Client, Results}
import com.microsoft.kusto.spark.utils.{CslCommandsGenerator, KustoClient, KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

private[kusto] case class KustoPartition(predicate: Option[String], idx: Int) extends Partition {
  override def index: Int = idx
}

private[kusto] case class KustoPartitionInfo(num: Int, column: String, mode: String)

private[kusto] case class KustoStorageParameters(account: String,
                                                 secrete: String,
                                                 container: String,
                                                 isKeyNotSas: Boolean)

private[kusto] case class KustoReadRequest(
                                            sparkSession: SparkSession,
                                            schema: StructType,
                                            cluster: String,
                                            database: String,
                                            query: String,
                                            appId: String,
                                            appKey: String,
                                            authorityId: String)

private[kusto] object KustoReader {
  private val myName = this.getClass.getSimpleName

  private[kusto] def leanBuildScan(request: KustoReadRequest): RDD[Row] = {
    val kustoClient = KustoClient.getAdmin(request.cluster, request.appId, request.appKey, request.authorityId)

    val kustoResult = kustoClient.execute(request.database, request.query)
    val serializer = KustoResponseDeserializer(kustoResult)
    request.sparkSession.createDataFrame(serializer.toRows, serializer.getSchema).rdd
  }

  private[kusto] def scaleBuildScan(request: KustoReadRequest, storage: KustoStorageParameters, partitionInfo: KustoPartitionInfo): RDD[Row] = {

    setupBlobAccess(request, storage)
    val partitions = calculatePartitions(partitionInfo)
    val reader = new KustoReader(partitions, request, storage)
    val directory = KustoQueryUtils.simplifyName(s"${request.appId}/dir${UUID.randomUUID()}/")

    for (partition <- partitions) {
      reader.exportPartitionToBlob(partition.asInstanceOf[KustoPartition], request, storage, directory)
    }

    val path = s"wasbs://${storage.container}@${storage.account}.blob.core.windows.net/$directory"
    request.sparkSession.read.parquet(s"$path").rdd
  }

  private[kusto] def setupBlobAccess(request: KustoReadRequest, storage: KustoStorageParameters): Unit = {
    val config = request.sparkSession.conf
    if (storage.isKeyNotSas) {
      config.set(s"fs.azure.account.key.${storage.account}.blob.core.windows.net", s"${storage.secrete}")
    }
    else {
      config.set(s"fs.azure.sas.${storage.container}.${storage.account}.blob.core.windows.net", s"${storage.secrete}")
    }
    config.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
  }

  private def calculatePartitions(partitionInfo: KustoPartitionInfo): Array[Partition] = {
    partitionInfo.mode match {
      case "hash" => calculateHashPartitions(partitionInfo)
      case "integral" | "timestamp" | "predicate" => throw new InvalidParameterException(s"Partitioning mode '${partitionInfo.mode}' is not yet supported ")
      case _ => throw new InvalidParameterException(s"Partitioning mode '${partitionInfo.mode}' is not valid")
    }
  }

  private def calculateHashPartitions(partitionInfo: KustoPartitionInfo): Array[Partition] = {
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

private[kusto] class KustoReader(partitions: Array[Partition], request: KustoReadRequest, storage: KustoStorageParameters) {
  private val myName = this.getClass.getSimpleName
  val client: Client = KustoClient.getAdmin(request.cluster, request.appId, request.appKey, request.authorityId)

  // Export a single partition from Kusto to transient Blob storage.
  // Returns the directory path for these blobs
  private[kusto] def exportPartitionToBlob(partition: KustoPartition,
                                           request: KustoReadRequest,
                                           storage: KustoStorageParameters,
                                           directory: String): Unit = {

    val exportCommand = CslCommandsGenerator.generateExportDataCommand(
      request.appId,
      request.query,
      storage.account,
      storage.container,
      directory,
      storage.secrete,
      storage.isKeyNotSas,
      partition.idx,
      partition.predicate,
      isAsync = true
    )

    val samplePeriod: FiniteDuration = 2 seconds
    val timeOut: FiniteDuration = 30 minutes
    val commandResult = client.execute(request.database, exportCommand)

    KDSU.verifyAsyncCommandCompletion(client, request.database, samplePeriod, timeOut, commandResult)
  }

  private[kusto] def importPartitionFromBlob(
                                              request: KustoReadRequest,
                                              storage: KustoStorageParameters,
                                              directory: String
                                            ): DataFrame = {

    request.sparkSession.read.parquet(s"wasbs://${storage.container}@${storage.account}.blob.core.windows.net/$directory")
  }
}

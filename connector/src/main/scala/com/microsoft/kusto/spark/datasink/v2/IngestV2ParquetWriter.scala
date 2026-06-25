// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.client.{IngestionOperation, QueuedIngestClient}
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.source.{BlobSource, CompressionType}
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.datasink.WriteOptions
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

/**
 * Self-contained Parquet writer for the kusto-ingest-v2 SDK. Uses Spark's native vectorized
 * Parquet writer to write DataFrames directly to storage (Azure Blob or OneLake), then submits
 * the Parquet files for ingestion via the ingest-v2 SDK.
 *
 * Container/path sourcing:
 *   - preferredUploadMethod=Storage → blobPaths from config API (SAS-authenticated)
 *   - preferredUploadMethod=Lake → oneLakePaths from config API (ambient AAD/impersonation)
 *
 * Flow: DataFrame → Spark Parquet Writer → Storage → BlobSource → ingest-v2 SDK
 */
object IngestV2ParquetWriter {
  private val myName = this.getClass.getSimpleName
  private val MaxBlobsPerBatch: Int = 70
  private val containerIndex = new AtomicInteger(0)

  /**
   * Write a DataFrame as Parquet to storage, then ingest via kusto-ingest-v2.
   * Uses containers from the config API response — no `.show ingestion resources` call.
   *
   * @param data
   *   The DataFrame to write
   * @param database
   *   Target Kusto database
   * @param table
   *   Target Kusto table
   * @param queuedClient
   *   kusto-ingest-v2 QueuedIngestClient
   * @param dmConfig
   *   DM configuration from config API (contains storage paths and batch limits)
   * @param writeOptions
   *   Write configuration
   * @param batchIdForTracing
   *   Batch identifier for tracing
   * @return
   *   List of IngestionOperations for status tracking
   */
  def ingestDataFrame(
      data: DataFrame,
      database: String,
      table: String,
      queuedClient: QueuedIngestClient,
      dmConfig: IngestionConfig,
      writeOptions: WriteOptions,
      batchIdForTracing: String): List[IngestionOperation] = {

    val sparkSession = data.sparkSession
    val requestId = writeOptions.requestId

    // Generate unique output directory for this write operation
    val outputDir = s"spark-ingest-v2/$requestId-$batchIdForTracing"

    // Route based on preferredUploadMethod from config API
    val (sparkWritePath, sourceUrls) = dmConfig.preferredUploadMethod match {
      case "Lake" if dmConfig.oneLakePaths.nonEmpty =>
        writeThroughOneLake(sparkSession, dmConfig, outputDir, data, requestId)
      case _ =>
        writeThroughBlobStorage(sparkSession, dmConfig, outputDir, data, requestId)
    }

    KDSU.logInfo(myName, s"Wrote ${sourceUrls.length} Parquet files (requestId: $requestId)")

    if (sourceUrls.isEmpty) {
      KDSU.logWarn(myName, "No Parquet files generated — DataFrame may be empty")
      return List.empty
    }

    // Build ingestion properties (no mapping needed for Parquet — schema embedded)
    val propsBuilder = IngestRequestPropertiesBuilder
      .create()
      .withEnableTracking(true)

    writeOptions.maybeSparkIngestionProperties.foreach { sparkProps =>
      if (sparkProps.ingestByTags != null && !sparkProps.ingestByTags.isEmpty) {
        propsBuilder.withIngestByTags(sparkProps.ingestByTags)
      }
    }

    val props = propsBuilder.build()

    // Submit Parquet files for ingestion in batches
    val batchSize = dmConfig.maxBlobsPerBatch.min(MaxBlobsPerBatch)
    val operations = scala.collection.mutable.ListBuffer[IngestionOperation]()
    val blobSources = sourceUrls.map { url =>
      new BlobSource(url, Format.parquet, UUID.randomUUID(), CompressionType.NONE)
    }

    blobSources.grouped(batchSize).foreach { batch =>
      KDSU.logInfo(
        myName,
        s"Submitting batch of ${batch.length} Parquet files for ingestion to $database.$table")

      val response = queuedClient
        .ingestAsyncJava(database, table, batch.toList.asJava, props)
        .join()

      val ingestResponse = response.getIngestResponse
      val operation = new IngestionOperation(
        ingestResponse.getIngestionOperationId,
        database,
        table,
        response.getIngestionType)
      operations += operation
    }

    KDSU.logInfo(
      myName,
      s"Submitted ${operations.size} ingestion operations for ${sourceUrls.length} Parquet files (requestId: $requestId)")

    operations.toList
  }

  /**
   * Write Parquet through Azure Blob Storage using SAS from config API containers.
   * Returns (sparkWritePath, list of source URLs for BlobSource submission).
   */
  private def writeThroughBlobStorage(
      sparkSession: SparkSession,
      dmConfig: IngestionConfig,
      outputDir: String,
      data: DataFrame,
      requestId: String): (String, Array[String]) = {

    // Pick container (round-robin across available containers)
    val containerPath = pickContainer(dmConfig.blobPaths)
    KDSU.logInfo(myName, s"Using blob storage container from config API (requestId: $requestId)")

    // Parse the container URL: https://<account>.blob.<suffix>/<container>?<sas>
    val (baseUrl, sasToken) = splitUrlAndSas(containerPath)
    val containerUri = new URI(baseUrl)
    val host = containerUri.getHost
    val storageAccount = host.split("\\.")(0)
    val endpointSuffix = host.substring(host.indexOf(".") + 1)
    val containerName = containerUri.getPath.stripPrefix("/")

    // Configure Hadoop FS with SAS token
    configureBlobHadoopFs(sparkSession, storageAccount, containerName, sasToken, endpointSuffix)

    // Build wasbs:// path for Spark write
    val wasbs = s"wasbs://$containerName@$storageAccount.$endpointSuffix/$outputDir"

    KDSU.logInfo(myName, s"Writing DataFrame as Parquet to blob storage (requestId: $requestId)")

    // Write using Spark's native Parquet writer
    data.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(wasbs)

    // List generated Parquet files
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(new URI(wasbs), hadoopConf)
    val outputPath = new org.apache.hadoop.fs.Path(wasbs)
    val parquetFiles = fs
      .listStatus(outputPath)
      .filter(f =>
        f.getPath.getName.endsWith(".parquet") || f.getPath.getName.endsWith(".snappy.parquet"))
      .map(f => s"$baseUrl/$outputDir/${f.getPath.getName}?$sasToken")

    (wasbs, parquetFiles)
  }

  /**
   * Write Parquet through OneLake using ambient AAD/impersonation auth.
   * Returns (sparkWritePath, list of source URLs for BlobSource submission).
   */
  private def writeThroughOneLake(
      sparkSession: SparkSession,
      dmConfig: IngestionConfig,
      outputDir: String,
      data: DataFrame,
      requestId: String): (String, Array[String]) = {

    // Pick OneLake folder (round-robin)
    val oneLakePath = pickContainer(dmConfig.oneLakePaths)
    KDSU.logInfo(
      myName,
      s"Using OneLake storage from config API: $oneLakePath (requestId: $requestId)")

    // Parse OneLake HTTPS URL: https://<host>/<workspace>/<artifact>/<subpath>
    val parsedUri = new URI(oneLakePath)
    val host = parsedUri.getHost
    val pathParts = parsedUri.getPath.stripPrefix("/").split("/", 2)
    val workspace = pathParts(0)
    val artifactPath = if (pathParts.length > 1) pathParts(1) else ""

    // Convert to abfss:// for Spark write (ambient AAD handles auth)
    val abfssBase = s"abfss://$workspace@$host/$artifactPath"
    val abfssWritePath = s"$abfssBase/$outputDir"

    // Configure Hadoop FS for OneLake (abfss with ambient AAD)
    configureOneLakeHadoopFs(sparkSession, host)

    KDSU.logInfo(
      myName,
      s"Writing DataFrame as Parquet to OneLake: $abfssWritePath (requestId: $requestId)")

    // Write using Spark's native Parquet writer
    data.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(abfssWritePath)

    // List generated Parquet files
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(new URI(abfssWritePath), hadoopConf)
    val outputPath = new org.apache.hadoop.fs.Path(abfssWritePath)
    val parquetFiles = fs
      .listStatus(outputPath)
      .filter(f =>
        f.getPath.getName.endsWith(".parquet") || f.getPath.getName.endsWith(".snappy.parquet"))
      .map { f =>
        // Submit HTTPS URLs to ingest-v2 SDK (OneLake accepts HTTPS)
        s"$oneLakePath/$outputDir/${f.getPath.getName}"
      }

    (abfssWritePath, parquetFiles)
  }

  /**
   * Configure Hadoop filesystem with SAS token for blob access (WASBS protocol).
   */
  private def configureBlobHadoopFs(
      sparkSession: SparkSession,
      storageAccount: String,
      containerName: String,
      sasToken: String,
      endpointSuffix: String): Unit = {

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConf.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure")

    // Set SAS token for this specific container
    val sasKey = s"fs.azure.sas.$containerName.$storageAccount.$endpointSuffix"
    hadoopConf.set(sasKey, sasToken)
  }

  /**
   * Configure Hadoop filesystem for OneLake access via abfss (ambient AAD/impersonation).
   * Relies on Spark runtime (Fabric/Databricks) having pre-configured AAD identity.
   */
  private def configureOneLakeHadoopFs(
      sparkSession: SparkSession,
      oneLakeHost: String): Unit = {

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

    // Set ABFS filesystem implementation
    hadoopConf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem")

    // Whitelist the OneLake endpoint
    val endpointKey = "fs.azure.abfs.valid.endpoints"
    val currentEndpoints = Option(hadoopConf.get(endpointKey)).getOrElse("")
    if (!currentEndpoints.contains(oneLakeHost)) {
      val updated =
        if (currentEndpoints.isEmpty) oneLakeHost
        else s"$currentEndpoints,$oneLakeHost"
      hadoopConf.set(endpointKey, updated)
      KDSU.logInfo(myName, s"Whitelisted OneLake endpoint: $oneLakeHost")
    }
  }

  /**
   * Split a URL into base URL and SAS token (split on first '?').
   */
  private def splitUrlAndSas(url: String): (String, String) = {
    val idx = url.indexOf('?')
    if (idx < 0) (url, "")
    else (url.substring(0, idx), url.substring(idx + 1))
  }

  /**
   * Pick a container/path from available list using round-robin.
   */
  private def pickContainer(paths: Seq[String]): String = {
    if (paths.size == 1) paths.head
    else {
      val idx = containerIndex.getAndIncrement() % paths.size
      paths(math.abs(idx))
    }
  }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.client.{IngestionOperation, QueuedIngestClient}
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.source.{BlobSource, CompressionType}
import com.microsoft.kusto.spark.datasink.WriteOptions
import com.microsoft.kusto.spark.utils.{ContainerAndSas, KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI
import java.util.UUID
import scala.jdk.CollectionConverters._

/**
 * Self-contained Parquet writer for the kusto-ingest-v2 SDK. Uses Spark's native vectorized
 * Parquet writer to write DataFrames directly to Azure Blob Storage, then submits the Parquet
 * files for ingestion via the ingest-v2 SDK.
 *
 * This eliminates all custom CSV serialization and leverages Spark's built-in columnar encoding,
 * compression, and type handling.
 *
 * Flow: DataFrame → Spark Parquet Writer → Azure Blob → BlobSource → ingest-v2 SDK
 */
object IngestV2ParquetWriter {
  private val myName = this.getClass.getSimpleName
  private val MaxBlobsPerBatch: Int = 70

  /**
   * Write a DataFrame as Parquet to blob storage, then ingest via kusto-ingest-v2.
   *
   * @param data
   *   The DataFrame to write
   * @param database
   *   Target Kusto database
   * @param table
   *   Target Kusto table
   * @param queuedClient
   *   kusto-ingest-v2 QueuedIngestClient
   * @param containerProvider
   *   Provides blob container URL + SAS
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
      containerProvider: () => ContainerAndSas,
      writeOptions: WriteOptions,
      batchIdForTracing: String): List[IngestionOperation] = {

    val sparkSession = data.sparkSession
    val requestId = writeOptions.requestId

    // Get a container with SAS for writing Parquet files
    val containerAndSas = containerProvider()
    val containerUri = new URI(containerAndSas.containerUrl)
    val storageAccount = containerUri.getHost.split("\\.")(0)
    val containerName = containerUri.getPath.stripPrefix("/")
    val endpointSuffix = containerUri.getHost.substring(containerUri.getHost.indexOf(".") + 1)

    // Generate unique output directory for this write operation
    val outputDir = s"spark-ingest-v2/$requestId-$batchIdForTracing"
    val blobBasePath = s"${containerAndSas.containerUrl}/$outputDir"

    // Configure Hadoop FS with SAS token for this container
    configureHadoopFs(
      sparkSession,
      storageAccount,
      containerName,
      containerAndSas.sas,
      endpointSuffix)

    // Build the wasbs:// path for Spark to write to
    val wasbs = s"wasbs://$containerName@$storageAccount.$endpointSuffix/$outputDir"

    KDSU.logInfo(myName, s"Writing DataFrame as Parquet to blob storage (requestId: $requestId)")

    // Use Spark's native Parquet writer — fully vectorized, columnar, compressed
    data.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(wasbs)

    // List the generated Parquet files
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(new URI(wasbs), hadoopConf)
    val outputPath = new org.apache.hadoop.fs.Path(wasbs)
    val parquetFiles = fs
      .listStatus(outputPath)
      .filter(f =>
        f.getPath.getName.endsWith(".parquet") || f.getPath.getName.endsWith(".snappy.parquet"))
      .map(f => s"$blobBasePath/${f.getPath.getName}?${containerAndSas.sas}")

    KDSU.logInfo(myName, s"Wrote ${parquetFiles.length} Parquet files (requestId: $requestId)")

    if (parquetFiles.isEmpty) {
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

    // Submit Parquet files for ingestion in batches of 70
    val operations = scala.collection.mutable.ListBuffer[IngestionOperation]()
    val blobSources = parquetFiles.map { blobUrl =>
      new BlobSource(blobUrl, Format.parquet, UUID.randomUUID(), CompressionType.NONE)
    }

    blobSources.grouped(MaxBlobsPerBatch).foreach { batch =>
      KDSU.logInfo(myName,
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

    KDSU.logInfo(myName,
      s"Submitted ${operations.size} ingestion operations for ${parquetFiles.length} Parquet files (requestId: $requestId)")

    operations.toList
  }

  /**
   * Configure Hadoop filesystem with SAS token for blob access. Uses WASBS protocol with
   * SAS-based authentication.
   */
  private def configureHadoopFs(
      sparkSession: SparkSession,
      storageAccount: String,
      containerName: String,
      sasToken: String,
      endpointSuffix: String): Unit = {

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

    // Set the filesystem implementation for wasbs://
    hadoopConf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConf.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure")

    // Set SAS token for this specific container
    val sasKey = s"fs.azure.sas.$containerName.$storageAccount.$endpointSuffix"
    hadoopConf.set(sasKey, sasToken)
  }
}

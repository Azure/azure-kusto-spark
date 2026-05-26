// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.client.{IngestionOperation, QueuedIngestClient}
import com.microsoft.azure.kusto.ingest.v2.common.models.{
  ExtendedIngestResponse,
  IngestRequestPropertiesBuilder
}
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping.IngestionMappingType
import com.microsoft.azure.kusto.ingest.v2.models.{Format, IngestRequestProperties}
import com.microsoft.azure.kusto.ingest.v2.source.{BlobSource, CompressionType}
import com.microsoft.azure.storage.blob.{BlobRequestOptions, CloudBlockBlob}
import com.microsoft.kusto.spark.datasink.{CountingWriter, RowCSVWriterUtils, WriteOptions}
import com.microsoft.kusto.spark.utils.{
  ContainerAndSas,
  KustoConstants => KCONST,
  KustoDataSourceUtils => KDSU,
  KustoQueryUtils
}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.zip.GZIPOutputStream
import java.util.{TimeZone, UUID}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

/**
 * Self-contained queued writer using the kusto-ingest-v2 SDK. Handles:
 *   - CSV serialization of Spark rows (via shared RowCSVWriterUtils)
 *   - GZip-compressed blob upload (duplicate of v1's createBlobWriter logic)
 *   - Multi-blob batch ingestion via kusto-ingest-v2 SDK QueuedIngestClient
 *
 * This class has NO dependency on ExtendedKustoClient or KustoClientCache.
 */
object IngestV2QueuedWriter {
  private val myName = this.getClass.getSimpleName
  private val GzipBufferSize: Int = 1000 * KCONST.DefaultBufferSize
  private val MaxBlobsPerBatch: Int = 70
  private val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("HH-mm-ss-SSSSSS").withZone(ZoneId.systemDefault)

  /**
   * Ingest rows from a single partition using the kusto-ingest-v2 SDK queued path. Serializes
   * rows to CSV, uploads to blob, batches blobs, then calls kusto-ingest-v2 SDK for ingestion.
   *
   * @return
   *   The list of IngestionOperations for status tracking
   */
  def ingestPartition(
      rows: Iterator[InternalRow],
      schema: StructType,
      database: String,
      table: String,
      queuedClient: QueuedIngestClient,
      containerProvider: () => ContainerAndSas,
      writeOptions: WriteOptions,
      batchIdForTracing: String): List[IngestionOperation] = {

    val partitionId = TaskContext.getPartitionId()
    val partitionIdStr = partitionId.toString
    val timeZone = TimeZone.getTimeZone(writeOptions.timeZone).toZoneId
    val maxBlobSize = writeOptions.batchLimit * KCONST.OneMegaByte
    val blobNamePrefix = s"${database}_${table}_${batchIdForTracing}"

    KDSU.logInfo(myName, s"Starting partition $partitionIdStr for $database.$table")

    val completedBlobs = ListBuffer[BlobSourceWithInfo]()
    val operations = ListBuffer[IngestionOperation]()

    var blobNumber = 0
    var curBlobUUID = UUID.randomUUID().toString
    var blobWriter =
      createBlobWriter(containerProvider, blobNamePrefix, partitionIdStr, blobNumber, curBlobUUID)

    for (row <- rows) {
      RowCSVWriterUtils.writeRowAsCSV(row, schema, timeZone, blobWriter.csvWriter)

      if (blobWriter.csvWriter.getCounter >= maxBlobSize) {
        finalizeBlobWrite(blobWriter)
        completedBlobs += BlobSourceWithInfo(blobWriter.blobUrl, blobWriter.csvWriter.getCounter)

        KDSU.logDebug(myName,
          s"Partition $partitionIdStr: sealed blob $blobNumber (size: ${blobWriter.csvWriter.getCounter} bytes)")

        // If we've accumulated MaxBlobsPerBatch, flush the batch
        if (completedBlobs.size >= MaxBlobsPerBatch) {
          val op = ingestBatch(queuedClient, database, table, completedBlobs.toList, writeOptions)
          operations += op
          completedBlobs.clear()
        }

        blobNumber += 1
        curBlobUUID = UUID.randomUUID().toString
        blobWriter = createBlobWriter(
          containerProvider,
          blobNamePrefix,
          partitionIdStr,
          blobNumber,
          curBlobUUID)
      }
    }

    // Finalize the last blob
    finalizeBlobWrite(blobWriter)
    if (blobWriter.csvWriter.getCounter > 0) {
      completedBlobs += BlobSourceWithInfo(blobWriter.blobUrl, blobWriter.csvWriter.getCounter)
    }

    // Ingest remaining blobs
    if (completedBlobs.nonEmpty) {
      val op = ingestBatch(queuedClient, database, table, completedBlobs.toList, writeOptions)
      operations += op
    }

    KDSU.logInfo(myName,
      s"Partition $partitionIdStr: completed with ${blobNumber + 1} blobs in ${operations.size} batches")

    operations.toList
  }

  private def ingestBatch(
      queuedClient: QueuedIngestClient,
      database: String,
      table: String,
      blobs: List[BlobSourceWithInfo],
      writeOptions: WriteOptions): IngestionOperation = {

    val blobSources: java.util.List[BlobSource] = blobs.map { b =>
      new BlobSource(b.blobUrl, Format.csv, UUID.randomUUID(), CompressionType.GZIP)
    }.asJava

    val props = buildIngestRequestProperties(writeOptions)

    KDSU.logInfo(myName, s"Ingesting batch of ${blobSources.size()} blobs to $database.$table")

    val response: ExtendedIngestResponse = queuedClient
      .ingestAsyncJava(database, table, blobSources, props)
      .join()

    val operationId = response.getIngestResponse.getIngestionOperationId
    KDSU.logDebug(myName, s"Batch ingestion operationId: $operationId")
    new IngestionOperation(operationId, database, table, response.getIngestionType)
  }

  private def buildIngestRequestProperties(
      writeOptions: WriteOptions): IngestRequestProperties = {
    val builder = IngestRequestPropertiesBuilder.create()
    builder.withEnableTracking(true)

    writeOptions.maybeSparkIngestionProperties.foreach { sparkProps =>
      if (sparkProps.flushImmediately) {
        builder.withSkipBatching(true)
      }
      Option(sparkProps.csvMappingNameReference).filter(_.nonEmpty).foreach { ref =>
        builder.withIngestionMapping(new IngestionMapping(ref, IngestionMappingType.CSV))
      }
    }

    builder.build()
  }

  private def createBlobWriter(
      containerProvider: () => ContainerAndSas,
      blobNamePrefix: String,
      partitionId: String,
      blobNumber: Int,
      blobUUID: String): V2BlobWriteResource = {

    val now = Instant.now()
    val blobName =
      s"${KustoQueryUtils.simplifyName(blobNamePrefix)}_${blobUUID}_${partitionId}_${blobNumber}_${formatter
          .format(now)}_spark.csv.gz"

    val containerAndSas = containerProvider()
    val blobUrl = s"${containerAndSas.containerUrl}/$blobName${containerAndSas.sas}"

    val currentBlob = new CloudBlockBlob(new URI(blobUrl))
    val options = new BlobRequestOptions()
    options.setConcurrentRequestCount(4)
    val gzip = new GZIPOutputStream(currentBlob.openOutputStream(null, options, null))
    val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)
    val buffer = new BufferedWriter(writer, GzipBufferSize)
    val csvWriter = CountingWriter(buffer)

    V2BlobWriteResource(buffer, gzip, csvWriter, blobUrl)
  }

  private def finalizeBlobWrite(resource: V2BlobWriteResource): Unit = {
    resource.writer.flush()
    resource.gzip.flush()
    resource.writer.close()
    resource.gzip.close()
  }
}

private[v2] case class V2BlobWriteResource(
    writer: BufferedWriter,
    gzip: GZIPOutputStream,
    csvWriter: CountingWriter,
    blobUrl: String)

private[v2] case class BlobSourceWithInfo(blobUrl: String, size: Long)

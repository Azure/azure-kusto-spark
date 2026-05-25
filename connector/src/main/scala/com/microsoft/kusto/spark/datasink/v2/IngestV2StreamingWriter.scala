// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.client.{
  IngestionOperation,
  ManagedStreamingIngestClient
}
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping.IngestionMappingType
import com.microsoft.azure.kusto.ingest.v2.models.{Format, IngestRequestProperties}
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource
import com.microsoft.kusto.spark.datasink.{
  CountingWriter,
  RowCSVWriterUtils,
  WriteOptions
}
import com.microsoft.kusto.spark.utils.{
  ByteArrayOutputStreamWithOffset,
  KustoConstants => KCONST
}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.io.{BufferedWriter, ByteArrayInputStream, OutputStreamWriter}
import java.util.{TimeZone, UUID}
import scala.collection.mutable.ListBuffer

/**
 * Self-contained streaming writer using the kusto-ingest-v2 SDK. Uses
 * ManagedStreamingIngestClient which automatically handles:
 * - Streaming ingestion for data ≤ 10MB (direct HTTP body to engine)
 * - Automatic fallback to queued ingestion for larger data
 * - Per-table backoff state machine
 *
 * This class has NO dependency on ExtendedKustoClient or KustoClientCache.
 */
object IngestV2StreamingWriter {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Ingest rows from a single partition using the kusto-ingest-v2 SDK managed
   * streaming path. Rows are serialized to CSV in memory chunks and sent via
   * the ManagedStreamingIngestClient.
   *
   * @return The list of IngestionOperations for status tracking
   */
  def ingestPartition(
      rows: Iterator[InternalRow],
      schema: StructType,
      database: String,
      table: String,
      managedStreamingClient: ManagedStreamingIngestClient,
      writeOptions: WriteOptions,
      batchIdForTracing: String): List[IngestionOperation] = {

    val partitionId = TaskContext.getPartitionId().toString
    val timeZone = TimeZone.getTimeZone(writeOptions.timeZone).toZoneId
    val maxStreamingSize = writeOptions.streamIngestUncompressedMaxSize
    val operations = ListBuffer[IngestionOperation]()

    var byteArrayOutputStream = new ByteArrayOutputStreamWithOffset()
    var streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    var writer = new BufferedWriter(streamWriter)
    var csvWriter = CountingWriter(writer)

    for (row <- rows) {
      RowCSVWriterUtils.writeRowAsCSV(row, schema, timeZone, csvWriter)

      if (csvWriter.getCounter >= maxStreamingSize) {
        writer.flush()
        streamWriter.flush()

        val data = byteArrayOutputStream.toByteArray
        val size = byteArrayOutputStream.size()

        logger.info(
          "Streaming {} bytes from partition {} batch {} via kusto-ingest-v2 SDK",
          size.toString,
          partitionId,
          batchIdForTracing)

        val op = streamData(
          managedStreamingClient,
          database,
          table,
          data,
          size,
          writeOptions)
        operations += op

        // Reset for next chunk
        byteArrayOutputStream = new ByteArrayOutputStreamWithOffset()
        streamWriter = new OutputStreamWriter(byteArrayOutputStream)
        writer = new BufferedWriter(streamWriter)
        csvWriter = CountingWriter(writer)
      }
    }

    // Flush remaining data
    writer.flush()
    streamWriter.flush()
    if (byteArrayOutputStream.size() > 0) {
      val data = byteArrayOutputStream.toByteArray
      val size = byteArrayOutputStream.size()

      logger.info(
        "Streaming final {} bytes from partition {} batch {} via kusto-ingest-v2 SDK",
        size.toString,
        partitionId,
        batchIdForTracing)

      val op = streamData(
        managedStreamingClient,
        database,
        table,
        data,
        size,
        writeOptions)
      operations += op
    }

    logger.info(
      "Finished streaming partition {} with {} chunks",
      partitionId,
      operations.size.toString)

    operations.toList
  }

  private def streamData(
      client: ManagedStreamingIngestClient,
      database: String,
      table: String,
      data: Array[Byte],
      size: Int,
      writeOptions: WriteOptions): IngestionOperation = {

    val inputStream = new ByteArrayInputStream(data, 0, size)
    val source = new StreamSource(inputStream, Format.csv)

    val props = buildIngestRequestProperties(writeOptions)

    val response = client
      .ingestAsyncJava(database, table, source, props)
      .join()

    val operationId = response.getIngestResponse.getIngestionOperationId
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
        builder.withIngestionMapping(
          new IngestionMapping(ref, IngestionMappingType.CSV))
      }
    }

    builder.build()
  }
}

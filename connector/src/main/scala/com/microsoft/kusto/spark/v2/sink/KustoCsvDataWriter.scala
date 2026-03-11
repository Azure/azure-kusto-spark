// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.sink

import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import com.microsoft.azure.kusto.data.auth.CloudInfo
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas
import com.microsoft.azure.kusto.ingest.result.IngestionResult
import com.microsoft.azure.kusto.ingest.source.{BlobSourceInfo, CompressionType}
import com.microsoft.azure.kusto.ingest.{
  IngestClient,
  IngestionProperties,
  ManagedStreamingIngestClient
}
import com.microsoft.azure.storage.blob.{BlobRequestOptions, CloudBlockBlob}
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink._
import com.microsoft.kusto.spark.utils.{
  ByteArrayOutputStreamWithOffset,
  ExtendedKustoClient,
  KustoClientCache,
  KustoQueryUtils,
  OperationMetrics,
  KustoConstants => KCONST,
  KustoDataSourceUtils => KDSU
}
import io.github.resilience4j.retry.RetryConfig
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneId}
import java.util.zip.GZIPOutputStream
import java.util.{TimeZone, UUID}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

final case class KustoCsvDataWriter(
    partitionId: Int,
    taskId: Long,
    tableCoordinates: KustoCoordinates,
    authentication: KustoAuthentication,
    writeOptions: WriteOptions,
    schema: StructType,
    tmpTableName: String,
    batchIdIfExists: String)
    extends DataWriter[InternalRow] {

  private val className = this.getClass.getSimpleName
  private val GzipBufferSize: Int = 1000 * KCONST.DefaultBufferSize
  private val retryConfig: RetryConfig = RetryConfig.custom
    .maxAttempts(KCONST.MaxIngestRetryAttempts)
    .retryExceptions(classOf[IngestionServiceException])
    .build
  private val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("HH-mm-ss-SSSSSS").withZone(ZoneId.systemDefault)
  private val partitionIdStr = partitionId.toString

  private val kustoClient: ExtendedKustoClient = KustoClientCache.getClient(
    tableCoordinates.clusterUrl,
    authentication,
    tableCoordinates.ingestionUrl,
    tableCoordinates.clusterAlias)

  // Pre-warm cloud info cache on the executor
  CloudInfo.retrieveCloudInfoForCluster(kustoClient.ingestKcsb.getClusterUrl)

  private val ingestClient: IngestClient = {
    val client = kustoClient.ingestClient
    val reqRetryOpts = new RequestRetryOptions(
      RetryPolicyType.FIXED,
      KCONST.QueueRetryAttempts,
      Duration.ofSeconds(KCONST.DefaultTimeoutQueueing),
      null,
      null,
      null)
    client.setQueueRequestOptions(reqRetryOpts)
    client
  }

  private val tableName: String =
    if (writeOptions.writeMode == WriteMode.Transactional) tmpTableName
    else tableCoordinates.table.get

  private val ingestionProperties: IngestionProperties = {
    val props = writeOptions.maybeSparkIngestionProperties match {
      case Some(sparkIngestionProperties) =>
        sparkIngestionProperties.toIngestionProperties(tableCoordinates.database, tableName)
      case None => new IngestionProperties(tableCoordinates.database, tableName)
    }
    if (writeOptions.writeMode == WriteMode.Transactional) {
      props.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE)
      props.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
    }
    props.setDataFormat(DataFormat.CSV.name)
    props
  }

  private val maxBlobSize: Long =
    writeOptions.batchLimit.toLong * KCONST.OneMegaByte
  private val timeZone: ZoneId =
    TimeZone.getTimeZone(writeOptions.timeZone).toZoneId
  private val ingestionResults: ListBuffer[IngestionResult] = ListBuffer.empty
  private var ingestionCount: Int = 0

  // Mutable state for the current blob being written to
  private var currentBlobUUID: String = UUID.randomUUID().toString
  private var currentBlobWriter: BlobWriteResource =
    createBlobWriter(partitionIdStr, 0, currentBlobUUID)
  private var blobNumber: Int = 0

  // Streaming mode state
  private var streamingClient: Option[ManagedStreamingIngestClient] = None
  private var byteArrayOutputStream: ByteArrayOutputStreamWithOffset = _
  private var streamWriter: OutputStreamWriter = _
  private var streamBufferedWriter: BufferedWriter = _
  private var streamCsvWriter: CountingWriter = _
  private var streamLastIndex: Int = 0
  private var streamTotalSize: Long = 0L
  private var isStreamingMode: Boolean = false

  if (writeOptions.writeMode == WriteMode.KustoStreaming) {
    isStreamingMode = true
    streamingClient = Some(kustoClient.streamingClient)
    byteArrayOutputStream = new ByteArrayOutputStreamWithOffset()
    streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    streamBufferedWriter = new BufferedWriter(streamWriter)
    streamCsvWriter = CountingWriter(streamBufferedWriter)
  }

  override def write(record: InternalRow): Unit = {
    if (isStreamingMode) {
      writeStreaming(record)
    } else {
      writeBatched(record)
    }
  }

  private def writeBatched(record: InternalRow): Unit = {
    RowCSVWriterUtils.writeRowAsCSV(record, schema, timeZone, currentBlobWriter.csvWriter)
    val count = currentBlobWriter.csvWriter.getCounter
    if (count >= maxBlobSize) {
      KDSU.logInfo(
        className,
        s"Sealing blob in partition $partitionIdStr for requestId: '${writeOptions.requestId}', " +
          s"blob number $blobNumber, with size $count")
      KustoCSVWriter.finalizeBlobWrite(currentBlobWriter)
      ingestBlob(currentBlobWriter, currentBlobUUID, flushImmediately = true)
      blobNumber += 1
      currentBlobUUID = UUID.randomUUID().toString
      currentBlobWriter = createBlobWriter(partitionIdStr, blobNumber, currentBlobUUID)
    }
  }

  private def writeStreaming(record: InternalRow): Unit = {
    RowCSVWriterUtils.writeRowAsCSV(record, schema, timeZone, streamCsvWriter)
    if (streamCsvWriter.getCounter >= writeOptions.streamIngestUncompressedMaxSize) {
      streamBufferedWriter.flush()
      streamWriter.flush()
      if (streamLastIndex != 0) {
        val firstBytes = byteArrayOutputStream.toByteArray
        val newStream = byteArrayOutputStream.createNewFromOffset(streamLastIndex)
        byteArrayOutputStream = newStream
        streamTotalSize += streamLastIndex
        streamBytesIntoKusto(firstBytes, streamLastIndex)
        streamLastIndex = newStream.size()
        streamWriter = new OutputStreamWriter(byteArrayOutputStream)
        streamBufferedWriter = new BufferedWriter(streamWriter)
        streamCsvWriter = CountingWriter(streamBufferedWriter, newStream.size())
      } else {
        streamBytesIntoKusto(
          byteArrayOutputStream.getByteArrayOrCopy,
          byteArrayOutputStream.size())
        byteArrayOutputStream.reset()
        streamTotalSize += streamCsvWriter.getCounter
        streamCsvWriter.resetCounter()
      }
    } else {
      streamBufferedWriter.flush()
      streamLastIndex = byteArrayOutputStream.size()
    }
  }

  override def commit(): WriterCommitMessage = {
    if (isStreamingMode) {
      commitStreaming()
    } else {
      commitBatched()
    }
  }

  private def commitBatched(): WriterCommitMessage = {
    val commitStartNanos = System.nanoTime()
    KDSU.logInfo(
      className,
      s"Committing partition $partitionIdStr for requestId: '${writeOptions.requestId}'")
    KustoCSVWriter.finalizeBlobWrite(currentBlobWriter)
    if (currentBlobWriter.csvWriter.getCounter > 0) {
      ingestBlob(currentBlobWriter, currentBlobUUID, flushImmediately = false)
    }

    // For transactional mode, poll ingestion status on the executor before returning
    if (writeOptions.writeMode == WriteMode.Transactional) {
      pollIngestionResults()
    }

    OperationMetrics.logMetric(
      className,
      "v2.csvWriter.commit",
      (System.nanoTime() - commitStartNanos) / 1000000L,
      Map(
        "partition" -> partitionIdStr,
        "blobsIngested" -> ingestionCount.toString,
        "writeMode" -> writeOptions.writeMode.toString,
        "requestId" -> writeOptions.requestId))

    KustoWriterCommitMessage(partitionId, ingestionCount)
  }

  private def commitStreaming(): WriterCommitMessage = {
    val commitStartNanos = System.nanoTime()
    streamBufferedWriter.flush()
    byteArrayOutputStream.flush()
    IOUtils.close(streamBufferedWriter, byteArrayOutputStream)
    if (streamCsvWriter.getCounter > 0) {
      streamTotalSize += streamCsvWriter.getCounter
      streamBytesIntoKusto(byteArrayOutputStream.getByteArrayOrCopy, byteArrayOutputStream.size())
    }
    if (streamTotalSize > KCONST.WarnStreamingBytes) {
      KDSU.logWarn(
        className,
        s"Total of $streamTotalSize bytes were ingested. Consider 'Queued' writeMode.")
    }

    OperationMetrics.logMetric(
      className,
      "v2.csvWriter.commitStreaming",
      (System.nanoTime() - commitStartNanos) / 1000000L,
      Map(
        "partition" -> partitionIdStr,
        "totalBytes" -> streamTotalSize.toString,
        "requestId" -> writeOptions.requestId))

    KustoWriterCommitMessage(partitionId, ingestionCount)
  }

  override def abort(): Unit = {
    KDSU.logWarn(
      className,
      s"Aborting partition $partitionIdStr for requestId: '${writeOptions.requestId}'")
    closeBlobWriterQuietly()
  }

  override def close(): Unit = {
    closeBlobWriterQuietly()
  }

  private def closeBlobWriterQuietly(): Unit = {
    try {
      if (!isStreamingMode && currentBlobWriter != null) {
        currentBlobWriter.writer.close()
        currentBlobWriter.gzip.close()
      }
      if (isStreamingMode) {
        IOUtils.close(streamBufferedWriter, byteArrayOutputStream)
      }
    } catch {
      case _: Exception => // best-effort cleanup
    }
  }

  private def ingestBlob(
      blobResource: BlobWriteResource,
      blobUUID: String,
      flushImmediately: Boolean): Unit = {
    val ingestStartNanos = System.nanoTime()
    var props = ingestionProperties
    if (!props.getFlushImmediately && flushImmediately) {
      props = SparkIngestionProperties.cloneIngestionProperties(ingestionProperties)
      props.setFlushImmediately(true)
    }
    val blobUri = blobResource.blob.getStorageUri.getPrimaryUri.toString
    val result = KDSU.retryApplyFunction(
      i => {
        Try(
          ingestClient.ingestFromBlob(
            new BlobSourceInfo(blobUri + blobResource.sas, CompressionType.gz, UUID.randomUUID()),
            props)) match {
          case Success(x) =>
            val blobUrlWithSas = s"$blobUri${blobResource.sas}"
            val containerWithSas = new ContainerWithSas(blobUrlWithSas, null)
            kustoClient.reportIngestionResult(containerWithSas, success = true)
            x
          case Failure(e: Throwable) =>
            KDSU.reportExceptionAndThrow(
              className,
              e,
              s"Queueing blob for ingestion, retry '$i', partition " +
                s"$partitionIdStr for requestId: '${writeOptions.requestId}")
            val blobUrlWithSas = s"$blobUri${blobResource.sas}"
            val containerWithSas = new ContainerWithSas(blobUrlWithSas, null)
            kustoClient.reportIngestionResult(containerWithSas, success = false)
            null
        }
      },
      retryConfig,
      "Ingest into Kusto")
    if (result != null) {
      ingestionResults.append(result)
      ingestionCount += 1
    }

    OperationMetrics.logMetric(
      className,
      "v2.csvWriter.ingestBlob",
      (System.nanoTime() - ingestStartNanos) / 1000000L,
      Map(
        "partition" -> partitionIdStr,
        "blobNumber" -> ingestionCount.toString,
        "requestId" -> writeOptions.requestId))

    KDSU.logInfo(
      className,
      s"Queued blob for ingestion in partition $partitionIdStr " +
        s"for requestId: '${writeOptions.requestId}'")
  }

  private def streamBytesIntoKusto(bytes: Array[Byte], length: Int): Unit = {
    KDSU.retryApplyFunction(
      i => {
        val inputStream = new ByteArrayInputStream(bytes, 0, length)
        val streamSourceInfo =
          new com.microsoft.azure.kusto.ingest.source.StreamSourceInfo(inputStream)
        Try(streamingClient.get.ingestFromStream(streamSourceInfo, ingestionProperties)) match {
          case Success(status) =>
            status.getIngestionStatusCollection.forEach(ingestionStatus => {
              KDSU.logInfo(
                className,
                s"Batch $batchIdIfExists IngestionStatus { " +
                  s"status: '${ingestionStatus.status.toString}', " +
                  s"details: ${ingestionStatus.details}, " +
                  s"activityId: ${ingestionStatus.activityId}, " +
                  s"retry: $i" +
                  "}")
            })
          case Failure(e: Throwable) =>
            KDSU.reportExceptionAndThrow(
              className,
              e,
              s"Streaming ingestion in partition $partitionIdStr " +
                s"for requestId: '${writeOptions.requestId}' failed")
        }
      },
      retryConfig,
      "Streaming ingest to Kusto")
  }

  private def pollIngestionResults(): Unit = {
    val pollStartNanos = System.nanoTime()
    val requestId = writeOptions.requestId
    val ingestionInfoString =
      s"RequestId: $requestId cluster: '${tableCoordinates.clusterAlias}', " +
        s"database: '${tableCoordinates.database}', table: '$tmpTableName' $batchIdIfExists"

    KDSU.logInfo(
      className,
      s"Polling ${ingestionResults.size} ingestion result(s) on executor for " +
        s"partition $partitionIdStr, requestId: $requestId")

    ingestionResults.zipWithIndex.foreach { case (result, idx) =>
      FinalizeHelper.pollOnResult(
        PartitionResult(result, partitionId),
        requestId,
        writeOptions.timeout.toMillis,
        ingestionInfoString,
        !writeOptions.kustoCustomDebugWriteOptions.ensureNoDuplicatedBlobs)
    }
    // Release references after polling — no longer needed
    ingestionResults.clear()

    OperationMetrics.logMetric(
      className,
      "v2.csvWriter.pollIngestion",
      (System.nanoTime() - pollStartNanos) / 1000000L,
      Map(
        "partition" -> partitionIdStr,
        "resultsPolled" -> ingestionCount.toString,
        "requestId" -> requestId))
  }

  private def createBlobWriter(
      partId: String,
      blobNum: Int,
      blobUUID: String): BlobWriteResource = {
    val now = Instant.now()
    val blobName = s"${KustoQueryUtils.simplifyName(
        tableCoordinates.database)}_${tmpTableName}_${blobUUID}_${partId}_${blobNum}_${formatter
        .format(now)}_spark.csv.gz"

    val containerAndSas =
      kustoClient.getTempBlobForIngestion(writeOptions.maybeIngestionBlobStorage)

    val currentBlob = new CloudBlockBlob(
      new URI(s"${containerAndSas.containerUrl}/$blobName${containerAndSas.sas}"))
    val currentSas = containerAndSas.sas
    val options = new BlobRequestOptions()
    options.setConcurrentRequestCount(4)
    val gzip: GZIPOutputStream = new GZIPOutputStream(
      currentBlob.openOutputStream(null, options, null))
    val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)
    val buffer: BufferedWriter = new BufferedWriter(writer, GzipBufferSize)
    val csvWriter = CountingWriter(buffer)
    BlobWriteResource(buffer, gzip, csvWriter, currentBlob, currentSas)
  }
}

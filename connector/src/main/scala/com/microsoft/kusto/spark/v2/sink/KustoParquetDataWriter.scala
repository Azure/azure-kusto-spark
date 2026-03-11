// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.sink

import com.microsoft.azure.kusto.data.auth.CloudInfo
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestionProperties}
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.azure.kusto.ingest.result.IngestionResult
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo
import com.microsoft.azure.storage.blob.{BlobRequestOptions, CloudBlockBlob}
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.{
  FinalizeHelper,
  PartitionResult,
  WriteMode,
  WriteOptions
}
import com.microsoft.kusto.spark.utils.{
  ExtendedKustoClient,
  KustoClientCache,
  KustoQueryUtils,
  OperationMetrics,
  KustoConstants => KCONST,
  KustoDataSourceUtils => KDSU
}
import io.github.resilience4j.retry.RetryConfig
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.{OutputFile, PositionOutputStream}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import java.io.OutputStream
import java.net.URI
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

final case class KustoParquetDataWriter(
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

  CloudInfo.retrieveCloudInfoForCluster(kustoClient.ingestKcsb.getClusterUrl)

  private val ingestClient: IngestClient = kustoClient.ingestClient

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
    props.setDataFormat(DataFormat.PARQUET)
    props.setFlushImmediately(true)
    val additionalProps = new util.HashMap[String, String]()
    val existingProps = props.getAdditionalProperties
    if (existingProps != null) {
      additionalProps.putAll(existingProps)
    }
    additionalProps.put("nativeParquetIngestion", "true")
    props.setAdditionalProperties(additionalProps)
    props
  }

  private val maxBlobSize: Long = writeOptions.batchLimit.toLong * KCONST.OneMegaByte
  private val ingestionResults: ListBuffer[IngestionResult] = ListBuffer.empty
  private var ingestionCount: Int = 0

  private val containerAndSas =
    kustoClient.getTempBlobForIngestion(writeOptions.maybeIngestionBlobStorage)

  private var fileIndex: Int = 0
  private var currentWriter: ParquetWriter[InternalRow] = _
  private var currentBlobName: String = _
  private var rowCount: Long = 0L

  initNewParquetFile()

  override def write(record: InternalRow): Unit = {
    currentWriter.write(record)
    rowCount += 1

    val dataSize = currentWriter.getDataSize
    if (dataSize >= maxBlobSize) {
      KDSU.logInfo(
        className,
        s"Parquet file reached $dataSize bytes (~$rowCount rows) in partition " +
          s"$partitionIdStr. Sealing and ingesting, then starting new file.")
      sealAndIngestCurrentFile()
      initNewParquetFile()
    }
  }

  override def commit(): WriterCommitMessage = {
    val commitStartNanos = System.nanoTime()
    KDSU.logInfo(
      className,
      s"Committing Parquet partition $partitionIdStr for requestId: '${writeOptions.requestId}', " +
        s"$rowCount rows written across ${fileIndex + 1} file(s)")

    if (rowCount > 0 || fileIndex == 0) {
      sealAndIngestCurrentFile()
    } else {
      closeCurrentWriterQuietly()
    }

    // For transactional mode, poll ingestion status on the executor before returning
    if (writeOptions.writeMode == WriteMode.Transactional) {
      pollIngestionResults()
    }

    OperationMetrics.logMetric(
      className,
      "v2.parquetWriter.commit",
      (System.nanoTime() - commitStartNanos) / 1000000L,
      Map(
        "partition" -> partitionIdStr,
        "totalRows" -> rowCount.toString,
        "filesWritten" -> (fileIndex + 1).toString,
        "blobsIngested" -> ingestionCount.toString,
        "writeMode" -> writeOptions.writeMode.toString,
        "requestId" -> writeOptions.requestId))

    KustoWriterCommitMessage(partitionId, ingestionCount)
  }

  override def abort(): Unit = {
    KDSU.logWarn(
      className,
      s"Aborting Parquet partition $partitionIdStr for requestId: '${writeOptions.requestId}'")
    closeCurrentWriterQuietly()
  }

  override def close(): Unit = {
    closeCurrentWriterQuietly()
  }

  private def initNewParquetFile(): Unit = {
    val blobUUID = UUID.randomUUID().toString
    val now = Instant.now()
    currentBlobName = s"${KustoQueryUtils.simplifyName(
        tableCoordinates.database)}_${tmpTableName}_${blobUUID}_${partitionIdStr}_${fileIndex}_${formatter
        .format(now)}_spark.parquet"

    // Write Parquet directly to Azure Blob via SDK — bypasses Hadoop FS entirely,
    // avoiding Databricks LokiFileSystem/shading/cache issues with WASB credentials.
    val blobUri = s"${containerAndSas.containerUrl}/$currentBlobName${containerAndSas.sas}"
    val blob = new CloudBlockBlob(new URI(blobUri))
    val blobOptions = new BlobRequestOptions()
    blobOptions.setConcurrentRequestCount(4)
    val outputFile = new BlobOutputFile(blob, blobOptions)

    val writeConf = new Configuration()
    ParquetWriteSupport.setSchema(schema, writeConf)
    // ParquetWriteSupport.init() → SparkToParquetSchemaConverter reads these from Configuration
    writeConf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValueString)
    writeConf.set(
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.defaultValueString)
    writeConf.set(
      SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key,
      SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.defaultValueString)

    currentWriter = new InternalRowParquetWriterBuilder(outputFile)
      .withConf(writeConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()
    rowCount = 0L
  }

  private def sealAndIngestCurrentFile(): Unit = {
    val sealStartNanos = System.nanoTime()
    val writeStartTime = System.currentTimeMillis()
    currentWriter.close()
    val writeDurationMs = System.currentTimeMillis() - writeStartTime

    val ingestUrl =
      s"${containerAndSas.containerUrl}/$currentBlobName${containerAndSas.sas}"

    KDSU.logInfo(
      className,
      s"Parquet part file uploaded: '$currentBlobName', partition=$partitionIdStr, " +
        s"rows=$rowCount, fileIndex=$fileIndex, writeDuration=${writeDurationMs}ms, " +
        s"maxBlobSize=${maxBlobSize / (1024 * 1024)}MB, " +
        s"requestId='${writeOptions.requestId}'")

    val result = KDSU.retryApplyFunction(
      i => {
        Try(
          ingestClient.ingestFromBlob(new BlobSourceInfo(ingestUrl), ingestionProperties)) match {
          case Success(x) => x
          case Failure(e: Throwable) =>
            KDSU.reportExceptionAndThrow(
              className,
              e,
              s"Ingesting Parquet blob, retry '$i', partition $partitionIdStr " +
                s"for requestId: '${writeOptions.requestId}'")
            null
        }
      },
      retryConfig,
      "Ingest Parquet into Kusto")

    if (result != null) {
      ingestionResults.append(result)
      ingestionCount += 1
    }

    OperationMetrics.logMetric(
      className,
      "v2.parquetWriter.sealAndIngest",
      (System.nanoTime() - sealStartNanos) / 1000000L,
      Map(
        "partition" -> partitionIdStr,
        "fileIndex" -> fileIndex.toString,
        "rows" -> rowCount.toString,
        "writeDurationMs" -> writeDurationMs.toString,
        "requestId" -> writeOptions.requestId))

    KDSU.logInfo(
      className,
      s"Queued Parquet blob for ingestion in partition $partitionIdStr " +
        s"for requestId: '${writeOptions.requestId}'")

    fileIndex += 1
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

    ingestionResults.foreach { result =>
      FinalizeHelper.pollOnResult(
        PartitionResult(result, partitionId),
        requestId,
        writeOptions.timeout.toMillis,
        ingestionInfoString,
        !writeOptions.kustoCustomDebugWriteOptions.ensureNoDuplicatedBlobs)
    }
    ingestionResults.clear()

    OperationMetrics.logMetric(
      className,
      "v2.parquetWriter.pollIngestion",
      (System.nanoTime() - pollStartNanos) / 1000000L,
      Map(
        "partition" -> partitionIdStr,
        "resultsPolled" -> ingestionCount.toString,
        "requestId" -> requestId))
  }

  private def closeCurrentWriterQuietly(): Unit = {
    try {
      if (currentWriter != null) {
        currentWriter.close()
      }
    } catch {
      case _: Exception => // best-effort
    }
  }
}

// Parquet OutputFile that writes directly to Azure Blob Storage via the Azure SDK.
// Bypasses Hadoop FileSystem entirely — no WASB/ABFS config or FS cache needed.
private class BlobOutputFile(blob: CloudBlockBlob, options: BlobRequestOptions)
    extends OutputFile {

  override def create(blockSizeHint: Long): PositionOutputStream = {
    val blobStream = blob.openOutputStream(null, options, null)
    new TrackingPositionOutputStream(blobStream)
  }

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
    create(blockSizeHint)

  override def supportsBlockSize(): Boolean = false
  override def defaultBlockSize(): Long = 0L
}

// PositionOutputStream wrapper that tracks bytes written
private class TrackingPositionOutputStream(inner: OutputStream) extends PositionOutputStream {
  private var pos: Long = 0L

  override def getPos: Long = pos

  override def write(b: Int): Unit = {
    inner.write(b)
    pos += 1
  }

  override def write(b: Array[Byte]): Unit = {
    inner.write(b)
    pos += b.length
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    inner.write(b, off, len)
    pos += len
  }

  override def flush(): Unit = inner.flush()
  override def close(): Unit = inner.close()
}

// Builder that creates a ParquetWriter using Spark's ParquetWriteSupport for InternalRow.
// Uses OutputFile (not Path) to bypass Hadoop FileSystem.
private class InternalRowParquetWriterBuilder(outputFile: OutputFile)
    extends ParquetWriter.Builder[InternalRow, InternalRowParquetWriterBuilder](outputFile) {

  override protected def self(): InternalRowParquetWriterBuilder = this

  override protected def getWriteSupport(
      conf: Configuration): org.apache.parquet.hadoop.api.WriteSupport[InternalRow] = {
    new ParquetWriteSupport()
  }
}

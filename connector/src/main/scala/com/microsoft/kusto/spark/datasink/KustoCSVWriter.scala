// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.data.auth.CloudInfo
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas
import com.microsoft.azure.kusto.ingest.source.{BlobSourceInfo, CompressionType, StreamSourceInfo}
import com.microsoft.azure.kusto.ingest.{
  IngestClient,
  IngestionProperties,
  ManagedStreamingIngestClient
}
import com.microsoft.azure.storage.blob.{BlobRequestOptions, CloudBlockBlob}
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.FinalizeHelper.finalizeIngestionWhenWorkersSucceeded
import com.microsoft.kusto.spark.utils.KustoConstants.{MaxIngestRetryAttempts, WarnStreamingBytes}
import com.microsoft.kusto.spark.utils.{
  ByteArrayOutputStreamWithOffset,
  ExtendedKustoClient,
  KustoClientCache,
  KustoQueryUtils,
  KustoConstants => KCONST,
  KustoDataSourceUtils => KDSU
}
import io.github.resilience4j.retry.RetryConfig
import org.apache.commons.io.IOUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant}
import java.util
import java.util.zip.GZIPOutputStream
import java.util.{TimeZone, UUID}
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object KustoCSVWriter {
  private val className = this.getClass.getSimpleName
  private val GzipBufferSize: Int = 1000 * KCONST.DefaultBufferSize
  private val retryConfig = RetryConfig.custom
    .maxAttempts(MaxIngestRetryAttempts)
    .retryExceptions(classOf[IngestionServiceException])
    .build
  private val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("HH-mm-ss-SSSSSS").withZone(ZoneId.systemDefault)

  private[kusto] def ingestRowsIntoTempTbl(
      rows: Iterator[InternalRow],
      batchIdForTracing: String,
      partitionsResults: CollectionAccumulator[PartitionResult],
      parameters: KustoWriteResource): Unit = {
    if (rows.isEmpty) {
      KDSU.logWarn(
        className,
        s"sink to Kusto table '${parameters.coordinates.table.get}' with no rows to write " +
          s"on partition ${TaskContext.getPartitionId()} $batchIdForTracing")
    } else {
      val ingestionProperties = getIngestionProperties(
        parameters.writeOptions,
        parameters.coordinates.database,
        if (parameters.writeOptions.writeMode == WriteMode.Transactional) {
          parameters.tmpTableName
        } else {
          parameters.coordinates.table.get
        })
      if (parameters.writeOptions.writeMode == WriteMode.KustoStreaming) {
        streamRowsIntoKustoByWorkers(batchIdForTracing, rows, ingestionProperties, parameters)
      } else {
        ingestToTemporaryTableByWorkers(
          batchIdForTracing,
          rows,
          partitionsResults,
          ingestionProperties,
          parameters)
      }
    }
  }

  private def ingestRowsIntoKusto(
      rows: Iterator[InternalRow],
      ingestClient: IngestClient,
      ingestionProperties: IngestionProperties,
      partitionsResults: CollectionAccumulator[PartitionResult],
      batchIdForTracing: String,
      parameters: KustoWriteResource): Unit = {
    if (parameters.writeOptions.writeMode == WriteMode.Transactional) {
      ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE)
      ingestionProperties.setReportLevel(
        IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
    }
    ingestionProperties.setDataFormat(DataFormat.CSV.name)
    ingestRows(
      rows,
      parameters,
      ingestClient,
      ingestionProperties,
      partitionsResults,
      batchIdForTracing)
    KDSU.logInfo(
      className,
      s"Ingesting from blob(s) partition: ${TaskContext.getPartitionId()} requestId: " +
        s"'${parameters.writeOptions.requestId}' batch$batchIdForTracing")
  }

  private[kusto] def getIngestionProperties(
      writeOptions: WriteOptions,
      database: String,
      tableName: String): IngestionProperties = {
    writeOptions.maybeSparkIngestionProperties match {
      case Some(sparkIngestionProperties) =>
        sparkIngestionProperties.toIngestionProperties(database, tableName)
      case None => new IngestionProperties(database, tableName)
    }
  }

  private def streamRowsIntoKustoByWorkers(
      batchIdForTracing: String,
      rows: Iterator[InternalRow],
      ingestionProperties: IngestionProperties,
      parameters: KustoWriteResource): Unit = {
    val streamingClient = KustoClientCache
      .getClient(
        parameters.coordinates.clusterUrl,
        parameters.authentication,
        parameters.coordinates.ingestionUrl,
        parameters.coordinates.clusterAlias)
      .streamingClient

    val timeZone = TimeZone.getTimeZone(parameters.writeOptions.timeZone).toZoneId
    var byteArrayOutputStream = new ByteArrayOutputStreamWithOffset()
    var streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    var writer = new BufferedWriter(streamWriter)
    var csvWriter = CountingWriter(writer)
    var totalSize = 0L
    var lastIndex = 0
    for ((row, index) <- rows.zipWithIndex) {
      RowCSVWriterUtils.writeRowAsCSV(row, parameters.schema, timeZone, csvWriter)
      if (csvWriter.getCounter >= parameters.writeOptions.streamIngestUncompressedMaxSize) {
        KDSU.logWarn(
          className,
          s"Batch $batchIdForTracing exceeds the max streaming size ${parameters.writeOptions.streamIngestUncompressedMaxSize} " +
            s"MB compressed!.Streaming ${csvWriter.getCounter} bytes from batch $batchIdForTracing." +
            s"Index of the batch ($index).")
        writer.flush()
        streamWriter.flush()
        if (lastIndex != 0) {
          val firstBB = byteArrayOutputStream.toByteArray
          val bb2 = byteArrayOutputStream.createNewFromOffset(lastIndex)
          byteArrayOutputStream = bb2
          totalSize += lastIndex

          streamBytesIntoKusto(
            batchIdForTracing,
            firstBB,
            ingestionProperties,
            parameters.writeOptions,
            streamingClient,
            lastIndex)
          lastIndex = bb2.size()
          streamWriter = new OutputStreamWriter(byteArrayOutputStream)
          writer = new BufferedWriter(streamWriter)
          csvWriter = CountingWriter(writer, bb2.size())
        } else {
          KDSU.logInfo(
            className,
            s"Streaming one line as individual byte as the row size is ${csvWriter.getCounter}. Batch id: $batchIdForTracing.")
          streamBytesIntoKusto(
            batchIdForTracing,
            byteArrayOutputStream.getByteArrayOrCopy,
            ingestionProperties,
            parameters.writeOptions,
            streamingClient,
            byteArrayOutputStream.size())
          byteArrayOutputStream.reset()
          totalSize += csvWriter.getCounter
          csvWriter.resetCounter()
        }
      } else {
        writer.flush()
        lastIndex = byteArrayOutputStream.size()
      }
    }

    writer.flush()
    byteArrayOutputStream.flush()
    IOUtils.close(writer, byteArrayOutputStream)
    if (csvWriter.getCounter > 0) {
      KDSU.logInfo(
        className,
        s"Streaming final batch of ${csvWriter.getCounter} bytes from batch $batchIdForTracing.")
      totalSize += csvWriter.getCounter

      streamBytesIntoKusto(
        batchIdForTracing,
        byteArrayOutputStream.getByteArrayOrCopy,
        ingestionProperties,
        parameters.writeOptions,
        streamingClient,
        byteArrayOutputStream.size())
    }
    if (totalSize > WarnStreamingBytes) {
      KDSU.logWarn(
        className,
        s"Total of $totalSize bytes were ingested in the batch. Please consider 'Queued' writeMode for ingestion.")
    }
  }

  private def streamBytesIntoKusto(
      batchIdForTracing: String,
      bytes: Array[Byte],
      ingestionProperties: IngestionProperties,
      writeOptions: WriteOptions,
      streamingClient: ManagedStreamingIngestClient,
      inputStreamLastIdx: Int): Unit = {
    KDSU.retryApplyFunction(
      i => {
        val inputStream = new ByteArrayInputStream(bytes, 0, inputStreamLastIdx)
        val streamSourceInfo = new StreamSourceInfo(inputStream)
        Try(streamingClient.ingestFromStream(streamSourceInfo, ingestionProperties)) match {
          case Success(status) =>
            status.getIngestionStatusCollection.forEach(ingestionStatus => {
              KDSU.logInfo(
                className,
                s"BatchId $batchIdForTracing IngestionStatus { " +
                  s"status: '${ingestionStatus.status.toString}', " +
                  s"details: ${ingestionStatus.details}, " +
                  s"activityId: ${ingestionStatus.activityId}, " +
                  s"errorCode: ${ingestionStatus.errorCode}, " +
                  s"errorCodeString: ${ingestionStatus.errorCodeString}," +
                  s"retry: $i" +
                  "}")
            })
          case Failure(e: Throwable) =>
            KDSU.reportExceptionAndThrow(
              className,
              e,
              "Streaming ingestion in partition " +
                s"${TaskContext.getPartitionId().toString} for requestId: '${writeOptions.requestId} failed")
        }
      },
      this.retryConfig,
      "Streaming ingest to Kusto")
  }

  private def ingestToTemporaryTableByWorkers(
      batchIdForTracing: String,
      rows: Iterator[InternalRow],
      partitionsResults: CollectionAccumulator[PartitionResult],
      ingestionProperties: IngestionProperties,
      parameters: KustoWriteResource): Unit = {
    val partitionId = TaskContext.getPartitionId()
    KDSU.logInfo(
      className,
      s"Processing partition: '$partitionId' in requestId: '${parameters.writeOptions.requestId}'$batchIdForTracing")
    val clientCache = KustoClientCache.getClient(
      parameters.coordinates.clusterUrl,
      parameters.authentication,
      parameters.coordinates.ingestionUrl,
      parameters.coordinates.clusterAlias)
    val ingestClient = clientCache.ingestClient
    CloudInfo.retrieveCloudInfoForCluster(clientCache.ingestKcsb.getClusterUrl)

    val reqRetryOpts = new RequestRetryOptions(
      RetryPolicyType.FIXED,
      KCONST.QueueRetryAttempts,
      Duration.ofSeconds(KCONST.DefaultTimeoutQueueing),
      null,
      null,
      null)
    ingestClient.setQueueRequestOptions(reqRetryOpts)
    ingestRowsIntoKusto(
      rows,
      ingestClient,
      ingestionProperties,
      partitionsResults,
      batchIdForTracing,
      parameters)
  }

  private def createBlobWriter(
      kustoParameters: KustoWriteResource,
      client: ExtendedKustoClient,
      partitionId: String,
      blobNumber: Int,
      blobUUID: String): BlobWriteResource = {
    val now = Instant.now()
    val tmpTableName = kustoParameters.tmpTableName
    val tableCoordinates = kustoParameters.coordinates
    val blobName = s"${KustoQueryUtils.simplifyName(
        tableCoordinates.database)}_${tmpTableName}_${blobUUID}_${partitionId}_${blobNumber}_${formatter
        .format(now)}_spark.csv.gz"

    val containerAndSas =
      client.getTempBlobForIngestion(kustoParameters.writeOptions.maybeIngestionBlobStorage)

    val currentBlob = new CloudBlockBlob(
      new URI(s"${containerAndSas.containerUrl}/$blobName${containerAndSas.sas}"))
    val currentSas = containerAndSas.sas
    val options = new BlobRequestOptions()
    options.setConcurrentRequestCount(4) // Should be configured from outside
    val gzip: GZIPOutputStream = new GZIPOutputStream(
      currentBlob.openOutputStream(null, options, null))

    val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)

    val buffer: BufferedWriter = new BufferedWriter(writer, GzipBufferSize)
    val csvWriter = CountingWriter(buffer)
    BlobWriteResource(buffer, gzip, csvWriter, currentBlob, currentSas)
  }

  @throws[IOException]
  private[kusto] def ingestRows(
      rows: Iterator[InternalRow],
      parameters: KustoWriteResource,
      ingestClient: IngestClient,
      ingestionProperties: IngestionProperties,
      partitionsResults: CollectionAccumulator[PartitionResult],
      batchIdForTracing: String): Unit = {
    val partitionId = TaskContext.getPartitionId()
    val partitionIdString = TaskContext.getPartitionId().toString
    val taskMap = new ConcurrentHashMap[String, BlobWriteResource]()

    def ingest(
        blobResource: BlobWriteResource,
        sas: String,
        flushImmediately: Boolean = false,
        blobUUID: String,
        kustoClient: ExtendedKustoClient): Unit = {
      var props = ingestionProperties
      val blobUri = blobResource.blob.getStorageUri.getPrimaryUri.toString
      if (parameters.writeOptions.kustoCustomDebugWriteOptions.ensureNoDuplicatedBlobs || (!props.getFlushImmediately && flushImmediately)) {
        props = SparkIngestionProperties.cloneIngestionProperties(ingestionProperties)
      }

      if (parameters.writeOptions.kustoCustomDebugWriteOptions.ensureNoDuplicatedBlobs) {
        val pref = KDSU.getDedupTagsPrefix(parameters.writeOptions.requestId, batchIdForTracing)
        val tag = pref + blobUUID
        val ingestIfNotExist = new util.ArrayList[String]
        ingestIfNotExist.addAll(props.getIngestIfNotExists)
        val ingestBy: util.List[String] = new util.ArrayList[String]
        ingestBy.addAll(props.getIngestByTags)

        ingestBy.add(tag)
        ingestIfNotExist.add(tag)
        props.setIngestByTags(ingestBy)
        props.setIngestIfNotExists(ingestIfNotExist)
      }

      if (!props.getFlushImmediately && flushImmediately) {
        props.setFlushImmediately(true)
      }
      val partitionsResult = KDSU.retryApplyFunction(
        i => {
          Try(
            ingestClient.ingestFromBlob(
              new BlobSourceInfo(blobUri + sas, CompressionType.gz, UUID.randomUUID()),
              props)) match {
            case Success(x) =>
              val blobUrlWithSas =
                s"${blobResource.blob.getStorageUri.getPrimaryUri.toString}${blobResource.sas}"
              val containerWithSas = new ContainerWithSas(blobUrlWithSas, null)
              kustoClient.reportIngestionResult(containerWithSas, success = true)
              x
            case Failure(e: Throwable) =>
              KDSU.reportExceptionAndThrow(
                className,
                e,
                s"Queueing blob for ingestion, retry number '$i', in partition " +
                  s"$partitionIdString for requestId: '${parameters.writeOptions.requestId}")
              val blobUrlWithSas =
                s"${blobResource.blob.getStorageUri.getPrimaryUri.toString}${blobResource.sas}"
              val containerWithSas = new ContainerWithSas(blobUrlWithSas, null)
              kustoClient.reportIngestionResult(containerWithSas, success = false)
              null
          }
        },
        this.retryConfig,
        "Ingest into Kusto")
      if (parameters.writeOptions.writeMode == WriteMode.Transactional) {
        partitionsResults.add(PartitionResult(partitionsResult, partitionId))
      }
      KDSU.logInfo(
        className,
        s"Queued blob for ingestion in partition $partitionIdString " +
          s"for requestId: '${parameters.writeOptions.requestId}")
    }
    val kustoClient = KustoClientCache.getClient(
      parameters.coordinates.clusterUrl,
      parameters.authentication,
      parameters.coordinates.ingestionUrl,
      parameters.coordinates.clusterAlias)
    val maxBlobSize = parameters.writeOptions.batchLimit * KCONST.OneMegaByte
    var curBlobUUID = UUID.randomUUID().toString
    val initialBlobWriter: BlobWriteResource =
      createBlobWriter(parameters, kustoClient, partitionIdString, 0, curBlobUUID)
    val timeZone = TimeZone.getTimeZone(parameters.writeOptions.timeZone).toZoneId
    val lastBlobWriter = rows.zipWithIndex.foldLeft[BlobWriteResource](initialBlobWriter) {
      case (blobWriter, row) =>
        RowCSVWriterUtils.writeRowAsCSV(row._1, parameters.schema, timeZone, blobWriter.csvWriter)

        val count = blobWriter.csvWriter.getCounter
        val shouldNotCommitBlockBlob = count < maxBlobSize
        if (shouldNotCommitBlockBlob) {
          blobWriter
        } else {
          if (parameters.writeOptions.kustoCustomDebugWriteOptions.ensureNoDuplicatedBlobs) {
            taskMap.put(curBlobUUID, blobWriter)
          } else {
            KDSU.logInfo(
              className,
              s"Sealing blob in partition $partitionIdString for requestId: '${parameters.writeOptions.requestId}', " +
                s"blob number ${row._2}, with size $count")
            finalizeBlobWrite(blobWriter)
            ingest(
              blobWriter,
              blobWriter.sas,
              flushImmediately =
                !parameters.writeOptions.kustoCustomDebugWriteOptions.disableFlushImmediately,
              curBlobUUID,
              kustoClient)
            curBlobUUID = UUID.randomUUID().toString
            createBlobWriter(parameters, kustoClient, partitionIdString, row._2, curBlobUUID)
          }
        }
    }

    KDSU.logInfo(
      className,
      s"Finished serializing rows in partition $partitionIdString for " +
        s"requestId: '${parameters.writeOptions.requestId}' ")
    finalizeBlobWrite(lastBlobWriter)
    if (lastBlobWriter.csvWriter.getCounter > 0) {
      if (parameters.writeOptions.kustoCustomDebugWriteOptions.ensureNoDuplicatedBlobs) {
        taskMap.put(curBlobUUID, lastBlobWriter)
      } else {
        ingest(
          lastBlobWriter,
          lastBlobWriter.sas,
          flushImmediately = false,
          curBlobUUID,
          kustoClient)
      }
    }
    if (parameters.writeOptions.kustoCustomDebugWriteOptions.ensureNoDuplicatedBlobs && taskMap
        .size() > 0) {
      taskMap.forEach((uuid, bw) => {
        ingest(bw, bw.sas, flushImmediately = false, uuid, kustoClient)
      })
    }
  }

  def finalizeBlobWrite(blobWriteResource: BlobWriteResource): Unit = {
    blobWriteResource.writer.flush()
    blobWriteResource.gzip.flush()
    blobWriteResource.writer.close()
    blobWriteResource.gzip.close()
  }
}

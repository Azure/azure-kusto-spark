// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import com.fasterxml.jackson.databind.JsonNode
import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.data.auth.CloudInfo
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas
import com.microsoft.azure.kusto.ingest.result.IngestionResult
import com.microsoft.azure.kusto.ingest.source.{BlobSourceInfo, CompressionType, StreamSourceInfo}
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestionProperties, ManagedStreamingIngestClient}
import com.microsoft.azure.storage.blob.{BlobRequestOptions, CloudBlockBlob}
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.FinalizeHelper.finalizeIngestionWhenWorkersSucceeded
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableGetSchemaAsRowsCommand
import com.microsoft.kusto.spark.utils.KustoConstants.{IngestSkippedTrace, MaxIngestRetryAttempts, WarnStreamingBytes}
import com.microsoft.kusto.spark.utils.{ByteArrayOutputStreamWithOffset, ExtendedKustoClient, KustoClientCache, KustoIngestionUtils, KustoQueryUtils, KustoConstants => KCONST, KustoDataSourceUtils => KDSU}
import io.github.resilience4j.retry.RetryConfig
import org.apache.commons.io.IOUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.security.InvalidParameterException
import java.time.{Clock, Duration, Instant}
import java.util
import java.util.zip.GZIPOutputStream
import java.util.{TimeZone, UUID}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap

object KustoWriter {
  private val className = this.getClass.getSimpleName
  val TempIngestionTablePrefix = "sparkTempTable_"
  val DelayPeriodBetweenCalls: Int = KCONST.DefaultPeriodicSamplePeriod.toMillis.toInt
  private val GzipBufferSize: Int = 1000 * KCONST.DefaultBufferSize
  private val retryConfig = RetryConfig.custom
    .maxAttempts(MaxIngestRetryAttempts)
    .retryExceptions(classOf[IngestionServiceException])
    .build
  private val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("HH-mm-ss-SSSSSS").withZone(ZoneId.systemDefault)

  private[kusto] def write(
      batchId: Option[Long],
      data: DataFrame,
      tableCoordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions,
      crp: ClientRequestProperties): Unit = {
    val batchIdIfExists = batchId.map(b => s"${b.toString}").getOrElse("")
    val kustoClient = KustoClientCache.getClient(
      tableCoordinates.clusterUrl,
      authentication,
      tableCoordinates.ingestionUrl,
      tableCoordinates.clusterAlias)

    val table = tableCoordinates.table.get
    // TODO put data.sparkSession.sparkContext.appName in client app name
    val tmpTableName: String = KDSU.generateTempTableName(
      data.sparkSession.sparkContext.appName,
      table,
      writeOptions.requestId,
      batchIdIfExists,
      writeOptions.userTempTableName)

    val stagingTableIngestionProperties = getSparkIngestionProperties(writeOptions)
    val schemaShowCommandResult = kustoClient
      .executeEngine(
        tableCoordinates.database,
        generateTableGetSchemaAsRowsCommand(tableCoordinates.table.get),
        "schemaShow",
        crp)
      .getPrimaryResults

    val targetSchema =
      schemaShowCommandResult.getData.asScala.map(c => c.get(0).asInstanceOf[JsonNode]).toArray

    KustoIngestionUtils.adjustSchema(
      writeOptions.writeMode,
      writeOptions.adjustSchema,
      data.schema,
      targetSchema,
      stagingTableIngestionProperties,
      writeOptions.tableCreateOptions,
      writeOptions.kustoCustomDebugWriteOptions)

    val rebuiltOptions =
      writeOptions.copy(maybeSparkIngestionProperties = Some(stagingTableIngestionProperties))

    val tableExists = schemaShowCommandResult.count() > 0
    val shouldIngest = kustoClient.shouldIngestData(
      tableCoordinates,
      writeOptions.maybeSparkIngestionProperties,
      tableExists,
      crp)

    if (!shouldIngest) {
      KDSU.logInfo(className, s"$IngestSkippedTrace '$table'")
    } else {
      if (writeOptions.userTempTableName.isDefined) {
        if (kustoClient
            .executeEngine(
              tableCoordinates.database,
              generateTableGetSchemaAsRowsCommand(writeOptions.userTempTableName.get),
              "schemaShow",
              crp)
            .getPrimaryResults
            .count() <= 0 ||
          !tableExists) {
          throw new InvalidParameterException(
            "Temp table name provided but the table does not exist. Either drop this " +
              "option or create the table beforehand.")
        }
      } else {
        // KustoWriter will create a temporary table ingesting the data to it.
        // Only if all executors succeeded the table will be appended to the original destination table.
        kustoClient.initializeTablesBySchema(
          tableCoordinates,
          tmpTableName,
          data.schema,
          targetSchema,
          writeOptions,
          crp,
          stagingTableIngestionProperties.creationTime == null)
      }

      if (writeOptions.writeMode == WriteMode.Transactional) {
        kustoClient.setMappingOnStagingTableIfNeeded(
          stagingTableIngestionProperties,
          tableCoordinates.database,
          tmpTableName,
          table,
          crp)
      }

      if (stagingTableIngestionProperties.flushImmediately) {
        KDSU.logWarn(
          className,
          "It's not recommended to set flushImmediately to true on production")
      }
      val cloudInfo = CloudInfo.retrieveCloudInfoForCluster(kustoClient.ingestKcsb.getClusterUrl)
      val rdd = data.queryExecution.toRdd
      val partitionsResults = rdd.sparkContext.collectionAccumulator[PartitionResult]
      val parameters = KustoWriteResource(
        authentication = authentication,
        coordinates = tableCoordinates,
        schema = data.schema,
        writeOptions = rebuiltOptions,
        tmpTableName = tmpTableName,
        cloudInfo = cloudInfo)
      val sinkStartTime = getCreationTime(stagingTableIngestionProperties)
      if (writeOptions.isAsync) {
        val asyncWork = rdd.foreachPartitionAsync { rows =>
          ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults, parameters)
        }
        KDSU.logInfo(className, s"asynchronous write to Kusto table '$table' in progress")
        // This part runs back on the driver

        if (writeOptions.writeMode == WriteMode.Transactional) {
          asyncWork.onComplete {
            case Success(_) =>
              finalizeIngestionWhenWorkersSucceeded(
                tableCoordinates,
                batchIdIfExists,
                tmpTableName,
                partitionsResults,
                writeOptions,
                crp,
                tableExists,
                rdd.sparkContext,
                authentication,
                kustoClient,
                sinkStartTime)
            case Failure(exception) =>
              if (writeOptions.userTempTableName.isEmpty) {
                kustoClient.cleanupIngestionByProducts(
                  tableCoordinates.database,
                  tmpTableName,
                  crp)
              }
              KDSU.reportExceptionAndThrow(
                className,
                exception,
                "writing data",
                tableCoordinates.clusterUrl,
                tableCoordinates.database,
                table,
                shouldNotThrow = true)
              KDSU.logError(
                className,
                "The exception is not visible in the driver since we're in async mode")
          }
        }
      } else {
        try
          rdd.foreachPartition { rows =>
            ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults, parameters)
          }
        catch {
          case exception: Exception =>
            if (writeOptions.writeMode == WriteMode.Transactional) {
              if (writeOptions.userTempTableName.isEmpty) {
                kustoClient.cleanupIngestionByProducts(
                  tableCoordinates.database,
                  tmpTableName,
                  crp)
              }
            }
            /* Throwing the exception will abort the job (explicitly on the driver) */
            throw exception
        }
        if (writeOptions.writeMode == WriteMode.Transactional) {
          finalizeIngestionWhenWorkersSucceeded(
            tableCoordinates,
            batchIdIfExists,
            tmpTableName,
            partitionsResults,
            writeOptions,
            crp,
            tableExists,
            rdd.sparkContext,
            authentication,
            kustoClient,
            sinkStartTime)
        }
      }
    }
  }

  private def getCreationTime(ingestionProperties: SparkIngestionProperties): Instant = {
    Option(ingestionProperties.creationTime) match {
      case Some(creationTimeVal) => creationTimeVal
      case None => Instant.now(Clock.systemUTC())
    }
  }

  private def ingestRowsIntoTempTbl(
      rows: Iterator[InternalRow],
      batchIdForTracing: String,
      partitionsResults: CollectionAccumulator[PartitionResult],
      parameters: KustoWriteResource): Unit = {
    if (rows.isEmpty) {
      KDSU.logWarn(
        className,
        s"sink to Kusto table '${parameters.coordinates.table.get}' with no rows to write " +
          s"on partition ${TaskContext.getPartitionId} $batchIdForTracing")
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
    // Transactional mode write into the temp table instead of the destination table
    if (parameters.writeOptions.writeMode == WriteMode.Transactional) {
      ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE)
      ingestionProperties.setReportLevel(
        IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
    }
    ingestionProperties.setDataFormat(DataFormat.CSV.name)
    /* A try block may be redundant here. An exception thrown has to be propagated depending on the exception */
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

  private def getIngestionProperties(
      writeOptions: WriteOptions,
      database: String,
      tableName: String): IngestionProperties = {
    writeOptions.maybeSparkIngestionProperties match {
      case Some(sparkIngestionProperties) =>
        sparkIngestionProperties.toIngestionProperties(database, tableName)
      case None => new IngestionProperties(database, tableName)
    }
  }

  private def getSparkIngestionProperties(
      writeOptions: WriteOptions): SparkIngestionProperties = {
    val sparkIngestionProperties =
      writeOptions.maybeSparkIngestionProperties.getOrElse(new SparkIngestionProperties())
    sparkIngestionProperties.ingestIfNotExists = new util.ArrayList()
    sparkIngestionProperties
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
    // TODO - use a pool of two streams?
    //    var curBbId = 0
    //    val byteArrayPool = Array[ByteArrayOutputStream](new ByteArrayOutputStream(), null)// Init the 2nd lazy.
    //    val byteArrayOutputStream = byteArrayPool[]
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
          // Split the byteArrayOutputStream into two - we need actually that the one we write to will be reset
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
            lastIndex
          ) // =4mb-size(last row)
          lastIndex = bb2.size()
          // TODO Is it really better > (other option is to copy the data from the stream to a new stream - which i try to avoid)?
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
        // flush before counting output size
        writer.flush()
        lastIndex = byteArrayOutputStream
          .size() // TODO Can i simply use csvWriter.getCounter without flush? (we count all bytes and no transformation is done)
      }
    }

    // Close all resources
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
        // The SDK will compress the stream by default.
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
                s"${TaskContext.getPartitionId.toString} for requestId: '${writeOptions.requestId} failed")
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
    val partitionId = TaskContext.getPartitionId
    KDSU.logInfo(
      className,
      s"Processing partition: '$partitionId' in requestId: '${parameters.writeOptions.requestId}'$batchIdForTracing")
    val clientCache = KustoClientCache.getClient(
      parameters.coordinates.clusterUrl,
      parameters.authentication,
      parameters.coordinates.ingestionUrl,
      parameters.coordinates.clusterAlias)
    val ingestClient = clientCache.ingestClient
    CloudInfo.manuallyAddToCache(clientCache.ingestKcsb.getClusterUrl, parameters.cloudInfo)

    val reqRetryOpts = new RequestRetryOptions(
      RetryPolicyType.FIXED,
      KCONST.QueueRetryAttempts,
      Duration.ofSeconds(KCONST.DefaultTimeoutQueueing),
      null,
      null,
      null)
    ingestClient.setQueueRequestOptions(reqRetryOpts)
    // We force blocking here, since the driver can only complete the ingestion process
    // once all partitions are ingested into the temporary table
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
    val partitionId = TaskContext.getPartitionId
    val partitionIdString = TaskContext.getPartitionId.toString
    val taskMap = new ConcurrentHashMap[String, BlobWriteResource]()

    def ingest(
        blobResource: BlobWriteResource,
        size: Long,
        sas: String,
        flushImmediately: Boolean = false,
        blobUUID: String,
        kustoClient: ExtendedKustoClient): Unit = {
      var props = ingestionProperties
      val blobUri = blobResource.blob.getStorageUri.getPrimaryUri.toString
      if (parameters.writeOptions.kustoCustomDebugWriteOptions.ensureNoDuplicatedBlobs || (!props.getFlushImmediately && flushImmediately)) {
        // Need to copy the maybeSparkIngestionProperties so that only this blob ingestion will be effected
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
      // write the data here
      val partitionsResult = KDSU.retryApplyFunction(
        i => {
          Try(
            ingestClient.ingestFromBlob(
              new BlobSourceInfo(blobUri + sas, CompressionType.gz, UUID.randomUUID()),
              props)) match {
            case Success(x) =>
              <!-- The statuses of the ingestion operations are now set in the ingestion result -->
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
    // This blobWriter will be used later to write the rows to blob storage from which it will be ingested to Kusto
    val initialBlobWriter: BlobWriteResource =
      createBlobWriter(parameters, kustoClient, partitionIdString, 0, curBlobUUID)
    val timeZone = TimeZone.getTimeZone(parameters.writeOptions.timeZone).toZoneId
    // Serialize rows to ingest and send to blob storage.
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
              blobWriter.csvWriter.getCounter,
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
          lastBlobWriter.csvWriter.getCounter,
          lastBlobWriter.sas,
          flushImmediately = false,
          curBlobUUID,
          kustoClient)
      }
    }
    if (parameters.writeOptions.kustoCustomDebugWriteOptions.ensureNoDuplicatedBlobs && taskMap
        .size() > 0) {
      taskMap.forEach((uuid, bw) => {
        ingest(bw, bw.csvWriter.getCounter, bw.sas, flushImmediately = false, uuid, kustoClient)
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

final case class BlobWriteResource(
    writer: BufferedWriter,
    gzip: GZIPOutputStream,
    csvWriter: CountingWriter,
    blob: CloudBlockBlob,
    sas: String)
final case class KustoWriteResource(
    authentication: KustoAuthentication,
    coordinates: KustoCoordinates,
    schema: StructType,
    writeOptions: WriteOptions,
    tmpTableName: String,
    cloudInfo: CloudInfo)

final case class PartitionResult(ingestionResult: IngestionResult, partitionId: Int)

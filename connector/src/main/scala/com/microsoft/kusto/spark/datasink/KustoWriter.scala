// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.data.auth.CloudInfo
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas
import com.microsoft.azure.kusto.ingest.result.IngestionResult
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
import com.microsoft.kusto.spark.datasource.{
  AuthMethod,
  TransientStorageCredentials,
  TransientStorageParameters
}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableGetSchemaAsRowsCommand
import com.microsoft.kusto.spark.utils.KustoConstants.{
  IngestSkippedTrace,
  MaxIngestRetryAttempts,
  WarnStreamingBytes
}
import com.microsoft.kusto.spark.utils.{
  ByteArrayOutputStreamWithOffset,
  ExtendedKustoClient,
  KustoClientCache,
  KustoIngestionUtils,
  KustoQueryUtils,
  KustoConstants => KCONST,
  KustoDataSourceUtils => KDSU
}
import io.github.resilience4j.retry.RetryConfig
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
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
import scala.jdk.CollectionConverters._
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
  private val objectMapper = new ObjectMapper()

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

    // Re-parse the schema JSON through the connector's (potentially shaded) ObjectMapper
    // to avoid ClassCastException when the Kusto SDK returns Jackson objects from a
    // different classloader (e.g. Databricks runtime provides unshaded Jackson while
    // the connector uber-jar shades it).
    val targetSchema =
      schemaShowCommandResult.getData.asScala
        .map(c => objectMapper.readTree(c.get(0).toString))
        .toArray

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
      val rdd = data.queryExecution.toRdd
      val partitionsResults = rdd.sparkContext.collectionAccumulator[PartitionResult]

      // For OneLake writes, ensure the endpoint is in the ABFS valid endpoints allowlist
      // on the driver side (propagated to executors via Spark's hadoop config distribution)
      rebuiltOptions.maybeTransientWriteStorage.foreach { transientStorage =>
        val hadoopConf = data.sparkSession.sparkContext.hadoopConfiguration
        ensureOneLakeEndpointAllowlisted(hadoopConf, transientStorage)
      }

      val parameters = KustoWriteResource(
        authentication = authentication,
        coordinates = tableCoordinates,
        schema = data.schema,
        writeOptions = rebuiltOptions,
        tmpTableName = tmpTableName)
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
    // Transactional mode write into the temp table instead of the destination table
    if (parameters.writeOptions.writeMode == WriteMode.Transactional) {
      ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE)
      ingestionProperties.setReportLevel(
        IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
    }
    ingestionProperties.setDataFormat(DataFormat.CSV.name)

    parameters.writeOptions.maybeTransientWriteStorage match {
      case Some(transientStorage) if transientStorage.storageCredentials.exists(_.isOneLake) =>
        // OneLake write path — uses Hadoop FileSystem (abfss) + ingestFromBlob with ;impersonate
        ingestRowsOneLake(
          rows,
          parameters,
          ingestClient,
          ingestionProperties,
          partitionsResults,
          batchIdForTracing,
          transientStorage)
      case Some(transientStorage) =>
        // Blob/ADLS2 with storageCredentials format — uses existing blob path with
        // credentials extracted from TransientStorageCredentials (SAS or ;impersonate)
        ingestRowsTransientBlob(
          rows,
          parameters,
          ingestClient,
          ingestionProperties,
          partitionsResults,
          batchIdForTracing,
          transientStorage)
      case None =>
        // Legacy path — uses DM-provided or IngestionStorageParameters containers
        ingestRows(
          rows,
          parameters,
          ingestClient,
          ingestionProperties,
          partitionsResults,
          batchIdForTracing)
    }
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
    // Pre-warm the CloudInfo cache on the executor to avoid an extra metadata
    // fetch during authentication. We call retrieveCloudInfoForCluster (which
    // caches internally) instead of manuallyAddToCache to avoid a direct
    // dependency on reactor.core.publisher.Mono, which is shaded in the
    // uber-jar and can cause NoSuchMethodError when an unshaded CloudInfo is
    // loaded from the Databricks/Spark runtime classpath.
    CloudInfo.retrieveCloudInfoForCluster(clientCache.ingestKcsb.getClusterUrl)

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

  private def createOneLakeWriter(
      kustoParameters: KustoWriteResource,
      oneLakeStorage: TransientStorageParameters,
      hadoopConf: Configuration,
      partitionId: String,
      blobNumber: Int,
      blobUUID: String): OneLakeWriteResource = {
    val now = Instant.now()
    val tmpTableName = kustoParameters.tmpTableName
    val tableCoordinates = kustoParameters.coordinates
    val blobName = s"${KustoQueryUtils.simplifyName(
        tableCoordinates.database)}_${tmpTableName}_${blobUUID}_${partitionId}_${blobNumber}_${formatter
        .format(now)}_spark.csv.gz"

    // Pick the first OneLake credential (round-robin could be added later)
    val cred = oneLakeStorage.storageCredentials
      .find(_.isOneLake)
      .getOrElse(
        throw new InvalidParameterException("No OneLake credential found in write storage"))

    // Write via abfss using Hadoop FileSystem (ambient AAD from Fabric Spark runtime)
    val abfssBase = cred.oneLakeAbfssBase
    val filePath = new Path(s"$abfssBase/$blobName")
    val fs = FileSystem.get(filePath.toUri, hadoopConf)
    val outputStream = fs.create(filePath, true)

    val gzip: GZIPOutputStream = new GZIPOutputStream(outputStream)
    val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)
    val buffer: BufferedWriter = new BufferedWriter(writer, GzipBufferSize)
    val csvWriter = CountingWriter(buffer)

    // Construct the https URL for ingestion (Kusto .ingest reads via https + ;impersonate)
    val httpsUrl = s"${cred.oneLakeUrl}/$blobName"
    OneLakeWriteResource(buffer, gzip, csvWriter, outputStream, httpsUrl)
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
          /*
          TODO: Param for Size in BlobSourceInfo is removed. We want to however keep the size in the blob name
          So at some point we have to see what is a workaround to add this. Perhaps V2 ?
           */
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

  /**
   * OneLake variant of ingestRows. Writes gzipped CSV to OneLake via Hadoop FileSystem (abfss),
   * then queues ingestion using the https URL with ;impersonate.
   */
  @throws[IOException]
  private[kusto] def ingestRowsOneLake(
      rows: Iterator[InternalRow],
      parameters: KustoWriteResource,
      ingestClient: IngestClient,
      ingestionProperties: IngestionProperties,
      partitionsResults: CollectionAccumulator[PartitionResult],
      batchIdForTracing: String,
      oneLakeStorage: TransientStorageParameters): Unit = {
    val partitionId = TaskContext.getPartitionId()
    val partitionIdString = partitionId.toString

    // Get Hadoop configuration — on the executor, new Configuration() picks up
    // configs distributed by Spark (core-site.xml, ABFS settings, etc.)
    val hadoopConf = new Configuration()

    // Ensure OneLake endpoint is in the ABFS valid endpoints allowlist
    ensureOneLakeEndpointAllowlisted(hadoopConf, oneLakeStorage)

    def ingestOneLake(
        resource: OneLakeWriteResource,
        flushImmediately: Boolean = false,
        blobUUID: String): Unit = {
      var props = ingestionProperties
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
              new BlobSourceInfo(resource.ingestUri, CompressionType.gz, UUID.randomUUID()),
              props)) match {
            case Success(x) => x
            case Failure(e: Throwable) =>
              KDSU.reportExceptionAndThrow(
                className,
                e,
                s"Queueing OneLake blob for ingestion, retry number '$i', in partition " +
                  s"$partitionIdString for requestId: '${parameters.writeOptions.requestId}")
              null
          }
        },
        this.retryConfig,
        "Ingest from OneLake into Kusto")
      if (parameters.writeOptions.writeMode == WriteMode.Transactional) {
        partitionsResults.add(PartitionResult(partitionsResult, partitionId))
      }
      KDSU.logInfo(
        className,
        s"Queued OneLake blob for ingestion in partition $partitionIdString " +
          s"for requestId: '${parameters.writeOptions.requestId}")
    }

    val maxBlobSize = parameters.writeOptions.batchLimit * KCONST.OneMegaByte
    var curBlobUUID = UUID.randomUUID().toString
    val initialWriter: OneLakeWriteResource =
      createOneLakeWriter(
        parameters,
        oneLakeStorage,
        hadoopConf,
        partitionIdString,
        0,
        curBlobUUID)
    val timeZone = TimeZone.getTimeZone(parameters.writeOptions.timeZone).toZoneId

    val lastWriter = rows.zipWithIndex.foldLeft[OneLakeWriteResource](initialWriter) {
      case (writer, row) =>
        RowCSVWriterUtils.writeRowAsCSV(row._1, parameters.schema, timeZone, writer.csvWriter)

        val count = writer.csvWriter.getCounter
        if (count < maxBlobSize) {
          writer
        } else {
          KDSU.logInfo(
            className,
            s"Sealing OneLake blob in partition $partitionIdString for requestId: '${parameters.writeOptions.requestId}', " +
              s"blob number ${row._2}, with size $count")
          finalizeOneLakeWrite(writer)
          ingestOneLake(
            writer,
            flushImmediately =
              !parameters.writeOptions.kustoCustomDebugWriteOptions.disableFlushImmediately,
            curBlobUUID)
          curBlobUUID = UUID.randomUUID().toString
          createOneLakeWriter(
            parameters,
            oneLakeStorage,
            hadoopConf,
            partitionIdString,
            row._2,
            curBlobUUID)
        }
    }

    KDSU.logInfo(
      className,
      s"Finished serializing rows to OneLake in partition $partitionIdString for " +
        s"requestId: '${parameters.writeOptions.requestId}' ")
    finalizeOneLakeWrite(lastWriter)
    if (lastWriter.csvWriter.getCounter > 0) {
      ingestOneLake(lastWriter, flushImmediately = false, curBlobUUID)
    }
  }

  private def finalizeOneLakeWrite(resource: OneLakeWriteResource): Unit = {
    resource.writer.flush()
    resource.gzip.flush()
    resource.writer.close()
    resource.gzip.close()
  }

  /**
   * Ensure OneLake endpoints are in the Hadoop ABFS valid endpoints allowlist on the executor.
   */
  private def ensureOneLakeEndpointAllowlisted(
      hadoopConf: Configuration,
      oneLakeStorage: TransientStorageParameters): Unit = {
    val endpointKey = "fs.azure.abfs.valid.endpoints"
    val current = Option(hadoopConf.get(endpointKey)).getOrElse("")
    val existingSet = current.split(',').map(_.trim).filter(_.nonEmpty).toSet
    val oneLakeEndpoints = oneLakeStorage.storageCredentials
      .filter(c => c != null && c.isOneLake)
      .map(_.oneLakeEndpoint)
      .filter(_ != null)
      .distinct
    val toAdd = oneLakeEndpoints.filter(ep => !existingSet.contains(ep))
    if (toAdd.nonEmpty) {
      val updated = (Seq(current).filter(_.nonEmpty) ++ toAdd).mkString(",")
      hadoopConf.set(endpointKey, updated)
      KDSU.logInfo(className, s"Added OneLake endpoints to $endpointKey: ${toAdd.mkString(",")}")
    }
  }

  /**
   * Blob/ADLS2 variant using TransientStorageCredentials (storageCredentials JSON format). Writes
   * gzipped CSV to blob via CloudBlockBlob using the SAS URL from the credentials, then queues
   * ingestion. Supports both SAS and ;impersonate auth.
   */
  @throws[IOException]
  private[kusto] def ingestRowsTransientBlob(
      rows: Iterator[InternalRow],
      parameters: KustoWriteResource,
      ingestClient: IngestClient,
      ingestionProperties: IngestionProperties,
      partitionsResults: CollectionAccumulator[PartitionResult],
      batchIdForTracing: String,
      transientStorage: TransientStorageParameters): Unit = {
    val partitionId = TaskContext.getPartitionId()
    val partitionIdString = partitionId.toString

    // Pick a credential (round-robin for multiple)
    val credIdx = partitionId % transientStorage.storageCredentials.length
    val cred = transientStorage.storageCredentials(credIdx)

    def createTransientBlobWriter(blobNumber: Int, blobUUID: String): BlobWriteResource = {
      val now = Instant.now()
      val tmpTableName = parameters.tmpTableName
      val tableCoordinates = parameters.coordinates
      val blobName =
        s"${KustoQueryUtils.simplifyName(tableCoordinates.database)}_${tmpTableName}_${blobUUID}_${partitionIdString}_${blobNumber}_${formatter
            .format(now)}_spark.csv.gz"

      // Construct blob URL with auth suffix
      val (containerUrl, sas) = cred.authMethod match {
        case AuthMethod.Sas =>
          val baseUrl = cred.sasUrl.split("\\?")(0)
          val sasKey = if (cred.sasKey.startsWith("?")) cred.sasKey else s"?${cred.sasKey}"
          (baseUrl, sasKey)
        case AuthMethod.Impersonation =>
          val baseUrl = cred.sasUrl.stripSuffix(TransientStorageParameters.ImpersonationString)
          (baseUrl, TransientStorageParameters.ImpersonationString)
        case AuthMethod.Key =>
          val url =
            s"https://${cred.storageAccountName}.blob.${transientStorage.endpointSuffix}/${cred.blobContainer}"
          (url, "") // Key auth handled differently
      }

      val currentBlob = new CloudBlockBlob(new URI(s"$containerUrl/$blobName$sas"))
      val options = new BlobRequestOptions()
      options.setConcurrentRequestCount(4)
      val gzip: GZIPOutputStream = new GZIPOutputStream(
        currentBlob.openOutputStream(null, options, null))
      val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)
      val buffer: BufferedWriter = new BufferedWriter(writer, GzipBufferSize)
      val csvWriter = CountingWriter(buffer)
      BlobWriteResource(buffer, gzip, csvWriter, currentBlob, sas)
    }

    def ingest(
        blobResource: BlobWriteResource,
        flushImmediately: Boolean = false,
        blobUUID: String): Unit = {
      var props = ingestionProperties
      val ingestUrl = blobResource.ingestUri
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
              new BlobSourceInfo(ingestUrl, CompressionType.gz, UUID.randomUUID()),
              props)) match {
            case Success(x) => x
            case Failure(e: Throwable) =>
              KDSU.reportExceptionAndThrow(
                className,
                e,
                s"Queueing transient blob for ingestion, retry number '$i', in partition " +
                  s"$partitionIdString for requestId: '${parameters.writeOptions.requestId}")
              null
          }
        },
        this.retryConfig,
        "Ingest transient blob into Kusto")
      if (parameters.writeOptions.writeMode == WriteMode.Transactional) {
        partitionsResults.add(PartitionResult(partitionsResult, partitionId))
      }
      KDSU.logInfo(
        className,
        s"Queued transient blob for ingestion in partition $partitionIdString " +
          s"for requestId: '${parameters.writeOptions.requestId}")
    }

    val maxBlobSize = parameters.writeOptions.batchLimit * KCONST.OneMegaByte
    var curBlobUUID = UUID.randomUUID().toString
    val initialWriter = createTransientBlobWriter(0, curBlobUUID)
    val timeZone = TimeZone.getTimeZone(parameters.writeOptions.timeZone).toZoneId

    val lastWriter = rows.zipWithIndex.foldLeft[BlobWriteResource](initialWriter) {
      case (writer, row) =>
        RowCSVWriterUtils.writeRowAsCSV(row._1, parameters.schema, timeZone, writer.csvWriter)
        val count = writer.csvWriter.getCounter
        if (count < maxBlobSize) {
          writer
        } else {
          KDSU.logInfo(
            className,
            s"Sealing transient blob in partition $partitionIdString for requestId: " +
              s"'${parameters.writeOptions.requestId}', blob number ${row._2}, with size $count")
          finalizeBlobWrite(writer)
          ingest(
            writer,
            flushImmediately =
              !parameters.writeOptions.kustoCustomDebugWriteOptions.disableFlushImmediately,
            curBlobUUID)
          curBlobUUID = UUID.randomUUID().toString
          createTransientBlobWriter(row._2, curBlobUUID)
        }
    }

    KDSU.logInfo(
      className,
      s"Finished serializing rows to transient blob in partition $partitionIdString for " +
        s"requestId: '${parameters.writeOptions.requestId}' ")
    finalizeBlobWrite(lastWriter)
    if (lastWriter.csvWriter.getCounter > 0) {
      ingest(lastWriter, flushImmediately = false, curBlobUUID)
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
    sas: String) {

  /** Blob URI for ingestion submission */
  def blobUri: String = blob.getStorageUri.getPrimaryUri.toString

  /** Full URI including auth suffix (SAS or ;impersonate) */
  def ingestUri: String = s"$blobUri$sas"
}

final case class OneLakeWriteResource(
    writer: BufferedWriter,
    gzip: GZIPOutputStream,
    csvWriter: CountingWriter,
    outputStream: OutputStream,
    httpsUrl: String) {

  /** Full URI for ingestion submission — OneLake URL with ;impersonate */
  def ingestUri: String = s"$httpsUrl;impersonate"
}
final case class KustoWriteResource(
    authentication: KustoAuthentication,
    coordinates: KustoCoordinates,
    schema: StructType,
    writeOptions: WriteOptions,
    tmpTableName: String)

final case class PartitionResult(ingestionResult: IngestionResult, partitionId: Int)

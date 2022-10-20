package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.azure.kusto.ingest.result.IngestionResult
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestionProperties}
import com.microsoft.azure.storage.RetryNoRetry
import com.microsoft.azure.storage.blob.{BlobRequestOptions, CloudBlockBlob}
import com.microsoft.azure.storage.queue.QueueRequestOptions
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.exceptions.TimeoutAwaitingPendingOperationException
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableGetSchemaAsRowsCommand
import com.microsoft.kusto.spark.utils.KustoConstants.{IngestSkippedTrace, MaxIngestRetryAttempts}
import com.microsoft.kusto.spark.utils.{ExtendedKustoClient, KustoAzureFsSetupCache, KustoClientCache, KustoIngestionUtils, KustoQueryUtils, KustoConstants => KCONST, KustoDataSourceUtils => KDSU}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator
import org.json.JSONObject
import io.github.resilience4j.retry.{Retry, RetryConfig}
import io.vavr.CheckedFunction0

import java.net.URI
import java.nio.charset.StandardCharsets
import java.security.InvalidParameterException
import java.util
import java.util.zip.GZIPOutputStream
import java.util.{TimeZone, UUID}

import com.microsoft.kusto.spark.datasink.FinalizeHelper.finalizeIngestionWhenWorkersSucceeded
import com.microsoft.kusto.spark.datasource.{TransientStorageCredentials, TransientStorageParameters}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.{DateTime, DateTimeZone}

import java.io.{BufferedWriter, IOException, OutputStreamWriter}
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Success, Try}

object KustoWriter {
  private val myName = this.getClass.getSimpleName
  val LegacyTempIngestionTablePrefix = "_tmpTable"
  val TempIngestionTablePrefix = "sparkTempTable_"
  val DelayPeriodBetweenCalls: Int = KCONST.DefaultPeriodicSamplePeriod.toMillis.toInt
  val GzipBufferSize: Int = 1000 * KCONST.DefaultBufferSize
  private val retryConfig = RetryConfig.custom.maxAttempts(MaxIngestRetryAttempts).retryExceptions(classOf[IngestionServiceException]).build

  private[kusto] def write(batchId: Option[Long],
                           data: DataFrame,
                           tableCoordinates: KustoCoordinates,
                           authentication: KustoAuthentication,
                           writeOptions: WriteOptions,
                           crp: ClientRequestProperties): Unit = {
    val batchIdIfExists = batchId.map(b => s",batch: ${b.toString}").getOrElse("")
    val kustoClient = KustoClientCache.getClient(tableCoordinates.clusterUrl, authentication, tableCoordinates.ingestionUrl, tableCoordinates.clusterAlias)

    val table = tableCoordinates.table.get
    // TODO put data.sparkSession.sparkContext.appName in client app name
    val tmpTableName: String = KDSU.generateTempTableName(data.sparkSession.sparkContext.appName, table,
      writeOptions.requestId, batchIdIfExists, writeOptions.userTempTableName)

    val stagingTableIngestionProperties = getSparkIngestionProperties(writeOptions)
    val schemaShowCommandResult = kustoClient.executeEngine(tableCoordinates.database,
      generateTableGetSchemaAsRowsCommand(tableCoordinates.table.get), crp).getPrimaryResults

    val targetSchema = schemaShowCommandResult.getData.asScala.map(c => c.get(0).asInstanceOf[JSONObject]).toArray
    KustoIngestionUtils.adjustSchema(writeOptions.adjustSchema, data.schema, targetSchema, stagingTableIngestionProperties)

    val rebuiltOptions = WriteOptions(
      writeOptions.pollingOnDriver,
      writeOptions.tableCreateOptions,
      writeOptions.isAsync,
      writeOptions.writeResultLimit,
      writeOptions.timeZone,
      writeOptions.timeout,
      Some(stagingTableIngestionProperties.toString()),
      writeOptions.batchLimit,
      writeOptions.requestId,
      writeOptions.autoCleanupTime,
      writeOptions.maxRetriesOnMoveExtents,
      writeOptions.minimalExtentsCountForSplitMerge,
      writeOptions.adjustSchema,
      writeOptions.isTransactionalMode,
      writeOptions.userTempTableName
    )

    implicit val parameters: KustoWriteResource = KustoWriteResource(authentication, tableCoordinates, data.schema,
      rebuiltOptions, tmpTableName)

    val tableExists = schemaShowCommandResult.count() > 0
    val shouldIngest = kustoClient.shouldIngestData(tableCoordinates, writeOptions.ingestionProperties, tableExists,
      crp)

    if (!shouldIngest) {
      KDSU.logInfo(myName, s"$IngestSkippedTrace '$table'")
    } else {
      if (writeOptions.userTempTableName.isDefined) {
        if (kustoClient.executeEngine(tableCoordinates.database,
          generateTableGetSchemaAsRowsCommand(writeOptions.userTempTableName.get), crp).getPrimaryResults.count() <= 0 ||
          !tableExists) {
          throw new InvalidParameterException("Temp table name provided but the table does not exist. Either drop this " +
            "option or create the table beforehand.")
        }
      } else {
        // KustoWriter will create a temporary table ingesting the data to it.
        // Only if all executors succeeded the table will be appended to the original destination table.
        kustoClient.initializeTablesBySchema(tableCoordinates, tmpTableName, data.schema, targetSchema, writeOptions,
          crp, stagingTableIngestionProperties.creationTime == null)
      }

      kustoClient.setMappingOnStagingTableIfNeeded(stagingTableIngestionProperties, tableCoordinates.database, tmpTableName, table, crp)
      if (stagingTableIngestionProperties.flushImmediately) {
        KDSU.logWarn(myName, "It's not recommended to set flushImmediately to true")
      }

      //    TODO remove until batching policy problem is good
      //      if (stagingTableIngestionProperties.flushImmediately) {
      //        KDSU.logWarn(myName, "It's not recommended to set flushImmediately to true on production")
      //      }

    /*if (writeOptions.isAsync) {
            ingestParquetDataIntoTempTbl(data,batchIdIfExists)
      }else {
        try
          ingestParquetDataIntoTempTbl(data,batchIdIfExists)
        catch {
          case exception: Exception => if (writeOptions.isTransactionalMode) {
            if (writeOptions.userTempTableName.isEmpty) {
              kustoClient.cleanupIngestionByProducts(
                tableCoordinates.database, tmpTableName, crp)
            }

            throw exception
          }
        }
        /*if (writeOptions.isTransactionalMode) {
          finalizeIngestionWhenWorkersSucceeded(
            tableCoordinates, batchIdIfExists, tmpTableName, partitionsResults, writeOptions,
            crp, tableExists, rdd.sparkContext, authentication, kustoClient)
        }*/
      }*/


      val rdd = data.queryExecution.toRdd
      val partitionsResults = rdd.sparkContext.collectionAccumulator[PartitionResult]
      if (writeOptions.isAsync) {
        val asyncWork = rdd.foreachPartitionAsync { rows => ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults) }
        KDSU.logInfo(myName, s"asynchronous write to Kusto table '$table' in progress")
        // This part runs back on the driver
        if (writeOptions.isTransactionalMode) {
          asyncWork.onSuccess {
            case _ => finalizeIngestionWhenWorkersSucceeded(
              tableCoordinates, batchIdIfExists, tmpTableName, partitionsResults,
              writeOptions, crp, tableExists, rdd.sparkContext, authentication, kustoClient)
          }
          asyncWork.onFailure {
            case exception: Exception =>
              if (writeOptions.userTempTableName.isEmpty) {
                kustoClient.cleanupIngestionByProducts(
                  tableCoordinates.database, tmpTableName, crp)
              }
              KDSU.reportExceptionAndThrow(myName, exception, "writing data", tableCoordinates.clusterUrl, tableCoordinates.database, table, shouldNotThrow = true)
              KDSU.logError(myName, "The exception is not visible in the driver since we're in async mode")
          }
        }
      } else {
        try
          rdd.foreachPartition { rows => ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults) }
        catch {
          case exception: Exception => if (writeOptions.isTransactionalMode) {
            if (writeOptions.userTempTableName.isEmpty) {
              kustoClient.cleanupIngestionByProducts(
                tableCoordinates.database, tmpTableName, crp)
            }

            throw exception
          }
        }
        if (writeOptions.isTransactionalMode) {
          finalizeIngestionWhenWorkersSucceeded(
          tableCoordinates, batchIdIfExists, tmpTableName, partitionsResults, writeOptions,
          crp, tableExists, rdd.sparkContext, authentication, kustoClient)
        }
      }
    }
  }

  def ingestParquetDataIntoTempTbl(data : DataFrame, batchIdForTracing: String)  (implicit parameters: KustoWriteResource): Unit = {
    if (data.isEmpty) {
      KDSU.logWarn(myName, s"sink to Kusto table '${parameters.coordinates.table.get}' with no rows to write on partition ${TaskContext.getPartitionId} $batchIdForTracing")
    } else {
      ingestToTemporaryTableByWorkers(batchIdForTracing, data)
    }
  }

  def ingestRowsIntoTempTbl(rows: Iterator[InternalRow], batchIdForTracing: String, partitionsResults: CollectionAccumulator[PartitionResult])
                           (implicit parameters: KustoWriteResource): Unit = {
    if (rows.isEmpty) {
      KDSU.logWarn(myName, s"sink to Kusto table '${parameters.coordinates.table.get}' with no rows to write on partition ${TaskContext.getPartitionId} $batchIdForTracing")
    } else {
      //ingestToTemporaryTableByWorkers(batchIdForTracing, rows, partitionsResults)
    }
  }

  def ingestParquetDataIntoKusto(data: DataFrame,
                                 ingestClient: IngestClient,
                                 batchIdForTracing: String)
                                (implicit parameters: KustoWriteResource): Unit = {
    import parameters._

    // Transactional mode write into the temp table instead of the destination table
    val ingestionProperties = getIngestionProperties(writeOptions,
      parameters.coordinates.database,
      if (writeOptions.isTransactionalMode) parameters.tmpTableName else parameters.coordinates.table.get)

    if (writeOptions.isTransactionalMode) {
      ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE)
      ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
    }
    ingestionProperties.setDataFormat(DataFormat.PARQUET.name)

    try {
      ingestParquetData(data, parameters, ingestClient, ingestionProperties)

      KDSU.logDebug(myName, s"Ingesting from - partition: ${TaskContext.getPartitionId()} requestId: '${writeOptions.requestId}' $batchIdForTracing")
    } catch {
      case e: Exception => if (writeOptions.isTransactionalMode) throw e
    }
  }

  def ingestRowsIntoKusto(rows: Iterator[InternalRow],
                          ingestClient: IngestClient,
                          partitionsResults: CollectionAccumulator[PartitionResult],
                          batchIdForTracing: String)
                         (implicit parameters: KustoWriteResource): Unit = {
   import parameters._

   // Transactional mode write into the temp table instead of the destination table
   val ingestionProperties = getIngestionProperties(writeOptions,
      parameters.coordinates.database,
      if (writeOptions.isTransactionalMode) parameters.tmpTableName else parameters.coordinates.table.get)

    if (writeOptions.isTransactionalMode) {
      ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE)
      ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
    }
    ingestionProperties.setDataFormat(DataFormat.CSV.name)

    try {
      val tasks = ingestRows(rows, parameters, ingestClient, ingestionProperties, partitionsResults)

      KDSU.logDebug(myName, s"Ingesting from ${if(tasks.size() == 1) "blob" else tasks.size() +
        " blobs"} - partition: ${TaskContext.getPartitionId()} requestId: '${writeOptions.requestId}' $batchIdForTracing")

      tasks.asScala.foreach(t => try {
        Await.result(t, KCONST.DefaultMaximumIngestionTime)
      } catch {
        case _: TimeoutException => throw new TimeoutAwaitingPendingOperationException(s"Timed out trying to ingest requestId: '${writeOptions.requestId}'")
      })
    } catch {
      case e: Exception => if(writeOptions.isTransactionalMode) throw e
    }
  }

  private def getIngestionProperties(writeOptions: WriteOptions, database: String, tableName: String): IngestionProperties = {
    if (writeOptions.ingestionProperties.isDefined) {
      val ingestionProperties = SparkIngestionProperties.fromString(writeOptions.ingestionProperties.get)
        .toIngestionProperties(database, tableName)

      ingestionProperties
    } else {
      new IngestionProperties(database, tableName)
    }
  }

  private def getSparkIngestionProperties(writeOptions: WriteOptions): SparkIngestionProperties = {
    val ingestionProperties = if (writeOptions.ingestionProperties.isDefined)
      SparkIngestionProperties.fromString(writeOptions.ingestionProperties.get)
    else
      new SparkIngestionProperties()
    ingestionProperties.ingestIfNotExists = new util.ArrayList()

    ingestionProperties
  }

  private def ingestToTemporaryTableByWorkers(
                                               batchIdForTracing: String,
                                               data: DataFrame)
                                             (implicit parameters: KustoWriteResource): Unit = {

    import parameters._
    val partitionId = TaskContext.getPartitionId
    KDSU.logInfo(myName, s"Processing partition: '$partitionId' in requestId: '${writeOptions.
      requestId}'$batchIdForTracing")
    val ingestClient = KustoClientCache.getClient(coordinates.clusterUrl, authentication, coordinates.ingestionUrl, coordinates.clusterAlias).ingestClient
    val queueRequestOptions = new QueueRequestOptions
    queueRequestOptions.setMaximumExecutionTimeInMs(KCONST.DefaultExecutionQueueing)
    queueRequestOptions.setTimeoutIntervalInMs(KCONST.DefaultTimeoutQueueing)
    queueRequestOptions.setRetryPolicyFactory(new RetryNoRetry)
    ingestClient.setQueueRequestOptions(queueRequestOptions)
    // We force blocking here, since the driver can only complete the ingestion process
    // once all partitions are ingested into the temporary table
    ingestParquetDataIntoKusto(data, ingestClient, batchIdForTracing)
  }

  def createBlobWriter(tableCoordinates: KustoCoordinates,
                       tmpTableName: String,
                       client: ExtendedKustoClient,
                       partitionId: String): BlobWriteResource = {
    val blobName = s"${KustoQueryUtils.simplifyName(tableCoordinates.database)}_${tmpTableName}_${UUID.randomUUID.toString}_${partitionId}_spark.csv.gz_bkp"

    val containerAndSas = client.getTempBlobForIngestion
    val currentBlob = new CloudBlockBlob(new URI(s"${containerAndSas.containerUrl}/$blobName${containerAndSas.sas}"))
    val currentSas = containerAndSas.sas
    val options = new BlobRequestOptions()
    options.setConcurrentRequestCount(4) // Should be configured from outside
    val gzip: GZIPOutputStream = new GZIPOutputStream(currentBlob.openOutputStream(null, options, null))

    val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)

    val buffer: BufferedWriter = new BufferedWriter(writer, GzipBufferSize)
    val csvWriter = CountingWriter(buffer)
    BlobWriteResource(buffer, gzip, csvWriter, currentBlob, currentSas)
  }

  private[kusto] def getWasbHadoopConfig(hadoopConfig: Configuration): Configuration = {
    hadoopConfig.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig
  }

  @throws[IOException]
  private[kusto] def ingestParquetData(data: DataFrame,
                                       parameters: KustoWriteResource,
                                       ingestClient: IngestClient,
                                       ingestionProperties: IngestionProperties): Unit
  = {
    val partitionId = TaskContext.getPartitionId
    val partitionIdString = TaskContext.getPartitionId.toString

    import parameters._
    val kustoClient = KustoClientCache.getClient(coordinates.clusterUrl, authentication, coordinates.ingestionUrl, coordinates.clusterAlias)

    val blobName = s"${KustoQueryUtils.simplifyName(coordinates.database)}_${tmpTableName}_${UUID.randomUUID.toString}_${partitionId}_spark.parquet"
    val containerAndSas = kustoClient.getTempBlobForIngestion
    val currentBlobURI = new URI(s"${containerAndSas.containerUrl}/$blobName${containerAndSas.sas}")
    //val currentBlob = new CloudBlockBlob(currentBlobURI)
    //val currentSas = containerAndSas.sas

    val storageCredentials = new TransientStorageCredentials()
    storageCredentials.parseSas(currentBlobURI.toString)

    // Set up the access in spark config
    val hadoopConfig = getWasbHadoopConfig(SparkSession.builder.getOrCreate().sparkContext.hadoopConfiguration)
    setUpAccessForAzureFS(hadoopConfig, storageCredentials)
    // Create the blob file path
    val blobRoot = s"wasbs://${storageCredentials.blobContainer}@${storageCredentials.storageAccountName}.blob.${storageCredentials.domainSuffix}"
    val blobNamePath = s"/$blobName"

    // Write the file
    data.write.parquet(s"$blobRoot$blobNamePath")

    // Get the list of files created. This will be used for ingestion
    val hdfs = FileSystem.get(currentBlobURI, hadoopConfig)
    val sourcePath = new Path(blobNamePath)
    val listOfFiles = new ListBuffer[String]()
    // List these files and add them to the list for processing
    Try(hdfs.listFiles(sourcePath, true)) match {
      case Success(remoteFileIterator) =>
        while (remoteFileIterator.hasNext) {
          val mayBeRemoteFile = Option(remoteFileIterator.next())
          mayBeRemoteFile match {
            case Some(remoteFile) => if (!"_SUCCESS".equals(remoteFile.getPath.getName)) listOfFiles.append(remoteFile.getPath.getName)
            case None => KDSU.logInfo(this.getClass.getName, s"Empty paths while parsing $sourcePath. " +
              "Will not be added for processing")
          }
        }
      // Cannot list these files. Error accessing the file system needs to be halted processing
      case Failure(exception) => throw exception
    }
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val listOfFilesToProcess = listOfFiles.toDF()
    val blobFileBase = s"https://${storageCredentials.storageAccountName}.blob.${storageCredentials.domainSuffix}/${storageCredentials.blobContainer}"

    listOfFilesToProcess.rdd.foreachPartition(partitionResult => {

      var props = ingestionProperties
      if (!ingestionProperties.getFlushImmediately) {
        // Need to copy the ingestionProperties so that only this one will be flushed immediately
        props = SparkIngestionProperties.cloneIngestionProperties(ingestionProperties)
        props.setFlushImmediately(true)
      }
      val parquetIngestor = new KustoParquetIngestor(ingestClient)
      //val parquetIngestor = new KustoParquetIngestor(ingestionProperties)
      partitionResult.foreach(rowIterator => {
        val fileName = rowIterator.getString(0)
        val fullPath = s"$blobFileBase$blobNamePath/$fileName"
        if (writeOptions.isTransactionalMode) {
          val blobSourceInfo = new BlobSourceInfo(fullPath)
          ingestClient.ingestFromBlob(blobSourceInfo, props)
          //parquetIngestor.ingest(fullPath, props)
        }
      })
    })


    KDSU.logInfo(myName, s"finished ingesting data  to tempTable $partitionIdString for requestId: '${writeOptions.requestId}' ")
  }

  private[kusto] def setUpAccessForAzureFS(hadoopConfig: Configuration, storageCredentials: TransientStorageCredentials): Unit = {
    val now = DateTime.now(DateTimeZone.UTC)
    if (!storageCredentials.sasDefined) {
      if (!KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(storageCredentials.storageAccountName
        , storageCredentials.storageAccountKey, now)) {
        hadoopConfig.set(s"fs.azure.account.key.${storageCredentials.storageAccountName}." +
          s"blob.${storageCredentials.domainSuffix}",
          s"${storageCredentials.storageAccountKey}")
      }
    }
    else {
      if (!KustoAzureFsSetupCache.updateAndGetPrevSas(storageCredentials.blobContainer,
        storageCredentials.storageAccountName, storageCredentials.sasKey, now)) {
        hadoopConfig.set(s"fs.azure.sas.${storageCredentials.blobContainer}.${storageCredentials.storageAccountName}." +
          s"blob.${storageCredentials.domainSuffix}",
          s"${storageCredentials.sasKey}")
      }
    }
  }


@throws[IOException]
  private[kusto] def ingestRows(rows: Iterator[InternalRow],
                                parameters: KustoWriteResource,
                                ingestClient: IngestClient,
                                ingestionProperties: IngestionProperties,
                                partitionsResults: CollectionAccumulator[PartitionResult]): util.ArrayList[Future[Unit]]
  = {
    val partitionId = TaskContext.getPartitionId
    val partitionIdString = TaskContext.getPartitionId.toString
    def ingest(blob: CloudBlockBlob, size: Long, sas: String, flushImmediately: Boolean = false,
               transactional: Boolean, requestId: String): Future[Unit] = {
      Future {
        var props = ingestionProperties
        if (!ingestionProperties.getFlushImmediately && flushImmediately) {
          // Need to copy the ingestionProperties so that only this one will be flushed immediately
          props = SparkIngestionProperties.cloneIngestionProperties(ingestionProperties)
          props.setFlushImmediately(true)
        }
        val blobUri = blob.getStorageUri.getPrimaryUri.toString
        val blobPath = blobUri + sas
        val blobSourceInfo = new BlobSourceInfo(blobPath, size)

        if (transactional) {
          partitionsResults.add(
            PartitionResult(KDSU.retryFunction(() => {
              KDSU.logInfo(myName, s"Queued blob for ingestion in partition $partitionIdString for requestId: '$requestId}")
              ingestClient.ingestFromBlob(blobSourceInfo, props)
            }, this.retryConfig, "Ingest into Kusto"),
              partitionId))
        }
      }
    }

    import parameters._

    val kustoClient = KustoClientCache.getClient(coordinates.clusterUrl, authentication, coordinates.ingestionUrl, coordinates.clusterAlias)
    val maxBlobSize = writeOptions.batchLimit * KCONST.OneMegaByte

    // This blobWriter will be used later to write the rows to blob storage from which it will be ingested to Kusto
    val initialBlobWriter: BlobWriteResource = createBlobWriter(coordinates, tmpTableName, kustoClient, partitionIdString)
    val timeZone = TimeZone.getTimeZone(writeOptions.timeZone).toZoneId

    val ingestionTasks: util.ArrayList[Future[Unit]] = new util.ArrayList()

    // Serialize rows to ingest and send to blob storage.
    val lastBlobWriter = rows.foldLeft[BlobWriteResource](initialBlobWriter) {
      case (blobWriter, row) =>
        RowCSVWriterUtils.writeRowAsCSV(row, schema, timeZone, blobWriter.csvWriter)

        val count = blobWriter.csvWriter.getCounter
        val shouldNotCommitBlockBlob = count < maxBlobSize
        if (shouldNotCommitBlockBlob) {
          blobWriter
        } else {
          KDSU.logInfo(myName, s"Sealing blob in partition $partitionIdString for requestId: '${writeOptions.requestId}', " +
            s"blob number ${ingestionTasks.size}, with size $count")
          finalizeBlobWrite(blobWriter)
          val task = ingest(blobWriter.blob, blobWriter.csvWriter.getCounter, blobWriter.sas, flushImmediately =
            true, writeOptions.isTransactionalMode, writeOptions.requestId)
          ingestionTasks.add(task)
          createBlobWriter(coordinates, tmpTableName, kustoClient, partitionIdString)
        }
    }

    KDSU.logInfo(myName, s"finished serializing rows in partition $partitionIdString for requestId: '${writeOptions.requestId}' ")
    finalizeBlobWrite(lastBlobWriter)
    if (lastBlobWriter.csvWriter.getCounter > 0) {
      ingestionTasks.add(ingest(lastBlobWriter.blob, lastBlobWriter.csvWriter.getCounter, lastBlobWriter.sas,
        flushImmediately = false, writeOptions.isTransactionalMode, writeOptions.requestId))
    }

    ingestionTasks
  }

  def finalizeBlobWrite(blobWriteResource: BlobWriteResource): Unit = {
    blobWriteResource.writer.flush()
    blobWriteResource.gzip.flush()
    blobWriteResource.writer.close()
    blobWriteResource.gzip.close()
  }
}

case class BlobWriteResource(writer: BufferedWriter, gzip: GZIPOutputStream, csvWriter: CountingWriter, blob: CloudBlockBlob, sas: String)

case class KustoWriteResource(authentication: KustoAuthentication,
                              coordinates: KustoCoordinates,
                              schema: StructType,
                              writeOptions: WriteOptions,
                              tmpTableName: String)

case class PartitionResult(ingestionResult: IngestionResult, partitionId: Int)

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.data.auth.CloudInfo
import com.microsoft.azure.kusto.ingest.IngestionProperties
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.FinalizeHelper.finalizeIngestionWhenWorkersSucceeded
import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import com.microsoft.kusto.spark.utils.{
  ExtendedKustoClient,
  KustoAzureFsSetupCache,
  KustoConstants,
  KustoQueryUtils,
  KustoDataSourceUtils => KDSU
}
import io.github.resilience4j.retry.RetryConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.CollectionAccumulator

import java.net.URI
import java.time.{Clock, Instant}
import java.util
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors}
import java.util.concurrent.atomic.AtomicReference

/**
 * Writes data to Kusto using Parquet serialization format.
 *
 * Uses Spark's built-in data.write.parquet() to produce Parquet part files on blob storage. A
 * background polling thread monitors the output directory and ingests each part file into Kusto
 * as soon as it appears, overlapping ingestion with the ongoing Spark write. This mirrors the CSV
 * path's immediate-ingest-on-blob-seal pattern (see KustoCSVWriter.ingestRows) but adapted for
 * Parquet where Spark controls file creation internally.
 */
object KustoParquetWriter {
  private val className = this.getClass.getSimpleName
  private val retryConfig = RetryConfig.custom
    .maxAttempts(KustoConstants.MaxIngestRetryAttempts)
    .retryExceptions(classOf[IngestionServiceException])
    .build
  private val PollIntervalMs = 2000L

  def write(
      data: DataFrame,
      tableCoordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions,
      crp: ClientRequestProperties,
      batchIdIfExists: String,
      tmpTableName: String,
      tableExists: Boolean,
      stagingTableIngestionProperties: SparkIngestionProperties,
      kustoClient: ExtendedKustoClient): Unit = {
    val table = tableCoordinates.table.get
    val sparkContext = data.sparkSession.sparkContext

    KDSU.logInfo(
      className,
      s"KustoParquetWriter.write invoked for table '$table'" +
        s", writeMode=${writeOptions.writeMode}, writeFormat=${writeOptions.writeFormat}" +
        s", tmpTable='$tmpTableName', requestId='${writeOptions.requestId}' $batchIdIfExists")

    // Get a temporary blob container for staging parquet files
    val containerAndSas =
      kustoClient.getTempBlobForIngestion(writeOptions.maybeIngestionBlobStorage)
    val creds = new TransientStorageCredentials(
      containerAndSas.containerUrl + containerAndSas.sas)

    // Configure Hadoop FS to write to Azure blob storage via WASB
    val hadoopConf = sparkContext.hadoopConfiguration
    configureWasbFs(hadoopConf, creds)

    // Build the blob root path where parquet files will be written
    val blobUUID = UUID.randomUUID().toString
    val dirName =
      s"${KustoQueryUtils.simplifyName(tableCoordinates.database)}_${tmpTableName}_${blobUUID}_spark_parquet"
    val blobRoot =
      s"wasbs://${creds.blobContainer}@${creds.storageAccountName}.blob.${creds.domainSuffix}"
    val parquetDir = s"$blobRoot/$dirName"

    // Build the base HTTPS URL for ingestion (Kusto requires https:// blob URLs)
    val containerName = creds.blobContainer.split("/")(0)
    val ingestUrlBase =
      s"https://${creds.storageAccountName}.blob.${creds.domainSuffix}/$containerName"

    // Build ingestion properties
    val tableName =
      if (writeOptions.writeMode == WriteMode.Transactional) tmpTableName else table
    val ingestionProperties =
      getIngestionProperties(writeOptions, tableCoordinates.database, tableName)
    if (writeOptions.writeMode == WriteMode.Transactional) {
      ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE)
      ingestionProperties.setReportLevel(
        IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
    }
    ingestionProperties.setDataFormat(DataFormat.PARQUET)
    val existingProps = ingestionProperties.getAdditionalProperties
    if (existingProps != null) {
      existingProps.put("nativeParquetIngestion", "true")
    } else {
      val additionalProps = new util.HashMap[String, String]()
      additionalProps.put("nativeParquetIngestion", "true")
      ingestionProperties.setAdditionalProperties(additionalProps)
    }
    ingestionProperties.setFlushImmediately(true)

    // Pre-warm CloudInfo cache
    CloudInfo.retrieveCloudInfoForCluster(kustoClient.ingestKcsb.getClusterUrl)

    val partitionsResults = sparkContext.collectionAccumulator[PartitionResult]
    val ingestClient = kustoClient.ingestClient
    val parquetIngestor = new KustoParquetIngestor(ingestClient)

    // Track which files have already been ingested so the poller doesn't re-ingest
    val ingestedFiles = new util.HashSet[String]()
    // Queue of ingestion results collected by the background thread
    val ingestionResultQueue = new ConcurrentLinkedQueue[PartitionResult]()
    // Signal from the writer thread that data.write.parquet() has completed
    val writeCompleteLatch = new CountDownLatch(1)
    // Capture any exception from the background ingestion thread
    val ingestionError = new AtomicReference[Throwable](null)

    KDSU.logInfo(
      className,
      s"Writing Parquet data to '$parquetDir' for table '$table' " +
        s"requestId='${writeOptions.requestId}' $batchIdIfExists")

    // Start background thread that polls for new part files and ingests them immediately
    val executor = Executors.newSingleThreadExecutor()
    val ingestionFuture = executor.submit(new Runnable {
      override def run(): Unit = {
        try {
          val hdfs = FileSystem.get(new URI(blobRoot), hadoopConf)
          val dirPath = new Path(s"/$dirName")

          // Poll until the write is complete AND we've ingested all files
          var writeDone = false
          while (!writeDone || !Thread.currentThread().isInterrupted) {
            // Check for new part files
            try {
              if (hdfs.exists(dirPath)) {
                val newFiles = listNewParquetFiles(hdfs, dirPath, ingestedFiles)
                newFiles.foreach { fileName =>
                  val blobPath = s"$ingestUrlBase/$dirName/$fileName${creds.sasKey}"
                  KDSU.logInfo(
                    className,
                    s"[Incremental] Ingesting parquet blob '$fileName' for table '$tableName' " +
                      s"requestId='${writeOptions.requestId}' $batchIdIfExists")

                  val result = KDSU.retryApplyFunction(
                    _ => parquetIngestor.ingest(blobPath, ingestionProperties),
                    retryConfig,
                    "Ingest Parquet into Kusto")

                  ingestionResultQueue.add(PartitionResult(result, 0))
                  ingestedFiles.add(fileName)

                  KDSU.logInfo(
                    className,
                    s"[Incremental] Queued parquet blob '$fileName' for ingestion, " +
                      s"total ingested so far: ${ingestedFiles.size()} $batchIdIfExists")
                }
              }
            } catch {
              case _: java.io.FileNotFoundException =>
              // Directory doesn't exist yet, Spark hasn't started writing — keep polling
            }

            // Check if the Spark write has completed
            writeDone = writeCompleteLatch.getCount == 0
            if (!writeDone) {
              Thread.sleep(PollIntervalMs)
            } else {
              // Do one final scan to catch any files written between the last poll and completion
              try {
                if (hdfs.exists(dirPath)) {
                  val finalFiles = listNewParquetFiles(hdfs, dirPath, ingestedFiles)
                  finalFiles.foreach { fileName =>
                    val blobPath = s"$ingestUrlBase/$dirName/$fileName${creds.sasKey}"
                    KDSU.logInfo(
                      className,
                      s"[Final] Ingesting parquet blob '$fileName' for table '$tableName' " +
                        s"requestId='${writeOptions.requestId}' $batchIdIfExists")

                    val result = KDSU.retryApplyFunction(
                      _ => parquetIngestor.ingest(blobPath, ingestionProperties),
                      retryConfig,
                      "Ingest Parquet into Kusto")

                    ingestionResultQueue.add(PartitionResult(result, 0))
                    ingestedFiles.add(fileName)
                  }
                }
              } catch {
                case _: java.io.FileNotFoundException => // ignore
              }
              // Exit the loop — we're done
              return
            }
          }
        } catch {
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
          case e: Throwable =>
            KDSU.logError(className, s"Background ingestion thread failed: ${e.getMessage}")
            ingestionError.set(e)
        }
      }
    })

    // Run the Spark parquet write on the main thread
    try {
      data.write.parquet(parquetDir)
    } finally {
      // Signal the background thread that writing is done
      writeCompleteLatch.countDown()
    }

    KDSU.logInfo(
      className,
      s"Parquet write complete for table '$table', waiting for background ingestion to finish $batchIdIfExists")

    // Wait for the background ingestion thread to finish its final scan
    try {
      ingestionFuture.get()
    } finally {
      executor.shutdown()
    }

    // Check if the background thread encountered an error
    val bgError = ingestionError.get()
    if (bgError != null) {
      throw new RuntimeException(
        s"Background parquet ingestion failed for table '$table': ${bgError.getMessage}",
        bgError)
    }

    // Drain all results into the accumulator
    var result = ingestionResultQueue.poll()
    while (result != null) {
      if (writeOptions.writeMode == WriteMode.Transactional) {
        partitionsResults.add(result)
      }
      result = ingestionResultQueue.poll()
    }

    KDSU.logInfo(
      className,
      s"All ${ingestedFiles.size()} parquet blob(s) ingested for table '$tableName' " +
        s"requestId='${writeOptions.requestId}' $batchIdIfExists")

    // Finalize transactional write: poll results then move extents from temp table to target
    val sinkStartTime = getCreationTime(stagingTableIngestionProperties)
    if (writeOptions.writeMode == WriteMode.Transactional) {
      finalizeIngestionWhenWorkersSucceeded(
        tableCoordinates,
        batchIdIfExists,
        tmpTableName,
        partitionsResults,
        writeOptions,
        crp,
        tableExists,
        sparkContext,
        authentication,
        kustoClient,
        sinkStartTime)
    }
  }

  /** Configure Hadoop to access Azure Blob Storage via WASB protocol using SAS token. */
  private[kusto] def configureWasbFs(
      hadoopConf: Configuration,
      creds: TransientStorageCredentials): Unit = {
    val now = Instant.now(Clock.systemUTC())

    if (!KustoAzureFsSetupCache.updateAndGetPrevNativeAzureFs(now)) {
      hadoopConf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      hadoopConf.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      hadoopConf.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    }

    val sasToken =
      if (creds.sasKey.startsWith("?")) creds.sasKey.substring(1) else creds.sasKey

    if (!KustoAzureFsSetupCache.updateAndGetPrevSas(
        creds.blobContainer,
        creds.storageAccountName,
        sasToken,
        now)) {
      hadoopConf.set(
        s"fs.azure.sas.${creds.blobContainer}.${creds.storageAccountName}.blob.${creds.domainSuffix}",
        sasToken)
    }
  }

  /** List parquet part files in the directory that haven't been ingested yet. */
  private def listNewParquetFiles(
      hdfs: FileSystem,
      dirPath: Path,
      alreadyIngested: util.HashSet[String]): List[String] = {
    var newFiles = List.empty[String]
    val iterator = hdfs.listFiles(dirPath, true)
    while (iterator.hasNext) {
      val fileStatus = iterator.next()
      val name = fileStatus.getPath.getName
      // Skip Spark metadata files (_SUCCESS, _committed, _started) and already-ingested files
      if (!name.startsWith("_") && !alreadyIngested.contains(name)) {
        newFiles = name :: newFiles
      }
    }
    newFiles
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

  private def getCreationTime(ingestionProperties: SparkIngestionProperties): Instant = {
    Option(ingestionProperties.creationTime) match {
      case Some(creationTimeVal) => creationTimeVal
      case None => Instant.now(Clock.systemUTC())
    }
  }
}

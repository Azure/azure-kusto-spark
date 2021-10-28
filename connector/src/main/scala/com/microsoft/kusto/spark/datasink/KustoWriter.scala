package com.microsoft.kusto.spark.datasink

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.security.InvalidParameterException
import java.util
import java.util.zip.GZIPOutputStream
import java.util.{TimeZone, UUID}
import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.ingest.IngestionProperties.DATA_FORMAT
import com.microsoft.azure.kusto.ingest.result.IngestionResult
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestionProperties}
import com.microsoft.azure.storage.blob.{BlobRequestOptions, CloudBlockBlob}
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableGetSchemaAsRowsCommand
import com.microsoft.kusto.spark.utils.KustoConstants.IngestSkippedTrace
import com.microsoft.kusto.spark.utils.{KustoClient, KustoClientCache, KustoIngestionUtils, KustoQueryUtils, KustoConstants => KCONST, KustoDataSourceUtils => KDSU}
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator
import org.json.JSONObject

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, TimeoutException}

object KustoWriter {
  private val myName = this.getClass.getSimpleName
  val LegacyTempIngestionTablePrefix = "_tmpTable"
  val TempIngestionTablePrefix = "sparkTempTable_"
  val DelayPeriodBetweenCalls: Int = KCONST.DefaultPeriodicSamplePeriod.toMillis.toInt
  val GzipBufferSize: Int = 1000 * KCONST.DefaultBufferSize

  private[kusto] def write(batchId: Option[Long],
                           data: DataFrame,
                           tableCoordinates: KustoCoordinates,
                           authentication: KustoAuthentication,
                           writeOptions: WriteOptions,
                           crp: ClientRequestProperties): Unit = {
    val batchIdIfExists = batchId.map(b => s",batch: ${b.toString}").getOrElse("")
    val kustoClient = KustoClientCache.getClient(tableCoordinates.clusterAlias, tableCoordinates.clusterUrl, authentication)

    if (tableCoordinates.table.isEmpty) {
      KDSU.reportExceptionAndThrow(myName, new InvalidParameterException("Table name not specified"), "writing data",
        tableCoordinates.clusterUrl, tableCoordinates.database)
    }

    val table = tableCoordinates.table.get
    val tmpTableName: String = KustoQueryUtils.simplifyName(TempIngestionTablePrefix +
      data.sparkSession.sparkContext.appName +
      "_" + table + batchId.map(b => s"_${b.toString}").getOrElse("") + "_" + writeOptions.requestId)

    val stagingTableIngestionProperties = getSparkIngestionProperties(writeOptions)
    val schemaShowCommandResult = kustoClient.engineClient.execute(tableCoordinates.database,
      generateTableGetSchemaAsRowsCommand(tableCoordinates.table.get), crp).getPrimaryResults
    val targetSchema = schemaShowCommandResult.getData.asScala.map(c => c.get(0).asInstanceOf[JSONObject]).toArray
    KustoIngestionUtils.adjustSchema(writeOptions.adjustSchema, data.schema, targetSchema, stagingTableIngestionProperties)

    val rebuiltOptions = WriteOptions(writeOptions.tableCreateOptions, writeOptions.isAsync,
      writeOptions.writeResultLimit,
      writeOptions.timeZone, writeOptions.timeout,
      Some(stagingTableIngestionProperties.toString()),
      writeOptions.batchLimit,
      writeOptions.requestId,
      writeOptions.autoCleanupTime,
      writeOptions.maxRetriesOnMoveExtents,
      writeOptions.minimalExtentsCountForSplitMerge,
      writeOptions.adjustSchema)

    implicit val parameters: KustoWriteResource = KustoWriteResource(authentication, tableCoordinates, data.schema,
      rebuiltOptions, tmpTableName)

    val tableExists = schemaShowCommandResult.count() > 0
    val shouldIngest = kustoClient.shouldIngestData(tableCoordinates, writeOptions.IngestionProperties, tableExists,
      crp)

    if (!shouldIngest) {
      KDSU.logInfo(myName, s"$IngestSkippedTrace '$table'")
    } else {
      // KustoWriter will create a temporary table ingesting the data to it.
      // Only if all executors succeeded the table will be appended to the original destination table.
      kustoClient.initializeTablesBySchema(tableCoordinates, tmpTableName, data
        .schema, targetSchema, writeOptions, crp, stagingTableIngestionProperties.creationTime == null)
      KDSU.logInfo(myName, s"Successfully created temporary table $tmpTableName, will be deleted after completing the operation")

      kustoClient.setMappingOnStagingTableIfNeeded(stagingTableIngestionProperties, tableCoordinates.database, tmpTableName, table, crp)
      if (stagingTableIngestionProperties.flushImmediately) {
        KDSU.logWarn(myName, "Its not recommended to set flushImmediately to true")
      }
      val rdd = data.queryExecution.toRdd
      val partitionsResults = rdd.sparkContext.collectionAccumulator[PartitionResult]
      if (writeOptions.isAsync) {
        val asyncWork = rdd.foreachPartitionAsync { rows => ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults) }
        KDSU.logInfo(myName, s"asynchronous write to Kusto table '$table' in progress")
        // This part runs back on the driver
        asyncWork.onSuccess {
          case _ => kustoClient.finalizeIngestionWhenWorkersSucceeded(
            tableCoordinates, batchIdIfExists, kustoClient.engineClient, tmpTableName, partitionsResults,
            writeOptions,  crp, tableExists)
        }
        asyncWork.onFailure {
          case exception: Exception =>
            kustoClient.cleanupIngestionByProducts(
              tableCoordinates.database, kustoClient.engineClient, tmpTableName, crp)
            KDSU.reportExceptionAndThrow(myName, exception, "writing data", tableCoordinates.clusterUrl, tableCoordinates.database, table, shouldNotThrow = true)
            KDSU.logError(myName, "The exception is not visible in the driver since we're in async mode")
        }
      } else {
        try
          rdd.foreachPartition { rows => ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults) }
        catch {
          case exception: Exception =>
            kustoClient.cleanupIngestionByProducts(
              tableCoordinates.database, kustoClient.engineClient, tmpTableName, crp)
            throw exception
        }
        kustoClient.finalizeIngestionWhenWorkersSucceeded(
          tableCoordinates, batchIdIfExists, kustoClient.engineClient, tmpTableName, partitionsResults, writeOptions,
          crp, tableExists)
      }
    }
  }

  def ingestRowsIntoTempTbl(rows: Iterator[InternalRow], batchIdForTracing: String, partitionsResults: CollectionAccumulator[PartitionResult])
                           (implicit parameters: KustoWriteResource): Unit =
    if (rows.isEmpty) {
      KDSU.logWarn(myName, s"sink to Kusto table '${parameters.coordinates.table.get}' with no rows to write on partition ${TaskContext.getPartitionId} $batchIdForTracing")
    } else {
      ingestToTemporaryTableByWorkers(batchIdForTracing, rows, partitionsResults)
    }

  def ingestRowsIntoKusto(rows: Iterator[InternalRow],
                          ingestClient: IngestClient,
                          partitionsResults: CollectionAccumulator[PartitionResult],
                          batchIdForTracing: String)
                         (implicit parameters: KustoWriteResource): Unit = {
    import parameters._

    val ingestionProperties = getIngestionProperties(writeOptions, parameters.coordinates.database, parameters.tmpTableName)
    ingestionProperties.setDataFormat(DATA_FORMAT.csv.name)
    ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table)
    ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses)

    val tasks = ingestRows(rows, parameters, ingestClient, ingestionProperties, partitionsResults)

    KDSU.logInfo(myName, s"Ingesting from blob - partition: ${TaskContext.getPartitionId()} requestId: '${writeOptions.requestId}' $batchIdForTracing")

    tasks.asScala.foreach(t => try {
      Await.result(t, KCONST.DefaultIngestionTaskTime)
    } catch {
      case _: TimeoutException => KDSU.logWarn(myName, s"Timed out trying to ingest requestId: '${writeOptions.requestId}', no need to fail as the ingest might succeed")
    })
  }

  private def getIngestionProperties(writeOptions: WriteOptions, database: String, tableName: String): IngestionProperties = {
    if (writeOptions.IngestionProperties.isDefined) {
      val ingestionProperties = SparkIngestionProperties.fromString(writeOptions.IngestionProperties.get)
        .toIngestionProperties(database, tableName)

      ingestionProperties
    } else {
      new IngestionProperties(database, tableName)
    }
  }

  private def getSparkIngestionProperties(writeOptions: WriteOptions): SparkIngestionProperties = {
    val ingestionProperties = if (writeOptions.IngestionProperties.isDefined)
      SparkIngestionProperties.fromString(writeOptions.IngestionProperties.get)
    else
      new SparkIngestionProperties()
    ingestionProperties.ingestIfNotExists = new util.ArrayList()

    ingestionProperties
  }

  private def ingestToTemporaryTableByWorkers(
                                               batchIdForTracing: String,
                                               rows: Iterator[InternalRow],
                                               partitionsResults: CollectionAccumulator[PartitionResult])
                                             (implicit parameters: KustoWriteResource): Unit = {

    import parameters._
    val partitionId = TaskContext.getPartitionId
    KDSU.logInfo(myName, s"Ingesting partition: '$partitionId' in requestId: '${writeOptions.requestId}'$batchIdForTracing")
    val ingestClient = KustoClientCache.getClient(coordinates.clusterAlias, coordinates.clusterUrl, authentication).ingestClient

    // We force blocking here, since the driver can only complete the ingestion process
    // once all partitions are ingested into the temporary table
    ingestRowsIntoKusto(rows, ingestClient, partitionsResults, batchIdForTracing)
  }

  def createBlobWriter(tableCoordinates: KustoCoordinates,
                       tmpTableName: String,
                       client: KustoClient): BlobWriteResource = {
    val blobName = s"${KustoQueryUtils.simplifyName(tableCoordinates.database)}_${tmpTableName}_${UUID.randomUUID.toString}_spark.csv.gz"

    val containerAndSas = client.getTempBlobForIngestion
    val currentBlob = new CloudBlockBlob(new URI(containerAndSas.containerUrl + '/' + blobName + containerAndSas.sas))
    val currentSas = containerAndSas.sas
    val options = new BlobRequestOptions()
    options.setConcurrentRequestCount(4) // Should be configured from outside
    val gzip: GZIPOutputStream = new GZIPOutputStream(currentBlob.openOutputStream(null, options, null))

    val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)

    val buffer: BufferedWriter = new BufferedWriter(writer, GzipBufferSize)
    val csvWriter = CountingWriter(buffer)
    BlobWriteResource(buffer, gzip, csvWriter, currentBlob, currentSas)
  }

  @throws[IOException]
  private[kusto] def ingestRows(rows: Iterator[InternalRow],
                                parameters: KustoWriteResource,
                                ingestClient: IngestClient,
                                ingestionProperties: IngestionProperties,
                                partitionsResults: CollectionAccumulator[PartitionResult]): util.ArrayList[Future[Unit]]
  = {
    def ingest(blob: CloudBlockBlob, size: Long, sas: String, flushImmediately: Boolean = false): Future[Unit] = {
      val partitionId = TaskContext.getPartitionId
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

        partitionsResults.add(PartitionResult(ingestClient.ingestFromBlob(blobSourceInfo, props), partitionId))
      }
    }

    import parameters._

    val kustoClient = KustoClientCache.getClient(coordinates.clusterAlias, coordinates.clusterUrl, authentication)
    val maxBlobSize = writeOptions.batchLimit * KCONST.OneMegaByte

    // This blobWriter will be used later to write the rows to blob storage from which it will be ingested to Kusto
    val initialBlobWriter: BlobWriteResource = createBlobWriter(coordinates, tmpTableName, kustoClient)
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
          KDSU.logInfo(myName, s"Sealing blob in partition ${TaskContext.getPartitionId} for requestId: '${writeOptions.requestId}', " +
            s"blob number ${ingestionTasks.size}, with size $count")
          finalizeBlobWrite(blobWriter)
          val task = ingest(blobWriter.blob, blobWriter.csvWriter.getCounter, blobWriter.sas, flushImmediately = true)
          ingestionTasks.add(task)
          createBlobWriter(coordinates, tmpTableName, kustoClient)
        }
    }

    KDSU.logInfo(myName, s"finished serializing rows in partition ${TaskContext.getPartitionId} for requestId: '${writeOptions.requestId}' ")
    finalizeBlobWrite(lastBlobWriter)
    if (lastBlobWriter.csvWriter.getCounter > 0) {
      ingestionTasks.add(ingest(lastBlobWriter.blob, lastBlobWriter.csvWriter.getCounter, lastBlobWriter.sas))
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

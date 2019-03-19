package com.microsoft.kusto.spark.datasink


import java.io.{BufferedWriter, IOException, OutputStreamWriter}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPOutputStream
import java.util.{TimeZone, UUID}

import com.microsoft.azure.kusto.data.Client
import com.microsoft.azure.kusto.ingest.IngestionProperties.DATA_FORMAT
import com.microsoft.azure.kusto.ingest.result.{IngestionResult, IngestionStatus, OperationStatus}
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.IngestionProperties
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature
import com.microsoft.azure.storage.blob.{CloudBlobContainer, CloudBlockBlob}
import com.microsoft.kusto.spark.datasink
import com.microsoft.kusto.spark.datasource.{KustoAuthentication, KustoCoordinates, WriteOptions}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoClient, KustoQueryUtils, KustoConstants => KCONST, KustoDataSourceUtils => KDSU}
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DateType, StructField, StructType, TimestampType, StringType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.{FutureAction, TaskContext}
import shaded.parquet.org.codehaus.jackson.map.ObjectMapper

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}

object KustoWriter {
  private val myName = this.getClass.getSimpleName
  val TempIngestionTablePrefix = "_tmpTable"
  var OperationShowCommandResult = 16
  val completedState = "Completed"
  val inProgressState = "InProgress"
  val stateCol = "State"
  val statusCol = "Status"
  val delayPeriodBetweenCalls: Int = KCONST.defaultPeriodicSamplePeriod.toMillis.toInt
  val GZIP_BUFFER_SIZE: Int = KCONST.defaultBufferSize
  val maxBlobSize: Int = KCONST.defaultMaxBlobSize

  private[kusto] def write(batchId: Option[Long],
                           data: DataFrame,
                           tableCoordinates: KustoCoordinates,
                           authentication: KustoAuthentication, writeOptions: WriteOptions): Unit = {

    val batchIdIfExists = batchId.filter(_ != 0).map(_.toString).getOrElse("")
    val kustoAdminClient = KustoClient.getAdmin(authentication, tableCoordinates.cluster)
    val table = tableCoordinates.table.get
    val tmpTableName: String = KustoQueryUtils.simplifyName(TempIngestionTablePrefix +
      data.sparkSession.sparkContext.appName +
      "_" + table + batchIdIfExists + "_" + UUID.randomUUID().toString)
    implicit val parameters: KustoWriteResource = KustoWriteResource(authentication, tableCoordinates, data.schema, writeOptions, tmpTableName)

    cleanupTempTables(kustoAdminClient, tableCoordinates)

    // KustoWriter will create a temporary table ingesting the data to it.
    // Only if all executors succeeded the table will be appended to the original destination table.
    KDSU.createTmpTableWithSameSchema(kustoAdminClient, tableCoordinates, tmpTableName, writeOptions.tableCreateOptions, data.schema)
    KDSU.logInfo(myName, s"Successfully created temporary table $tmpTableName, will be deleted after completing the operation")
    val ingestKcsb = KustoClient.getKcsb(authentication, s"https://ingest-${tableCoordinates.cluster}.kusto.windows.net")
    val storageUri = KustoTempIngestStorageCache.getNewTempBlobReference(tableCoordinates.cluster, ingestKcsb)
    val rdd = data.queryExecution.toRdd
    if (writeOptions.isAsync) {
      val asyncWork: FutureAction[Unit] = rdd.foreachPartitionAsync { rows => ingestRowsIntoTempTbl(rows, batchIdIfExists, writeOptions.timeout, storageUri) }
      KDSU.logInfo(myName, s"asynchronous write to Kusto table '$table' in progress")
      // This part runs back on the driver
      asyncWork.onSuccess {
        case _ => finalizeIngestionWhenWorkersSucceeded(tableCoordinates, batchIdIfExists, kustoAdminClient, tmpTableName, isAsync = true)
      }
      asyncWork.onFailure {
        case exception: Exception =>
          tryCleanupIngestionByproducts(tableCoordinates.database, kustoAdminClient, tmpTableName)
          KDSU.reportExceptionAndThrow(myName, exception, "writing data", tableCoordinates.cluster, tableCoordinates.database, table, isLogDontThrow = true)
          KDSU.logError(myName, "The exception is not visible in the driver since we're in async mode")
      }
    }
    else {
      try {
        rdd.foreachPartition { rows => ingestRowsIntoTempTbl(rows, batchIdIfExists, writeOptions.timeout, storageUri) }
      } catch {
        case exception: Exception =>
          tryCleanupIngestionByproducts(tableCoordinates.database, kustoAdminClient, tmpTableName)
          throw exception.getCause
      }
      finalizeIngestionWhenWorkersSucceeded(tableCoordinates, batchIdIfExists, kustoAdminClient, tmpTableName)
      KDSU.logInfo(myName, s"write operation to Kusto table '$table' finished successfully")
    }
  }


  def ingestRowsIntoTempTbl(rows: Iterator[InternalRow], batchId: String, timeOut: FiniteDuration, storageUri: String)
                           (implicit parameters: KustoWriteResource): Unit =
    if (rows.isEmpty) {
      KDSU.logWarn(myName, s"sink to Kusto table '${parameters.coordinates.table}' with no rows to write on partition ${TaskContext.getPartitionId}")
    } else {
      ingestToTemporaryTableByWorkers(batchId, timeOut, storageUri, rows)
    }

  def cleanupTempTables(kustoAdminClient: Client, coordinates: KustoCoordinates): Unit = {

    val tempTablesOld: Seq[String] =
      kustoAdminClient.execute(generateFindOldTempTablesCommand(coordinates.database))
        .getValues.asScala
        .headOption.map(_.asScala)
        .getOrElse(Seq())

    Future {
      // Try delete temporary tablesToCleanup created and not used
      val tempTablesCurr: Seq[String] = kustoAdminClient.execute(coordinates.database, generateFindCurrentTempTablesCommand(TempIngestionTablePrefix))
        .getValues.get(0).asScala

      val tablesToCleanup: Seq[String] = tempTablesOld.intersect(tempTablesCurr)

      if (tablesToCleanup.nonEmpty) {
        kustoAdminClient.execute(coordinates.database, generateDropTablesCommand(tablesToCleanup.mkString(",")))
      }
    } onFailure {
      case ex: Exception =>
        KDSU.reportExceptionAndThrow(
          myName,
          ex,
          "trying to drop temporary tables", coordinates.cluster, coordinates.database, coordinates.table.getOrElse("Unspecified table name"),
          isLogDontThrow = true
        )
    }
  }

  def ingestRowsIntoKusto(rows: Iterator[InternalRow],
                          storageUri: String)
                         (implicit parameters: KustoWriteResource): Seq[IngestionResult] = {
    import parameters._

    val ingestClient = KustoClient.getIngest(authentication, coordinates.cluster)
    val ingestionProperties = new IngestionProperties(coordinates.database, tmpTableName)

    val blobList: Seq[CloudBlockBlob] = serializeRows(rows, storageUri, parameters)

    blobList.map { blob =>
      val signature = blob.getServiceClient.getCredentials.asInstanceOf[StorageCredentialsSharedAccessSignature]
      val blobPath = blob.getStorageUri.getPrimaryUri.toString + "?" + signature.getToken
      val blobSourceInfo = new BlobSourceInfo(blobPath)

      ingestionProperties.setDataFormat(DATA_FORMAT.csv.name)
      ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table)
      ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses)
      ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties)
    }
  }

  private def tryCleanupIngestionByproducts(database: String, kustoAdminClient: Client, tmpTableName: String) = {
    try {
      kustoAdminClient.execute(database, generateTableDropCommand(tmpTableName))
    }
    catch {
      case exception: Exception =>
        KDSU.reportExceptionAndThrow(myName, exception, s"deleting temporary table $tmpTableName", database = database, isLogDontThrow = true)
    }
  }

  private def finalizeIngestionWhenWorkersSucceeded(coordinates: KustoCoordinates,
                                                    batchIdIfExists: String,
                                                    kustoAdminClient: Client,
                                                    tmpTableName: String,
                                                    isAsync: Boolean = false): Unit = {
    import coordinates._
    try {
      // Move data to real table
      // Protect tmp table from merge/rebuild and move data to the table requested by customer. This operation is atomic.
      kustoAdminClient.execute(database, generateTableAlterMergePolicyCommand(tmpTableName, allowMerge = false, allowRebuild = false))
      kustoAdminClient.execute(database, generateTableMoveExtentsCommand(tmpTableName, table.get))
      KDSU.logInfo(myName, s"write to Kusto table '$table' finished successfully $batchIdIfExists")
    }
    catch {
      case exception: Exception =>
        KDSU.reportExceptionAndThrow(myName, exception, "finalizing write operation", cluster, database, table.get, isLogDontThrow = isAsync)
    }
    finally {
      tryCleanupIngestionByproducts(database, kustoAdminClient, tmpTableName)
    }
  }

  private def ingestToTemporaryTableByWorkers(
                                               batchId: String,
                                               timeout: FiniteDuration,
                                               storageUri: String,
                                               rows: Iterator[InternalRow])
                                             (implicit parameters: KustoWriteResource)
  : Unit = {

    import parameters._
    val partitionId = TaskContext.getPartitionId
    KDSU.logInfo(myName, s"Ingesting partition '$partitionId'")

    // We force blocking here, since the driver can only complete the ingestion process
    // once all partitions are ingested into the temporary table
    Await.result(
      Future(ingestRowsIntoKusto(rows, storageUri)).map { ingestionResults =>
        // Proceed only on success. Will throw on failure for the driver to handle
        ingestionResults.foreach { ingestionResult =>
          KDSU.runSequentially[IngestionStatus](
            func = () => ingestionResult.getIngestionStatusCollection().get(0),
            0, delayPeriodBetweenCalls, (timeout.toMillis / delayPeriodBetweenCalls + 5).toInt,
            res => res.status == OperationStatus.Pending,
            res => res.status match {
              case OperationStatus.Pending =>
                throw new RuntimeException(s"Ingestion to Kusto failed on timeout failure. Cluster: '${coordinates.cluster}', " +
                  s"database: '${coordinates.database}', table: '${coordinates.table}', batch: '$batchId', partition: '$partitionId'")
              case OperationStatus.Succeeded =>
                KDSU.logInfo(myName, s"Ingestion to Kusto succeeded. " +
                  s"Cluster: '${coordinates.cluster}', " +
                  s"database: '${coordinates.database}', " +
                  s"table: '${coordinates.table}', batch: '$batchId', partition: '$partitionId'")
              case otherStatus =>
                throw new RuntimeException(s"Ingestion to Kusto failed with status '$otherStatus'." +
                  s" Cluster: '${coordinates.cluster}', database: '${coordinates.database}', " +
                  s"table: '${coordinates.table}', batch: '$batchId', partition: '$partitionId'. Ingestion info: '${readIngestionResult(res)}'")
            }).await(timeout.toMillis, TimeUnit.MILLISECONDS)

        }
      },
      timeout)
  }

  def all(list: util.ArrayList[Boolean]): Boolean = {
    val it = list.iterator
    var res = true
    while (it.hasNext && res) {
      res = it.next()
    }
    res
  }

  def createBlobWriter(schema: StructType,
                       timeZone: String,
                       tableCoordinates: KustoCoordinates,
                       tmpTableName: String,
                       storageUri: String,
                       container: CloudBlobContainer): BlobWriteResource = {
    val blobName = s"${tableCoordinates.database}_${tmpTableName}_${UUID.randomUUID.toString}_SparkStreamUpload.gz"
    val blob: CloudBlockBlob = container.getBlockBlobReference(blobName)
    val gzip: GZIPOutputStream = new GZIPOutputStream(blob.openOutputStream())

    val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)
    val buffer: BufferedWriter = new BufferedWriter(writer, GZIP_BUFFER_SIZE)
    val csvWriter: CsvWriter = new CsvWriter(buffer, new CsvWriterSettings)
    datasink.BlobWriteResource(buffer, gzip, csvWriter, blob)
  }

  @throws[IOException]
  private[kusto] def serializeRows(rows: Iterator[InternalRow],
                                   storageUri: String,
                                   parameters: KustoWriteResource): Seq[CloudBlockBlob] = {
    import parameters._
    val container: CloudBlobContainer = new CloudBlobContainer(new URI(storageUri))
    //This blobWriter will be used later to write the rows to blob storage, if the limit exceeds
    val blobWriter: BlobWriteResource = createBlobWriter(schema, writeOptions.timeZone, coordinates, tmpTableName, storageUri, container)

    // Serialize rows to ingest and send to blob storage.
    // On each iteration calculate: (updated total file size, current blob writer, and the list of all blobs)
    val (_, _, blobs) = rows.foldLeft[(Long, BlobWriteResource, Seq[CloudBlockBlob])]((0, blobWriter, Seq())) { case ((size, blobWriter, blobsCreated), row) =>
      val csvRowResult: CsvRowResult = convertRowToCSV(row, schema, writeOptions.timeZone)
      val newTotalSize = size + csvRowResult.rowByteSize
      if (newTotalSize < maxBlobSize) {
        blobWriter.csvWriter.writeRow(csvRowResult.formattedRow)
        (newTotalSize, blobWriter, blobsCreated)
      } else {
        finalizeBlobWrite(blobWriter)
        (0, createBlobWriter(schema, writeOptions.timeZone, coordinates, tmpTableName, storageUri, container), blobsCreated :+ blobWriter.blob)
      }
    }

    if (blobs.isEmpty) {
      finalizeBlobWrite(blobWriter)
      Seq(blobWriter.blob)
    } else {
      blobs
    }
  }

  def finalizeBlobWrite(blobWriteResource: BlobWriteResource): Unit = {
    blobWriteResource.buffer.flush()
    blobWriteResource.gzip.flush()
    blobWriteResource.buffer.close()
    blobWriteResource.gzip.close()
  }

  def convertRowToCSV(row: InternalRow, schema: StructType, timeZone: String): CsvRowResult = {
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone(timeZone))
    val schemaFields: Array[StructField] = schema.fields

    val (fields, size) = row.toSeq(schema).foldLeft[(Seq[String], Int)](Seq[String](), 0) { (res, curr) =>
      val formattedField: String = if (curr == null) "" else {
        val fieldIndexInRow = res._1.size
        schemaFields(fieldIndexInRow).dataType match {
          case DateType => DateTimeUtils.toJavaDate(row.getInt(fieldIndexInRow)).toString
          case TimestampType => dateFormat.format(DateTimeUtils.toJavaTimestamp(row.getLong(fieldIndexInRow)))
          case StringType => row.getString(fieldIndexInRow)
          case _ => row.get(fieldIndexInRow, schemaFields(fieldIndexInRow).dataType).toString
        }
      }

      (res._1 :+ formattedField, res._2 + formattedField.length)
    }

    CsvRowResult(fields.toArray, size + fields.size)
  }


  private def readIngestionResult(statusRecord: IngestionStatus): String = {
    new ObjectMapper()
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(statusRecord)
  }
}

case class CsvRowResult(formattedRow: Array[String], rowByteSize: Long)

case class BlobWriteResource(buffer: BufferedWriter, gzip: GZIPOutputStream, csvWriter: CsvWriter, blob: CloudBlockBlob)

case class KustoWriteResource(authentication: KustoAuthentication,
                              coordinates: KustoCoordinates,
                              schema: StructType,
                              writeOptions: WriteOptions,
                              tmpTableName: String)

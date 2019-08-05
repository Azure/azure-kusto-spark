package com.microsoft.kusto.spark.datasink

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.security.InvalidParameterException
import java.util.zip.GZIPOutputStream
import java.util.{TimeZone, UUID}

import com.microsoft.azure.kusto.ingest.IngestionProperties.DATA_FORMAT
import com.microsoft.azure.kusto.ingest.result.IngestionResult
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestionProperties}
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature
import com.microsoft.azure.storage.blob.{CloudBlobContainer, CloudBlockBlob}
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.javaUtil.BufferWriterWithProgress
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoClient, KustoClientCache, KustoQueryUtils, KustoConstants => KCONST, KustoDataSourceUtils => KDSU}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.types.{DateType, StringType, StructType, TimestampType, _}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.{FutureAction, TaskContext}

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object KustoWriter {
  private val myName = this.getClass.getSimpleName
  val TempIngestionTablePrefix = "_tmpTable"
  val delayPeriodBetweenCalls: Int = KCONST.defaultPeriodicSamplePeriod.toMillis.toInt
  val GZIP_BUFFER_SIZE: Int = KCONST.defaultBufferSize
  val maxBlobSize: Int = KCONST.oneGiga - KCONST.oneMega

  private[kusto] def write(batchId: Option[Long],
                           data: DataFrame,
                           tableCoordinates: KustoCoordinates,
                           authentication: KustoAuthentication,
                           writeOptions: WriteOptions): Unit = {

    val batchIdIfExists = batchId.filter(_ != 0).map(_.toString).getOrElse("")
    val kustoClient = KustoClientCache.getClient(tableCoordinates.cluster, authentication)

    if (tableCoordinates.table.isEmpty) {
      KDSU.reportExceptionAndThrow(myName, new InvalidParameterException("Table name not specified"), "writing data",
        tableCoordinates.cluster, tableCoordinates.database)
    }

    val table = tableCoordinates.table.get

    val tmpTableName: String = KustoQueryUtils.simplifyName(TempIngestionTablePrefix +
      data.sparkSession.sparkContext.appName +
      "_" + table + batchIdIfExists + "_" + UUID.randomUUID().toString)
    implicit val parameters: KustoWriteResource = KustoWriteResource(authentication, tableCoordinates, data.schema, writeOptions, tmpTableName)

    cleanupTempTables(kustoClient, tableCoordinates)

    // KustoWriter will create a temporary table ingesting the data to it.
    // Only if all executors succeeded the table will be appended to the original destination table.
    kustoClient.createTmpTableWithSameSchema(tableCoordinates, tmpTableName, writeOptions.tableCreateOptions, data.schema)
    KDSU.logInfo(myName, s"Successfully created temporary table $tmpTableName, will be deleted after completing the operation")

    val rdd = data.queryExecution.toRdd
    val partitionsResults = rdd.sparkContext.collectionAccumulator[PartitionResult]

    if (writeOptions.isAsync) {
      val asyncWork: FutureAction[Unit] = rdd.foreachPartitionAsync { rows => ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults) }
      KDSU.logInfo(myName, s"asynchronous write to Kusto table '$table' in progress")
      // This part runs back on the driver
      asyncWork.onSuccess {
        case _ => kustoClient.finalizeIngestionWhenWorkersSucceeded(tableCoordinates, batchIdIfExists, kustoClient.engineClient, tmpTableName, partitionsResults, writeOptions.timeout, isAsync = true)
      }
      asyncWork.onFailure {
        case exception: Exception =>
          kustoClient.cleanupIngestionByproducts(tableCoordinates.database, kustoClient.engineClient, tmpTableName)
          KDSU.reportExceptionAndThrow(myName, exception, "writing data", tableCoordinates.cluster, tableCoordinates.database, table, shouldNotThrow = true)
          KDSU.logError(myName, "The exception is not visible in the driver since we're in async mode")
      }
    }
    else {
      try {
        rdd.foreachPartition { rows => ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults) }
      } catch {
        case exception: Exception =>
          kustoClient.cleanupIngestionByproducts(tableCoordinates.database, kustoClient.engineClient, tmpTableName)
          throw exception
      }
      kustoClient.finalizeIngestionWhenWorkersSucceeded(tableCoordinates, batchIdIfExists, kustoClient.engineClient, tmpTableName, partitionsResults, writeOptions.timeout)
    }
  }

  def ingestRowsIntoTempTbl(rows: Iterator[InternalRow], batchId: String, partitionsResults: CollectionAccumulator[PartitionResult])
                           (implicit parameters: KustoWriteResource): Unit =
    if (rows.isEmpty) {
      KDSU.logWarn(myName, s"sink to Kusto table '${parameters.coordinates.table.get}' with no rows to write on partition ${TaskContext.getPartitionId}")
    } else {
      ingestToTemporaryTableByWorkers(batchId, rows, partitionsResults)
    }

  def cleanupTempTables(kustoClient: KustoClient, coordinates: KustoCoordinates): Unit = {
    Future {
      val tempTablesOld: Seq[String] =
        kustoClient.engineClient.execute(generateFindOldTempTablesCommand(coordinates.database))
          .getValues.asScala
          .headOption.map(_.asScala)
          .getOrElse(Seq())

      // Try delete temporary tablesToCleanup created and not used
      val tempTablesCurr: Seq[String] = kustoClient.engineClient.execute(coordinates.database, generateFindCurrentTempTablesCommand(TempIngestionTablePrefix))
        .getValues.get(0).asScala

      val tablesToCleanup: Seq[String] = tempTablesOld.intersect(tempTablesCurr)

      if (tablesToCleanup.nonEmpty) {
        kustoClient.engineClient.execute(coordinates.database, generateDropTablesCommand(tablesToCleanup.mkString(",")))
      }
    } onFailure {
      case ex: Exception =>
        KDSU.reportExceptionAndThrow(
          myName,
          ex,
          "trying to drop temporary tables", coordinates.cluster, coordinates.database, coordinates.table.getOrElse("Unspecified table name"),
          shouldNotThrow = true
        )
    }
  }

  def ingestRowsIntoKusto(rows: Iterator[InternalRow],
                          ingestClient: IngestClient,
                          partitionsResults: CollectionAccumulator[PartitionResult])
                         (implicit parameters: KustoWriteResource): Unit = {
    import parameters._

    val ingestionProperties = if (writeOptions.IngestionProperties.isDefined) {
      SparkIngestionProperties.fromString(writeOptions.IngestionProperties.get).toIngestionProperties(coordinates.database, tmpTableName)
    } else {
      new IngestionProperties(coordinates.database, tmpTableName)
    }

    ingestionProperties.setDataFormat(DATA_FORMAT.csv.name)
    ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table)
    ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses)
    val targetTable = coordinates.table.get

    val blobList: Seq[(CloudBlockBlob, Long)] = serializeRows(rows, parameters)

    if (blobList.isEmpty) {
      KDSU.logWarn(myName, "No blobs created for ingestion to Kusto cluster " + {
        coordinates.cluster
      } +
        ", database " + {
        coordinates.database
      } + ", table " + targetTable)
    }

    KDSU.logWarn(myName, s"Ingesting using ingest client - partition: ${TaskContext.getPartitionId()}")
    val t1 = System.currentTimeMillis()

    blobList.map { blobSizePair =>
      val (blob, blobSize) = (blobSizePair._1, blobSizePair._2)
      val signature = blob.getServiceClient.getCredentials.asInstanceOf[StorageCredentialsSharedAccessSignature]
      val blobUri = blob.getStorageUri.getPrimaryUri.toString
      val blobPath = blobUri + "?" + signature.getToken
      val blobSourceInfo = new BlobSourceInfo(blobPath, blobSize)

      ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties)
    }.foreach(blob => partitionsResults.add(PartitionResult(blob, TaskContext.getPartitionId)))

    KDSU.logWarn(myName, s"Finished ingesting using ingest client - partition: ${TaskContext.getPartitionId()} , took:${System.currentTimeMillis() - t1}")
  }

  private def ingestToTemporaryTableByWorkers(
                                               batchId: String,
                                               rows: Iterator[InternalRow],
                                               partitionsResults: CollectionAccumulator[PartitionResult])
                                             (implicit parameters: KustoWriteResource): Unit = {

    import parameters._
    val partitionId = TaskContext.getPartitionId
    KDSU.logInfo(myName, s"Ingesting partition '$partitionId'")
    val ingestClient = KustoClientCache.getClient(coordinates.cluster, authentication).ingestClient

    // We force blocking here, since the driver can only complete the ingestion process
    // once all partitions are ingested into the temporary table
    ingestRowsIntoKusto(rows, ingestClient, partitionsResults)
  }

  def createBlobWriter(schema: StructType,
                       tableCoordinates: KustoCoordinates,
                       tmpTableName: String,
                       container: CloudBlobContainer): BlobWriteResource = {
    val blobName = s"${tableCoordinates.database}_${tmpTableName}_${UUID.randomUUID.toString}_SparkStreamUpload.gz"
    val blob: CloudBlockBlob = container.getBlockBlobReference(blobName)
    val gzip: GZIPOutputStream = new GZIPOutputStream(blob.openOutputStream())

    val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)

    val buffer: BufferWriterWithProgress = new BufferWriterWithProgress(writer, GZIP_BUFFER_SIZE)
    BlobWriteResource(buffer, gzip, blob)
  }

  @throws[IOException]
  private[kusto] def serializeRows(rows: Iterator[InternalRow],
                                   parameters: KustoWriteResource): Seq[(CloudBlockBlob, Long)] = {
    import parameters._

    val kustoClient = KustoClientCache.getClient(coordinates.cluster, authentication)
    val initialStorageUri = kustoClient.getNewTempBlobReference
    val initialContainer: CloudBlobContainer = new CloudBlobContainer(new URI(initialStorageUri))

    //This blobWriter will be used later to write the rows to blob storage from which it will be ingested to Kusto
    val initialBlobWriter: BlobWriteResource = createBlobWriter(schema, coordinates, tmpTableName, initialContainer)
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone(writeOptions.timeZone))

    // Serialize rows to ingest and send to blob storage.
    val (currentBlobWriter, blobsToIngest) = rows.foldLeft[(BlobWriteResource, Seq[(CloudBlockBlob, Long)])]((initialBlobWriter, Seq())) {
      case ((blobWriter, blobsCreated), row) =>

        writeRowAsCSV(row, schema, dateFormat, blobWriter.writer)

        // Check if we crossed the threshold for a single blob to ingest.
        // If so, add current blob to the list of blobs to ingest, and open a new blob writer
        if (blobWriter.writer.getProgress < maxBlobSize) {
          (blobWriter, blobsCreated)
        } else {
          finalizeBlobWrite(blobWriter)

          val container: CloudBlobContainer = new CloudBlobContainer(new URI(kustoClient.getNewTempBlobReference))
          (createBlobWriter(schema, coordinates, tmpTableName, container), blobsCreated :+ (blobWriter.blob, blobWriter.writer.getProgress))
        }
    }

    KDSU.logInfo(myName, s"finished serializing partition ${TaskContext.getPartitionId}")
    finalizeBlobWrite(currentBlobWriter)
    if (currentBlobWriter.writer.getProgress > 0) {
      blobsToIngest :+ (currentBlobWriter.blob, currentBlobWriter.writer.getProgress)
    }
    else {
      blobsToIngest
    }
  }

  def finalizeBlobWrite(blobWriteResource: BlobWriteResource): Unit = {
    blobWriteResource.writer.flush()
    blobWriteResource.gzip.flush()
    blobWriteResource.writer.close()
    blobWriteResource.gzip.close()
  }

  def writeRowAsCSV(row: InternalRow, schema: StructType, dateFormat: FastDateFormat, writer: BufferWriterWithProgress): Unit = {
    val schemaFields: Array[StructField] = schema.fields

    writeField(row, fieldIndexInRow = 0, schemaFields(0).dataType, dateFormat, writer, nested = false)
    for (i <- 1 until row.numFields) {
      writer.write(",")
      writeField(row, i, schemaFields(i).dataType, dateFormat, writer, nested = false)
    }

    writer.newLine()
  }

  private def writeField(row: SpecializedGetters, fieldIndexInRow: Int, dataType: DataType, dateFormat: FastDateFormat, bufferedWriter: BufferWriterWithProgress, nested: Boolean): Unit = {
    if (!row.isNullAt(fieldIndexInRow)) dataType match {
      case DateType => bufferedWriter.write(DateTimeUtils.toJavaDate(row.getInt(fieldIndexInRow)).toString)
      case TimestampType => bufferedWriter.write(dateFormat.format(DateTimeUtils.toJavaTimestamp(row.getLong(fieldIndexInRow))))
      case StringType => bufferedWriter.write(GetStringFromUTF8(row.getUTF8String(fieldIndexInRow), nested))
      case BooleanType => bufferedWriter.write(row.getBoolean(fieldIndexInRow).toString)
      case structType: StructType => convertStructToCsv(row.getStruct(fieldIndexInRow, structType.length), structType, dateFormat, bufferedWriter)
      case arrType: ArrayType => convertArrayToCsv(row.getArray(fieldIndexInRow), arrType.elementType, dateFormat, bufferedWriter)
      case mapType: MapType => convertMapToCsv(row.getMap(fieldIndexInRow), mapType, dateFormat, bufferedWriter)
      case _ => bufferedWriter.write(row.get(fieldIndexInRow, dataType).toString)
    }
  }

  private def convertStructToCsv(row: InternalRow, schema: StructType, dateFormat: FastDateFormat, bufferedWriter: BufferWriterWithProgress): Unit = {
    val fields = schema.fields

    if (fields.length != 0) {
      bufferedWriter.write("{")
      bufferedWriter.write("\"")
      bufferedWriter.write(fields(0).name)
      bufferedWriter.write("\"")
      bufferedWriter.write(":")
      writeField(row, 0, fields(0).dataType, dateFormat, bufferedWriter, nested = true)
      for (x <- 1 until fields.length) {
        bufferedWriter.write(",")
        bufferedWriter.write("\"")
        bufferedWriter.write(fields(x).name)
        bufferedWriter.write("\"")
        bufferedWriter.write(":")
        writeField(row, x, fields(x).dataType, dateFormat, bufferedWriter, nested = true)
      }
      bufferedWriter.write("}")
    }
  }

  private def convertArrayToCsv(ar: ArrayData, fieldsType: DataType, dateFormat: FastDateFormat, bufferedWriter: BufferWriterWithProgress): Unit = {
    if (ar.numElements() == 0) bufferedWriter.write("[]") else {
      bufferedWriter.write("[")
      writeField(ar, fieldIndexInRow = 0, fieldsType, dateFormat, bufferedWriter, nested = true)
      for (x <- 1 until ar.numElements()) {
        bufferedWriter.write(",")
        writeField(ar, x, fieldsType, dateFormat, bufferedWriter, nested = true)
      }
      bufferedWriter.write("]")
    }
  }

  private def convertMapToCsv(map: MapData, fieldsType: MapType, dateFormat: FastDateFormat, bufferedWriter: BufferWriterWithProgress): Unit = {
    val keys = map.keyArray()
    val values = map.valueArray()
    bufferedWriter.write("{")
    bufferedWriter.write("\"")
    writeField(keys, fieldIndexInRow = 0, dataType = fieldsType.keyType, dateFormat = dateFormat, bufferedWriter = bufferedWriter, nested = false)
    bufferedWriter.write("\"")
    bufferedWriter.write(":")
    writeField(values, fieldIndexInRow = 0, dataType = fieldsType.valueType, dateFormat = dateFormat, bufferedWriter = bufferedWriter, nested = true)
    for (x <- 1 until map.keyArray().numElements()) {
      bufferedWriter.write(",")
      bufferedWriter.write("\"")
      writeField(keys, x, fieldsType.keyType, dateFormat, bufferedWriter, nested = false)
      bufferedWriter.write("\"")
      bufferedWriter.write(":")
      writeField(values, x, fieldsType.valueType, dateFormat, bufferedWriter, nested = true)
    }

    bufferedWriter.write("}")
  }

  private def GetStringFromUTF8(str: UTF8String, nested: Boolean) = {
    if (str == null) null else if (nested) "\"" + str.toString + "\"" else str.toString
  }
}

case class BlobWriteResource(writer: BufferWriterWithProgress, gzip: GZIPOutputStream, blob: CloudBlockBlob)

case class KustoWriteResource(authentication: KustoAuthentication,
                              coordinates: KustoCoordinates,
                              schema: StructType,
                              writeOptions: WriteOptions,
                              tmpTableName: String)

case class PartitionResult(ingestionResult: IngestionResult, partitionId: Int)

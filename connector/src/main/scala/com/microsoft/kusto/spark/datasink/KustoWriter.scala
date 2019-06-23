package com.microsoft.kusto.spark.datasink


import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.security.InvalidParameterException
import java.util
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
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoClient, KustoClientCache, KustoQueryUtils, KustoConstants => KCONST, KustoDataSourceUtils => KDSU}
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
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
          kustoClient.tryCleanupIngestionByproducts(tableCoordinates.database, kustoClient.engineClient, tmpTableName)
          KDSU.reportExceptionAndThrow(myName, exception, "writing data", tableCoordinates.cluster, tableCoordinates.database, table, isLogDontThrow = true)
          KDSU.logError(myName, "The exception is not visible in the driver since we're in async mode")
      }
    }
    else {
      try {
        rdd.foreachPartition { rows => ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults) }
      } catch {
        case exception: Exception =>
          kustoClient.tryCleanupIngestionByproducts(tableCoordinates.database, kustoClient.engineClient, tmpTableName)
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
          isLogDontThrow = true
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

    blobList.map { blobSizePair =>
      val (blob, blobSize) = (blobSizePair._1, blobSizePair._2)
      val signature = blob.getServiceClient.getCredentials.asInstanceOf[StorageCredentialsSharedAccessSignature]
      val blobUri = blob.getStorageUri.getPrimaryUri.toString
      val blobPath = blobUri + "?" + signature.getToken
      val blobSourceInfo = new BlobSourceInfo(blobPath, blobSize)

      ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties)
    }.foreach(blob => partitionsResults.add(PartitionResult(blob, TaskContext.getPartitionId)))
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

  def all(list: util.ArrayList[Boolean]): Boolean = {
    val it = list.iterator
    var res = true
    while (it.hasNext && res) {
      res = it.next()
    }
    res
  }

  def createBlobWriter(schema: StructType,
                       tableCoordinates: KustoCoordinates,
                       tmpTableName: String,
                       container: CloudBlobContainer): BlobWriteResource = {
    val blobName = s"${tableCoordinates.database}_${tmpTableName}_${UUID.randomUUID.toString}_SparkStreamUpload.gz"
    val blob: CloudBlockBlob = container.getBlockBlobReference(blobName)
    val gzip: GZIPOutputStream = new GZIPOutputStream(blob.openOutputStream())

    val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)
    val buffer: BufferedWriter = new BufferedWriter(writer, GZIP_BUFFER_SIZE)
    val csvWriter: CsvWriter = new CsvWriter(buffer, new CsvWriterSettings)
    BlobWriteResource(buffer, gzip, csvWriter, blob)
  }

  @throws[IOException]
  private[kusto] def serializeRows(rows: Iterator[InternalRow],
                                   parameters: KustoWriteResource): Seq[(CloudBlockBlob, Long)] = {
    import parameters._

    var kustoClient = KustoClientCache.getClient(coordinates.cluster, authentication)
    var initialStorageUri = kustoClient.getNewTempBlobReference
    val initialContainer: CloudBlobContainer = new CloudBlobContainer(new URI(initialStorageUri))
    //This blobWriter will be used later to write the rows to blob storage from which it will be ingested to Kusto
    val initialBlobWriter: BlobWriteResource = createBlobWriter(schema, coordinates, tmpTableName, initialContainer)
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone(writeOptions.timeZone))

    // Serialize rows to ingest and send to blob storage.
    val (currentFileSize, currentBlobWriter, blobsToIngest) = rows.foldLeft[(Long, BlobWriteResource, Seq[(CloudBlockBlob, Long)])]((0, initialBlobWriter, Seq())) {
      case ((size, blobWriter, blobsCreated), row) =>
        val csvRowResult: CsvRowResult = convertRowToCSV(row, schema, dateFormat)
        blobWriter.csvWriter.writeRow(csvRowResult.formattedRow)
        val newTotalSize = size + csvRowResult.rowByteSize

        // Check if we crossed the threshold for a single blob to ingest.
        // If so, add current blob to the list of blobs to ingest, and open a new blob writer
        if (newTotalSize < maxBlobSize) {
          (newTotalSize, blobWriter, blobsCreated)
        } else {
          finalizeBlobWrite(blobWriter)
          val container: CloudBlobContainer = new CloudBlobContainer(new URI(kustoClient.getNewTempBlobReference))
          (0, createBlobWriter(schema, coordinates, tmpTableName, container), blobsCreated :+ (blobWriter.blob, newTotalSize))
        }
    }

    if (currentFileSize > 0) {
      finalizeBlobWrite(currentBlobWriter)
      blobsToIngest :+ (currentBlobWriter.blob, currentFileSize)
    }
    else {
      blobsToIngest
    }
  }

  def finalizeBlobWrite(blobWriteResource: BlobWriteResource): Unit = {
    blobWriteResource.buffer.flush()
    blobWriteResource.gzip.flush()
    blobWriteResource.buffer.close()
    blobWriteResource.gzip.close()
  }

  def convertRowToCSV(row: InternalRow, schema: StructType, dateFormat: FastDateFormat): CsvRowResult = {
    val schemaFields: Array[StructField] = schema.fields

    val (fields, size) = row.toSeq(schema).foldLeft[(Seq[String], Int)](Seq.empty[String], 0) { (res, curr) =>
      val formattedField: String = if (curr == null) "" else {
        val fieldIndexInRow = res._1.size
        val dataType = schemaFields(fieldIndexInRow).dataType
        getField(row, fieldIndexInRow, dataType, dateFormat, nested = false)
      }

      if (formattedField == null) (res._1 :+ "", res._2) else (res._1 :+ formattedField, res._2 + formattedField.length)
    }

    CsvRowResult(fields.toArray, size + fields.size)
  }

  private def getField(row: SpecializedGetters, fieldIndexInRow: Int, dataType: DataType, dateFormat: FastDateFormat, nested: Boolean) = {
    if (row.get(fieldIndexInRow, dataType) == null) null else dataType match {
      case DateType => DateTimeUtils.toJavaDate(row.getInt(fieldIndexInRow)).toString
      case TimestampType => dateFormat.format(DateTimeUtils.toJavaTimestamp(row.getLong(fieldIndexInRow)))
      case StringType => GetStringFromUTF8(row.getUTF8String(fieldIndexInRow), nested)
      case BooleanType => row.getBoolean(fieldIndexInRow).toString
      case structType: StructType => convertStructToCsv(row.getStruct(fieldIndexInRow, structType.length), structType, dateFormat)
      case arrType: ArrayType => convertArrayToCsv(row.getArray(fieldIndexInRow), arrType.elementType, dateFormat)
      case mapType: MapType => convertMapToCsv(row.getMap(fieldIndexInRow), mapType, dateFormat)
      case _ => row.get(fieldIndexInRow, dataType).toString
    }
  }

  private def convertStructToCsv(row: InternalRow, schema: StructType, dateFormat: FastDateFormat): String = {
    val fields = schema.fields
    val result: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()

    for (x <- fields.indices) {
      result(fields(x).name) = getField(row, x, fields(x).dataType, dateFormat, nested = true)
    }
    "{" + result.map { case (k, v) => "\"" + k + "\"" + ":" + v }.mkString(",") + "}"
  }

  private def convertArrayToCsv(ar: ArrayData, fieldsType: DataType, dateFormat: FastDateFormat): String = {
    if (ar == null) null else {
      if (ar.numElements() == 0) "[]" else {

        "[" + convertArrayToStringArray(ar, fieldsType, dateFormat).mkString(",") + "]"
      }
    }
  }

  private def convertArrayToStringArray(ar: ArrayData, fieldsType: DataType, dateFormat: FastDateFormat, nested: Boolean = true): Array[String] = {
    val result: Array[String] = new Array(ar.numElements())
    for (x <- 0 until ar.numElements()) {
      result(x) = getField(ar, x, fieldsType, dateFormat, nested)
    }
    result
  }

  private def convertMapToCsv(map: MapData, fieldsType: MapType, dateFormat: FastDateFormat): String = {
    val keys = convertArrayToStringArray(map.keyArray(), fieldsType.keyType, dateFormat, nested = false)
    val values = convertArrayToStringArray(map.valueArray(), fieldsType.valueType, dateFormat)

    val result: Array[String] = new Array(keys.length)

    for (x <- keys.indices) {
      result(x) = "\"" + keys(x) + "\"" + ":" + values(x)
    }

    "{" + result.mkString(",") + "}"
  }


  private def GetStringFromUTF8(str: UTF8String, nested: Boolean) = {
    if (str == null) null else if (nested) "\"" + str.toString + "\"" else str.toString
  }
}

case class CsvRowResult(formattedRow: Array[String], rowByteSize: Long)

case class BlobWriteResource(buffer: BufferedWriter, gzip: GZIPOutputStream, csvWriter: CsvWriter, blob: CloudBlockBlob)

case class KustoWriteResource(authentication: KustoAuthentication,
                              coordinates: KustoCoordinates,
                              schema: StructType,
                              writeOptions: WriteOptions,
                              tmpTableName: String)

case class PartitionResult(ingestionResult: IngestionResult, partitionId: Int)
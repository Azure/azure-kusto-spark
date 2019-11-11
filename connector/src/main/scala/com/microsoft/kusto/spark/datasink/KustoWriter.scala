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
import com.microsoft.azure.storage.blob.CloudBlockBlob
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
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
import org.apache.spark.TaskContext

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, TimeoutException}

object KustoWriter {
  private val myName = this.getClass.getSimpleName
  val TempIngestionTablePrefix = "_tmpTable"
  val delayPeriodBetweenCalls: Int = KCONST.defaultPeriodicSamplePeriod.toMillis.toInt
  val GZIP_BUFFER_SIZE: Int = KCONST.defaultBufferSize
  var maxBlobSize: Int = 100 * KCONST.oneMega

  private[kusto] def write(batchId: Option[Long],
                           data: DataFrame,
                           tableCoordinates: KustoCoordinates,
                           authentication: KustoAuthentication,
                           writeOptions: WriteOptions): Unit = {
    maxBlobSize = writeOptions.batchLimit * KCONST.oneMega
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

    val ingestionProperties = getIngestionProperties(writeOptions, parameters)
    kustoClient.setMappingOnStagingTableIfNeeded(ingestionProperties, table)
    if (ingestionProperties.getFlushImmediately){
      KDSU.logWarn(myName, "Its not recommended to set flushImmediately to true")
    }
    val rdd = data.queryExecution.toRdd
    val partitionsResults = rdd.sparkContext.collectionAccumulator[PartitionResult]

    if (writeOptions.isAsync) {
      val asyncWork = rdd.foreachPartitionAsync { rows => ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults) }
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
    } else {
      try
        rdd.foreachPartition { rows => ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults) }
      catch {
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

    val ingestionProperties = getIngestionProperties(writeOptions, parameters)
    ingestionProperties.setDataFormat(DATA_FORMAT.csv.name)
    ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table)
    ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses)

    val tasks = ingestRows(rows, parameters, ingestClient, ingestionProperties, partitionsResults)

    KDSU.logWarn(myName, s"Ingesting using ingest client - partition: ${TaskContext.getPartitionId()}")

    tasks.asScala.foreach(t => try {
      Await.result(t, KCONST.defaultIngestionTaskTime)
    } catch {
      case _: TimeoutException => KDSU.logWarn(myName, s"Timed out trying to ingest, no need to fail as the ingest might succeed")
    })
  }

  private def getIngestionProperties(writeOptions: WriteOptions, parameters: KustoWriteResource): IngestionProperties = {
    if (writeOptions.IngestionProperties.isDefined) {
      SparkIngestionProperties.fromString(writeOptions.IngestionProperties.get).toIngestionProperties(parameters.coordinates.database, parameters.tmpTableName)
    } else {
      new IngestionProperties(parameters.coordinates.database, parameters.tmpTableName)
    }
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
                       client: KustoClient): BlobWriteResource = {
    val blobName = s"${tableCoordinates.database}_${tmpTableName}_${UUID.randomUUID.toString}_spark.csv.gz"

    val containerAndSas = client.getNewTempBlobReference
    val currentBlob = new CloudBlockBlob(new URI(containerAndSas.containerUrl + '/' + blobName + containerAndSas.sas))
    val currentSas = containerAndSas.sas
    val gzip: GZIPOutputStream = new GZIPOutputStream(currentBlob.openOutputStream())

    val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)

    val buffer: BufferedWriter = new BufferedWriter(writer, GZIP_BUFFER_SIZE)
    val csvWriter = CountingCsvWriter(buffer)
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
      Future {
        var props = ingestionProperties
        if(!ingestionProperties.getFlushImmediately && flushImmediately){
          // Need to copy the ingestionProperties so that only this one will be flushed immediately
          props = SparkIngestionProperties.cloneIngestionProperties(ingestionProperties)
          props.setFlushImmediately(true)
        }
        val blobUri = blob.getStorageUri.getPrimaryUri.toString
        val blobPath = blobUri + sas
        val blobSourceInfo = new BlobSourceInfo(blobPath, size)
        partitionsResults.add(PartitionResult(ingestClient.ingestFromBlob(blobSourceInfo, props), TaskContext.getPartitionId))
      }
    }

    import parameters._

    val kustoClient = KustoClientCache.getClient(coordinates.cluster, authentication)

    //This blobWriter will be used later to write the rows to blob storage from which it will be ingested to Kusto
    val initialBlobWriter: BlobWriteResource = createBlobWriter(schema, coordinates, tmpTableName, kustoClient)
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone(writeOptions.timeZone))

    val ingestionTasks: util.ArrayList[Future[Unit]] = new util.ArrayList()

    // Serialize rows to ingest and send to blob storage.
    val lastBlobWriter = rows.foldLeft[BlobWriteResource](initialBlobWriter) {
      case (blobWriter, row) =>
        writeRowAsCSV(row, schema, dateFormat, blobWriter.csvWriter)

        val shouldNotCommitBlockBlob = blobWriter.csvWriter.getCounter < maxBlobSize
        if (shouldNotCommitBlockBlob) {
          blobWriter
        } else {
          KDSU.logInfo(myName, s"Sealing blob in partition ${TaskContext.getPartitionId}, number ${ingestionTasks.size}")
          finalizeBlobWrite(blobWriter)
          val task = ingest(blobWriter.blob, blobWriter.csvWriter.getCounter, blobWriter.sas, flushImmediately = true)
          ingestionTasks.add(task)
          createBlobWriter(schema, coordinates, tmpTableName, kustoClient)
        }
    }

    KDSU.logInfo(myName, s"finished serializing partition ${TaskContext.getPartitionId}")
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

  def writeRowAsCSV(row: InternalRow, schema: StructType, dateFormat: FastDateFormat, writer: CountingCsvWriter): Unit = {
    val schemaFields: Array[StructField] = schema.fields

    if (!row.isNullAt(0)) {
      writeField(row, fieldIndexInRow = 0, schemaFields(0).dataType, dateFormat, writer, nested = false)
    }

    for (i <- 1 until row.numFields) {
      writer.write(',')
      if (!row.isNullAt(i)) {
        writeField(row, i, schemaFields(i).dataType, dateFormat, writer, nested = false)
      }
    }

    writer.newLine()
  }

  // This method does not check for null at the current row idx and should be checked before !
  private def writeField(row: SpecializedGetters, fieldIndexInRow: Int, dataType: DataType, dateFormat: FastDateFormat, csvWriter: CountingCsvWriter, nested: Boolean): Unit = {
    dataType match {
      case DateType => csvWriter.writeStringField(DateTimeUtils.toJavaDate(row.getInt(fieldIndexInRow)).toString, nested)
      case TimestampType => csvWriter.writeStringField(dateFormat.format(DateTimeUtils.toJavaTimestamp(row.getLong(fieldIndexInRow))), nested)
      case StringType => GetStringFromUTF8(row.getUTF8String(fieldIndexInRow), nested, csvWriter)
      case BooleanType => csvWriter.write(row.getBoolean(fieldIndexInRow).toString)
      case structType: StructType => convertStructToCsv(row.getStruct(fieldIndexInRow, structType.length), structType, dateFormat, csvWriter, nested)
      case arrType: ArrayType => convertArrayToCsv(row.getArray(fieldIndexInRow), arrType.elementType, dateFormat, csvWriter, nested)
      case mapType: MapType => convertMapToCsv(row.getMap(fieldIndexInRow), mapType, dateFormat, csvWriter, nested)
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
        csvWriter.write(row.get(fieldIndexInRow, dataType).toString)
      case _: DecimalType => csvWriter.write(row.get(fieldIndexInRow, dataType).toString)
      case _ => csvWriter.writeStringField(row.get(fieldIndexInRow, dataType).toString, nested)
    }
  }

  private def convertStructToCsv(row: InternalRow, schema: StructType, dateFormat: FastDateFormat, csvWriter: CountingCsvWriter, nested: Boolean): Unit = {

    val fields = schema.fields
    if (fields.length != 0) {

      if (!nested) {
        csvWriter.write('"')
      }

      csvWriter.write('{')

      var x = 0
      var isNull = true
      while (x < fields.length && isNull) {
        isNull = row.isNullAt(x)
        if (!isNull) {
          writeStructField(x)
        }
        x += 1
      }

      while (x < fields.length) {
        if (!row.isNullAt(x)) {
          csvWriter.write(',')
          writeStructField(x)
        }
        x += 1
      }
      csvWriter.write('}')

      if (!nested) {
        csvWriter.write('"')
      }
    }

    def writeStructField(idx: Int): Unit = {
      csvWriter.writeStringField(fields(idx).name, nested = true)
      csvWriter.write(':')
      writeField(row, idx, fields(idx).dataType, dateFormat, csvWriter, nested = true)
    }
  }

  private def convertArrayToCsv(ar: ArrayData, fieldsType: DataType, dateFormat: FastDateFormat, csvWriter: CountingCsvWriter, nested: Boolean): Unit = {
    if (ar.numElements() == 0) csvWriter.write("[]") else {
      if (!nested) {
        csvWriter.write('"')
      }

      csvWriter.write('[')
      if (ar.isNullAt(0)) csvWriter.write("null") else writeField(ar, fieldIndexInRow = 0, fieldsType, dateFormat, csvWriter, nested = true)
      for (x <- 1 until ar.numElements()) {
        csvWriter.write(',')
        if (ar.isNullAt(x)) csvWriter.write("null") else writeField(ar, x, fieldsType, dateFormat, csvWriter, nested = true)
      }
      csvWriter.write(']')

      if (!nested) {
        csvWriter.write('"')
      }
    }
  }

  private def convertMapToCsv(map: MapData, fieldsType: MapType, dateFormat: FastDateFormat, csvWriter: CountingCsvWriter, nested: Boolean): Unit = {
    val keys = map.keyArray()
    val values = map.valueArray()

    if (!nested) {
      csvWriter.write('"')
    }

    csvWriter.write('{')

    var x = 0
    var isNull = true
    while (x < map.keyArray().numElements() && isNull) {
      isNull = values.isNullAt(x)
      if (!isNull) {
        writeMapField(x)
      }
      x += 1
    }

    while (x < map.keyArray().numElements()) {
      if (!values.isNullAt(x)) {
        csvWriter.write(',')
        writeMapField(x)
      }
      x += 1
    }

    csvWriter.write('}')
    if (!nested) {
      csvWriter.write('"')
    }

    def writeMapField(idx: Int): Unit = {
      csvWriter.write('"')
      writeField(keys, fieldIndexInRow = idx, dataType = fieldsType.keyType, dateFormat = dateFormat, csvWriter = csvWriter, nested = false)
      csvWriter.write('"')
      csvWriter.write(':')
      writeField(values, fieldIndexInRow = idx, dataType = fieldsType.valueType, dateFormat = dateFormat, csvWriter = csvWriter, nested = true)
    }
  }

  private def GetStringFromUTF8(str: UTF8String, nested: Boolean, writer: CountingCsvWriter): Unit = {
    writer.writeStringField(str.toString, nested)
  }
}

case class BlobWriteResource(writer: BufferedWriter, gzip: GZIPOutputStream, csvWriter: CountingCsvWriter, blob: CloudBlockBlob, sas: String)

case class KustoWriteResource(authentication: KustoAuthentication,
                              coordinates: KustoCoordinates,
                              schema: StructType,
                              writeOptions: WriteOptions,
                              tmpTableName: String)

case class PartitionResult(ingestionResult: IngestionResult, partitionId: Int)

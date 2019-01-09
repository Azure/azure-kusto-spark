package com.microsoft.kusto.spark.datasink

import java.io._
import java.nio.charset.StandardCharsets

import com.microsoft.kusto.spark.datasource.KustoOptions.SinkTableCreationMode
import com.microsoft.kusto.spark.datasource.{KustoDataSourceUtils => KDSU}
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.spark.annotation.InterfaceStability.Evolving
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.Iterator

@Evolving
class KustoSink(
   sqlContext: SQLContext,
   cluster: String,
   database: String,
   table: String,
   appId: String,
   appKey: String,
   authorityId: String,
   enableAsync: Boolean = false,
   tableCreation: SinkTableCreationMode.Value = SinkTableCreationMode.FailIfNotExist,
   saveMode: SaveMode = SaveMode.Append) extends Sink with Serializable {

  private val myName = this.getClass.getSimpleName
  val MessageSource = "KustoSink"
  @volatile private var latestBatchId = -1L

  override def toString = "KustoSink"

  @throws[IOException]
  private[kusto] def serializeRows(rows: Iterator[InternalRow], schema: StructType) = {
    val outStream = new ByteArrayOutputStream
    val streamToWrite = outStream

    val writer = new OutputStreamWriter(streamToWrite, StandardCharsets.UTF_8)
    val csvWriter = new CsvWriter(writer, new CsvWriterSettings)
    val csvSerializer = new KustoCsvSerializationUtils(schema)

    for (row <- rows) {
      val columns = csvSerializer.convertRow(row)
      csvWriter.writeRow(columns)
    }

    csvWriter.flush()
    writer.flush()
    outStream.flush()

    csvWriter.close()
    writer.close()

    val bytes = outStream.toByteArray
    outStream.close()

    bytes
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      KDSU.logInfo(myName, s"Skipping already committed batch $batchId")
    } else {

      KustoWriter.write(Option(batchId), data, cluster, database, table, appId, appKey, authorityId, enableAsync, tableCreation, saveMode)
      latestBatchId = batchId
    }
  }
}
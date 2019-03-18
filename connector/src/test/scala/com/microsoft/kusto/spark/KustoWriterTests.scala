package com.microsoft.kusto.spark

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPOutputStream

import com.microsoft.kusto.spark.datasink.{BlobWriteResource, KustoWriter}
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, FunSpec, Matchers}


class KustoWriterTests extends FlatSpec with Matchers {

  def getDF(): DataFrame = {
    val sparkConf = new SparkConf().set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSink")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.read.format("csv").option("header", "false").load("src/test/resources/TestData/ShortTestData.csv")
  }

  "convertRowToCSV" should "convert the row as expected" in {
    val df: DataFrame = getDF()
    val dfRow: InternalRow = getDF().queryExecution.toRdd.collect().head
    KustoWriter.convertRowToCSV(dfRow, df.schema, "UTC").formattedRow shouldEqual Array("John Doe", "1")
  }
  "convertRowToCSV" should "calculate row size as expected" in {
    val df: DataFrame = getDF()
    val dfRow: InternalRow = getDF().queryExecution.toRdd.collect().head
    val expectedSize = "John Doe,1\n".getBytes(StandardCharsets.UTF_8).length
    KustoWriter.convertRowToCSV(dfRow, df.schema, "UTC").rowByteSize shouldEqual expectedSize
  }

  "finalizeFileWrite" should "should flush and close buffers" in {

    val gzip = mock(classOf[GZIPOutputStream])
    val buffer = mock(classOf[BufferedWriter])
    val csvWriter = new CsvWriter(buffer, new CsvWriterSettings)

    val fileWriteResource = BlobWriteResource(buffer, gzip, csvWriter, null)
    KustoWriter.finalizeBlobWrite(fileWriteResource)

    verify(gzip, times(1)).flush()
    verify(gzip, times(1)).close()

    verify(buffer, times(1)).flush()
    verify(buffer, times(1)).close()
  }

}

package com.microsoft.kusto.spark

import java.io.BufferedWriter
import java.util.zip.GZIPOutputStream
import com.microsoft.kusto.spark.datasink.{FileWriteResource, KustoWriter}
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.mockito.Mockito._
import org.scalatest.{FunSpec, Matchers}


class KustoWriterTests extends FunSpec with Matchers {

  def getDF(): DataFrame = {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop")
    val sparkConf = new SparkConf().set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSink")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.read.format("csv").option("header", "false").load("src/test/resources/TestData/ShortTestData.csv")
  }

  describe("convertRowToCSV") {
    it("should convert the row as expected") {
      val df: DataFrame = getDF()
      val dfRow: InternalRow = getDF().queryExecution.toRdd.collect().head
      KustoWriter.convertRowToCSV(dfRow, df.schema, "UTC").formattedRow shouldEqual Array("John Doe", "1")
    }
    it("should calculate row size as expected") {
      val df: DataFrame = getDF()
      val dfRow: InternalRow = getDF().queryExecution.toRdd.collect().head
      val expectedSize = "John Doe,1\n".getBytes("utf-8")
      KustoWriter.convertRowToCSV(dfRow, df.schema, "UTC").formattedRow shouldEqual expectedSize
    }
  }

  describe("finalizeFileWrite") {
    it("should flush and close buffers") {

      val gzip = mock(classOf[GZIPOutputStream])
      val buffer = mock(classOf[BufferedWriter])
      val csvWriter = new CsvWriter(buffer, new CsvWriterSettings)

      val fileWriteResource = FileWriteResource(buffer, gzip, csvWriter, null)
      KustoWriter.finalizeFileWrite(fileWriteResource)

      verify(gzip, times(1)).flush()
      verify(gzip, times(1)).close()

      verify(buffer, times(1)).flush()
      verify(buffer, times(1)).close()
    }
  }

}

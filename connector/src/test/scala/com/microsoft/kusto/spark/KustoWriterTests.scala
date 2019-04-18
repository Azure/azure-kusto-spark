package com.microsoft.kusto.spark

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.util
import java.util.zip.GZIPOutputStream

import com.microsoft.kusto.spark.datasink.{BlobWriteResource, KustoWriter}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}


class KustoWriterTests extends FlatSpec with Matchers {

  def getDF(): DataFrame = {
    val sparkConf = new SparkConf().set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSink")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.read.format("csv").option("header", "false").load("src/test/resources/ShortTestData/ShortTestData.csv")
  }

  "convertRowToCsv" should "convert the row as expected" in {
    val df: DataFrame = getDF()
    val dfRow: InternalRow = getDF().queryExecution.toRdd.collect().head
    KustoWriter.convertRowToCSV(dfRow, df.schema, "UTC").formattedRow shouldEqual Array("John Doe", "1")
  }

  "convertRowToCsv" should "calculate row size as expected" in {
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

  "getColumnsSchema" should "parse table schema correctly" in {
    //Verify part of the following schema:
    // "{\"Name\":\"Subscriptions\",\"OrderedColumns\":[{\"Name\":\"SubscriptionGuid\",\"Type\":\"System.String\",\"CslType\":\"string\"},{\"Name\":\"Identifier\",\"Type\":\"System.String\",\"CslType\":\"string\"},{\"Name\":\"SomeNumber\",\"Type\":\"System.Int64\",\"CslType\":\"long\"},{\"Name\":\"IsCurrent\",\"Type\":\"System.SByte\",\"CslType\":\"bool\"},{\"Name\":\"LastModifiedOn\",\"Type\":\"System.DateTime\",\"CslType\":\"datetime\"},{\"Name\":\"IntegerValue\",\"Type\":\"System.Int32\",\"CslType\":\"int\"}]}"
    val element1 = new util.ArrayList[String]
    element1.add("SubscriptionGuid")
    element1.add("string")
    element1.add("System.String")

    val element2 = new util.ArrayList[String]
    element2.add("Identifier")
    element2.add("string")
    element2.add("System.String")

    val element3 = new util.ArrayList[String]
    element3.add("SomeNumber")
    element3.add("long")
    element3.add("System.Int64")

    val resultTable =  new util.ArrayList[util.ArrayList[String]]
    resultTable.add(element1)
    resultTable.add(element2)
    resultTable.add(element3)

    //("SubscriptionGuid", "string", "System.String")
    val parsedSchema = KDSU.extractSchemaFromResultTable(resultTable)
    // We could add new elements for IsCurrent:bool,LastModifiedOn:datetime,IntegerValue:int
    parsedSchema shouldEqual "SubscriptionGuid:string,Identifier:string,SomeNumber:long"
  }
}

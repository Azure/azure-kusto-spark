package com.microsoft.kusto.spark

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.util
import java.util.TimeZone
import java.util.zip.GZIPOutputStream

import com.microsoft.kusto.spark.datasink.{BlobWriteResource, KustoWriter}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}


@RunWith(classOf[JUnitRunner])
class KustoWriterTests extends FlatSpec with Matchers {

  def getDF(isNestedSchema: Boolean): DataFrame = {
    val sparkConf = new SparkConf().set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSink")
      .setMaster("local[*]")

    val customSchema = if (isNestedSchema) StructType(Array(StructField("Name", StringType, true), StructField("Number", IntegerType, true))) else null
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    if(isNestedSchema) sparkSession.read.format("csv").option("header", "false").schema(customSchema).load("src/test/resources/ShortTestData/ShortTestData.csv")
    else sparkSession.read.format("json").option("header", "true").load("src/test/resources/TestData/TestDynamicFields.csv")
  }

  "convertRowToCsv" should "convert the row as expected" in {
    val df: DataFrame = getDF(isNestedSchema=true)
    val dfRow: InternalRow = df.queryExecution.toRdd.collect().head
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone("UTC"))
    KustoWriter.convertRowToCSV(dfRow, df.schema, dateFormat).formattedRow shouldEqual Array("John Doe", "1")
  }

  "convertRowToCsv" should "convert the row as expected, including nested types." in {
    val df: DataFrame = getDF(isNestedSchema=false)
    val dfRow: InternalRow = df.queryExecution.toRdd.collect().head
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone("UTC"))
    KustoWriter.convertRowToCSV(dfRow, df.schema, dateFormat).formattedRow shouldEqual
      Array("[true,false,null]", "[1,2,3,null]", "", "value", "[\"a\",\"b\",\"c\",null]", "[[\"a\",\"b\",\"c\"],null]",
      "{\"string_ar\":[\"a\",\"b\",\"c\"],\"int\":1,\"string\":\"abc\",\"int_ar\":[1,2,3],\"bool\":true,\"dict_ar\":[{\"int\":1,\"string\":\"a\"},{\"int\":2,\"string\":\"b\"}]}")
  }

  "convertRowToCsv" should "calculate row size as expected" in {
    val df: DataFrame = getDF(isNestedSchema=true)
    val dfRow: InternalRow = df.queryExecution.toRdd.collect().head
    val expectedSize = "John Doe,1\n".getBytes(StandardCharsets.UTF_8).length
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone("UTC"))
    KustoWriter.convertRowToCSV(dfRow, df.schema, dateFormat).rowByteSize shouldEqual expectedSize
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

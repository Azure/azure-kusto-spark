package com.microsoft.kusto.spark

import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util
import java.util.TimeZone
import java.util.zip.GZIPOutputStream

import com.microsoft.kusto.spark.datasink.{BlobWriteResource, CountingCsvWriter, KustoWriter}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}


@RunWith(classOf[JUnitRunner])
class KustoWriterTests extends FlatSpec with Matchers {

  val lineSep: String = java.security.AccessController.doPrivileged(new sun.security.action.GetPropertyAction("line.separator"))

  def getDF(isNestedSchema: Boolean): DataFrame = {

    val sparkConf: SparkConf = new SparkConf().set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSink")
      .setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val customSchema = if (isNestedSchema) StructType(Array(StructField("Name", StringType, nullable = true), StructField("Number", IntegerType, nullable = true))) else null
    if (isNestedSchema) sparkSession.read.format("csv").option("header", "false").schema(customSchema).load("src/test/resources/ShortTestData/ShortTestData.csv")
    else sparkSession.read.format("json").option("header", "true").load("src/test/resources/TestData/TestDynamicFields.json")
  }

  "convertRowToCsv" should "convert the row as expected" in {
    val df: DataFrame = getDF(isNestedSchema = true)
    val dfRow: InternalRow = df.queryExecution.toRdd.collect().head
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone("UTC"))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    val writer = new BufferedWriter(streamWriter)
    val csvWriter = CountingCsvWriter(writer)
    KustoWriter.writeRowAsCSV(dfRow, df.schema, dateFormat, csvWriter)
    writer.flush()
    writer.close()
    byteArrayOutputStream.toString() shouldEqual "\"John,\"\" Doe\",1" + lineSep
  }

  "convertRowToCsv" should "convert the row as expected, including nested types." in {
    val df: DataFrame = getDF(isNestedSchema = false)
    val dfRow: InternalRow = df.queryExecution.toRdd.collect().head
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone("UTC"))
    val expected = Array("[true,false,]", "[1,2,3,]", "", "\"value\"", "[\"a\",\"b\",\"c\",]", "[[\"a\",\"b\",\"c\"],]",
      "{\"bool\":true,\"dict_ar\":[{\"int\":1,\"string\":\"a\"},{\"int\":2,\"string\":\"b\"}],\"int\":1,\"int_ar\":[1,2,3],\"string\":\"abc\",\"string_ar\":[\"a\",\"b\",\"c\"]}").mkString(",") + lineSep
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    val writer = new BufferedWriter(streamWriter)
    val csvWriter = CountingCsvWriter(writer)
    KustoWriter.writeRowAsCSV(dfRow, df.schema, dateFormat, csvWriter)
    writer.flush()
    writer.close()
    val got = byteArrayOutputStream.toString
    got shouldEqual expected
  }

  "convertRowToCsv" should "calculate row size as expected" in {
    val df: DataFrame = getDF(isNestedSchema = true)
    val dfRow: InternalRow = df.queryExecution.toRdd.collect().head
    val expectedSize = ("\"John,\"\" Doe\",1" + lineSep).getBytes(StandardCharsets.UTF_8).length
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone("UTC"))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    val writer = new BufferedWriter(streamWriter)
    val csvWriter = CountingCsvWriter(writer)

    KustoWriter.writeRowAsCSV(dfRow, df.schema, dateFormat, csvWriter)
    writer.flush()
    writer.close()
    csvWriter.getProgress shouldEqual expectedSize
  }

  "finalizeFileWrite" should "should flush and close buffers" in {
    val gzip = mock(classOf[GZIPOutputStream])
    val buffer = mock(classOf[BufferedWriter])
    val csvWriter = CountingCsvWriter(buffer)

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

    val resultTable = new util.ArrayList[util.ArrayList[String]]
    resultTable.add(element1)
    resultTable.add(element2)
    resultTable.add(element3)

    //("SubscriptionGuid", "string", "System.String")
    val parsedSchema = KDSU.extractSchemaFromResultTable(resultTable)
    // We could add new elements for IsCurrent:bool,LastModifiedOn:datetime,IntegerValue:int
    parsedSchema shouldEqual "['SubscriptionGuid']:string,['Identifier']:string,['SomeNumber']:long"
  }

  "convertRowToCsv" should "convert the row as expected with maps" in {
    val sparkConf: SparkConf = new SparkConf().set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSink")
      .setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val someData = List(
      Map("asd" -> Row(Array("stringVal")),
        "asd2" -> Row(Array("stringVal2")))
    )
    val someSchema = List(
      StructField("mapToArray", MapType(StringType, new StructType().add("arrayStrings", ArrayType(StringType, containsNull = true), nullable = true), valueContainsNull = true), nullable = true)
    )

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(KustoWriterTests.asRows(someData)),
      StructType(KustoWriterTests.asSchema(someSchema))
    )

    val dfRow: InternalRow = df.queryExecution.toRdd.collect().head
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone("UTC"))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    val writer = new BufferedWriter(streamWriter)
    val csvWriter = CountingCsvWriter(writer)

    KustoWriter.writeRowAsCSV(dfRow, df.schema, dateFormat, csvWriter)
    writer.flush()
    writer.close()
    byteArrayOutputStream.toString shouldEqual Array("{\"asd\":{\"arrayStrings\":[\"stringVal\"]},\"asd2\":{\"arrayStrings\":[\"stringVal2\"]}}").mkString(",") + lineSep
  }
}

object KustoWriterTests {
  def asRows[U](values: List[U]): List[Row] = {
    values.map {
      case row: Row => row.asInstanceOf[Row]
      case prod: Product => Row(prod.productIterator.toList: _*)
      case any => Row(any)
    }
  }

  def asSchema[U <: Product](fields: List[U]): List[StructField] = {
    fields.map {
      case x: StructField => x.asInstanceOf[StructField]
      case (name: String, dataType: DataType, nullable: Boolean) =>
        StructField(
          name,
          dataType,
          nullable
        )
    }
  }
}

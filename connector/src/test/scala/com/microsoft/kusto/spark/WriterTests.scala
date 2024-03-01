//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark

import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.kusto.spark.datasink._
import com.microsoft.kusto.spark.utils.{KustoConstants, KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.TimeZone
import java.util.zip.GZIPOutputStream

class WriterTests extends AnyFlatSpec with Matchers {

  val objectMapper = new ObjectMapper

  val lineSep: String = System.lineSeparator()
  val sparkConf: SparkConf = new SparkConf()
    .set("spark.testing", "true")
    .set("spark.ui.enabled", "false")
    .setAppName("SimpleKustoDataSink")
    .setMaster("local[*]")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  def getDF(isNestedSchema: Boolean): DataFrame = {
    val customSchema =
      if (isNestedSchema)
        StructType(
          Array(
            StructField(KustoConstants.Schema.NAME, StringType, nullable = true),
            StructField("Number", IntegerType, nullable = true)))
      else null
    if (isNestedSchema)
      sparkSession.read
        .format("csv")
        .option("header", "false")
        .schema(customSchema)
        .load("src/test/resources/ShortTestData/ShortTestData.csv")
    else
      sparkSession.read
        .format("json")
        .option("header", "true")
        .load("src/test/resources/TestData/json/TestDynamicFields.json")
  }

  "convertRowToCsv" should "convert the row as expected" in {
    val df: DataFrame = getDF(isNestedSchema = true)
    val dfRow: InternalRow = df.queryExecution.toRdd.collect().head

    val byteArrayOutputStream = new ByteArrayOutputStream()
    val streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    val writer = new BufferedWriter(streamWriter)
    val csvWriter = CountingWriter(writer)
    RowCSVWriterUtils.writeRowAsCSV(
      dfRow,
      df.schema,
      TimeZone.getTimeZone("UTC").toZoneId,
      csvWriter)
    writer.flush()
    writer.close()
    byteArrayOutputStream.toString() shouldEqual "\"John,\"\" Doe\",1" + lineSep
  }

  "convertRowToCsv" should "convert the row as expected, including nested types." in {
    val df: DataFrame = getDF(isNestedSchema = false)
    val dfRow: InternalRow = df.queryExecution.toRdd.collect().head
    val expected =
      """"[true,false,null]","[1,2,3,null]",,"value","[""a"",""b"",""c"",null]","[[""a"",""b"",""c""],null]","{""bool"":true,""dict_ar"":[{""int"":1,""string"":""a""},{""int"":2,""string"":""b""}],""int"":1,""int_ar"":[1,2,3],""string"":""abc"",""string_ar"":[""a"",""b"",""c""]}""""
        .concat(lineSep)

    val byteArrayOutputStream = new ByteArrayOutputStream()
    val streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    val writer = new BufferedWriter(streamWriter)
    var csvWriter = CountingWriter(writer)
    RowCSVWriterUtils.writeRowAsCSV(
      dfRow,
      df.schema,
      TimeZone.getTimeZone("UTC").toZoneId,
      csvWriter)
    writer.flush()
    val got = byteArrayOutputStream.toString()

    got shouldEqual expected
  }

  "convertRowToCsv" should "calculate row size as expected" in {
    val df: DataFrame = getDF(isNestedSchema = true)
    val dfRow: InternalRow = df.queryExecution.toRdd.collect().head
    val expectedSize = ("\"John,\"\" Doe\",1" + lineSep).getBytes(StandardCharsets.UTF_8).length
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    val writer = new BufferedWriter(streamWriter)
    val csvWriter = CountingWriter(writer)

    RowCSVWriterUtils.writeRowAsCSV(
      dfRow,
      df.schema,
      TimeZone.getTimeZone("UTC").toZoneId,
      csvWriter)
    writer.flush()
    writer.close()
    csvWriter.getCounter shouldEqual expectedSize
  }

  "finalizeFileWrite" should "should flush and close buffers" in {
    val gzip = mock(classOf[GZIPOutputStream])
    val buffer = mock(classOf[BufferedWriter])
    val csvWriter = CountingWriter(buffer)

    val fileWriteResource = BlobWriteResource(buffer, gzip, csvWriter, null, null)
    KustoWriter.finalizeBlobWrite(fileWriteResource)

    verify(gzip, times(1)).flush()
    verify(gzip, times(1)).close()

    verify(buffer, times(1)).flush()
    verify(buffer, times(1)).close()
  }

  "getColumnsSchema" should "parse table schema correctly" in {
    // Verify part of the following schema:
    // "{\"Name\":\"Subscriptions\",\"OrderedColumns\":[{\"Name\":\"SubscriptionGuid\",\"Type\":\"System.String\",\"CslType\":\"string\"},{\"Name\":\"Identifier\",\"Type\":\"System.String\",\"CslType\":\"string\"},{\"Name\":\"SomeNumber\",\"Type\":\"System.Int64\",\"CslType\":\"long\"},{\"Name\":\"IsCurrent\",\"Type\":\"System.SByte\",\"CslType\":\"bool\"},{\"Name\":\"LastModifiedOn\",\"Type\":\"System.DateTime\",\"CslType\":\"datetime\"},{\"Name\":\"IntegerValue\",\"Type\":\"System.Int32\",\"CslType\":\"int\"}]}"
    val element1 = objectMapper.createObjectNode
    element1.put(KustoConstants.Schema.CSLTYPE, "string")
    element1.put(KustoConstants.Schema.NAME, "SubscriptionGuid")
    element1.put(KustoConstants.Schema.TYPE, "System.String")

    val element2 = objectMapper.createObjectNode
    element2.put(KustoConstants.Schema.NAME, "Identifier")
    element2.put(KustoConstants.Schema.CSLTYPE, "string")
    element2.put(KustoConstants.Schema.TYPE, "System.String")

    val element3 = objectMapper.createObjectNode
    element3.put(KustoConstants.Schema.TYPE, "System.Int64")
    element3.put(KustoConstants.Schema.CSLTYPE, "long")
    element3.put(KustoConstants.Schema.NAME, "SomeNumber")

    val resultTable = objectMapper.createArrayNode()
    resultTable.add(element1)
    resultTable.add(element2)
    resultTable.add(element3)

    val parsedSchema = KDSU.extractSchemaFromResultTable(resultTable)
    // We could add new elements for IsCurrent:bool,LastModifiedOn:datetime,IntegerValue:int
    parsedSchema shouldEqual "['SubscriptionGuid']:string,['Identifier']:string,['SomeNumber']:long"
  }

  "convertRowToCsv" should "convert the row as expected with maps and right escaping" in {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSink")
      .setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val someData =
      List(Map("asd" -> Row(Array("stringVal\n\r\\\"")), "asd2" -> Row(Array("stringVal2\b\f"))))

    val tz = ZonedDateTime.parse("2018-06-18T15:00:00.123456789+04:00")
    val ts =
      Timestamp.valueOf(tz.withZoneSameInstant(TimeZone.getDefault.toZoneId).toLocalDateTime)

    val someDateData = List(
      Map(
        "asd" -> Row(
          Date.valueOf("1991-09-07"),
          ts,
          false,
          java.math.BigDecimal.valueOf(1 / 100000.toDouble))))

    val someEmptyArrays = List(Row(Row(Array(null, ""), "")), Row(Row(null, "")))

    val someDecimalData = List(
      Row(BigDecimal("123456789.123456789")),
      Row(BigDecimal("123456789123456789.123456789123456789")),
      Row(BigDecimal("-123456789123456789.123456789123456789")),
      Row(BigDecimal("0.123456789123456789")))
    val otherDecimalData = List(
      Row(BigDecimal("-123456789123456789")),
      Row(BigDecimal("0")),
      Row(BigDecimal("0.1")),
      Row(BigDecimal("-0.1")))

    val someSchema = List(
      StructField(
        "mapToArray",
        MapType(
          StringType,
          new StructType()
            .add("arrayStrings", ArrayType(StringType, containsNull = true), nullable = true),
          valueContainsNull = true),
        nullable = true))
    val someDateSchema = List(
      StructField(
        "mapToStruct",
        MapType(
          StringType,
          new StructType()
            .add("date", DateType, nullable = true)
            .add("time", TimestampType)
            .add("booly", BooleanType)
            .add("deci", DataTypes.createDecimalType(20, 14)),
          valueContainsNull = true),
        nullable = true))
    val someEmptyArraysSchema = List(
      StructField(
        "emptyStruct",
        new StructType()
          .add("emptyArray", ArrayType(StringType, containsNull = true), nullable = true)
          .add("emptyString", StringType)))
    val someDecimalSchema = List(StructField("BigDecimals", DataTypes.createDecimalType(38, 10)))

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(WriterTests.asRows(someData)),
      StructType(WriterTests.asSchema(someSchema)))

    val dateDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(WriterTests.asRows(someDateData)),
      StructType(WriterTests.asSchema(someDateSchema)))

    val emptyArraysDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(WriterTests.asRows(someEmptyArrays)),
      StructType(WriterTests.asSchema(someEmptyArraysSchema)))

    val someDecimalDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(WriterTests.asRows(someDecimalData)),
      StructType(WriterTests.asSchema(someDecimalSchema)))

    val otherDecimalDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(WriterTests.asRows(otherDecimalData)),
      StructType(WriterTests.asSchema(someDecimalSchema)))

    val dfRow: InternalRow = df.queryExecution.toRdd.collect().head
    val dfRow2 = dateDf.queryExecution.toRdd.collect.head
    val dfRows3 = emptyArraysDf.queryExecution.toRdd.collect
    val dfRows4 = someDecimalDf.queryExecution.toRdd.collect
    val dfRows5 = otherDecimalDf.queryExecution.toRdd.collect

    val byteArrayOutputStream = new ByteArrayOutputStream()
    val streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    val writer = new BufferedWriter(streamWriter)
    val csvWriter = CountingWriter(writer)

    RowCSVWriterUtils.writeRowAsCSV(dfRow, df.schema, tz.getZone, csvWriter)

    writer.flush()
    val res1 = byteArrayOutputStream.toString
    res1 shouldEqual "\"{\"\"asd\"\":{\"\"arrayStrings\"\":[\"\"stringVal\\n\\r\\\\\\\"\"\"\"]},\"\"asd2\"\":{\"\"arrayStrings\"\":[\"\"stringVal2\\b\\f\"\"]}}\"" + lineSep
    val sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")

    byteArrayOutputStream.reset()
    RowCSVWriterUtils.writeRowAsCSV(dfRow2, dateDf.schema, tz.getZone, csvWriter)
    writer.flush()
    val res2 = byteArrayOutputStream.toString
    res2 shouldEqual "\"{\"\"asd\"\":{\"\"date\"\":\"\"1991-09-07\"\"," +
      s"""""time"":""${sdf.format(ts.toInstant.atZone(tz.getZone))}"",""" +
      "\"\"booly\"\":false,\"\"deci\"\":\"\"0.00001000000000\"\"}}\"" + lineSep

    byteArrayOutputStream.reset()
    RowCSVWriterUtils.writeRowAsCSV(dfRows3(0), emptyArraysDf.schema, tz.getZone, csvWriter)
    RowCSVWriterUtils.writeRowAsCSV(dfRows3(1), emptyArraysDf.schema, tz.getZone, csvWriter)
    writer.flush()
    val res3 = byteArrayOutputStream.toString
    res3 shouldEqual "\"{\"\"emptyArray\"\":[null,\"\"\"\"],\"\"emptyString\"\":\"\"\"\"}\"" + lineSep + "\"{\"\"emptyString\"\":\"\"\"\"}\"" + lineSep

    byteArrayOutputStream.reset()
    for (row <- dfRows4) {
      RowCSVWriterUtils.writeRowAsCSV(row, someDecimalDf.schema, tz.getZone, csvWriter)
    }

    writer.flush()
    val res4 = byteArrayOutputStream.toString
    res4 shouldEqual "\"123456789.1234567890\"" + lineSep + "\"123456789123456789.1234567891\"" + lineSep + "\"-123456789123456789.1234567891\"" + lineSep +
      "\"0.1234567891\"" + lineSep

    byteArrayOutputStream.reset()
    for (row <- dfRows5) {
      RowCSVWriterUtils.writeRowAsCSV(row, someDecimalDf.schema, tz.getZone, csvWriter)
    }

    writer.flush()
    writer.close()
    val res5 = byteArrayOutputStream.toString
    res5 shouldEqual "\"-123456789123456789.0000000000\"" + lineSep + "\"0.0000000000\"" + lineSep + "\"0.1000000000\"" +
      lineSep + "\"-0.1000000000\"" + lineSep
  }
}

object WriterTests {
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
        StructField(name, dataType, nullable)
    }
  }
}

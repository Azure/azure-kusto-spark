// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

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

    while (rowCount < expectedNumberOfRows && timeElapsedMs < timeoutMs) {
      val result = kustoAdminClient.executeQuery(database, query).getPrimaryResults
      result.next()
      rowCount = result.getInt(0)
      Thread.sleep(sleepPeriodMs)
      timeElapsedMs += sleepPeriodMs
    }

    if (cleanupAllTables) {
      if (tableCleanupPrefix.isEmpty) {
        throw new InvalidParameterException(
          "Tables cleanup prefix must be set if 'cleanupAllTables' is 'true'")
      }
      tryDropAllTablesByPrefix(kustoAdminClient, database, tableCleanupPrefix)
    } else {
      kustoAdminClient.executeMgmt(database, generateDropTablesCommand(table))
    }

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

  def tryDropAllTablesByPrefix(
      kustoAdminClient: Client,
      database: String,
      tablePrefix: String): Unit = {
    try {
      val res = kustoAdminClient.executeMgmt(
        database,
        generateFindCurrentTempTablesCommand(Array(tablePrefix)))
      val tablesToCleanup = res.getPrimaryResults.getData.asScala.map(row => row.get(0))

      if (tablesToCleanup.nonEmpty) {
        kustoAdminClient.executeMgmt(
          database,
          generateDropTablesCommand(tablesToCleanup.mkString(",")))
      }
    } catch {
      case exception: Exception =>
        KDSU.logWarn(
          className,
          s"Failed to delete temporary tables with exception: ${exception.getMessage}")
    }
  }

    val byteArrayOutputStream = new ByteArrayOutputStream()
    val streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    val writer = new BufferedWriter(streamWriter)
    val csvWriter = CountingWriter(writer)

    RowCSVWriterUtils.writeRowAsCSV(dfRow, df.schema, tz.getZone, csvWriter)

    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.executeMgmt(
      kustoConnectionOptions.database,
      generateTempTableCreateCommand(table, targetSchema))
    table
  }

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

  "convertRowToCsv" should "convert the row as expected with binary data" in {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSink")
      .setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val streamWriter = new OutputStreamWriter(byteArrayOutputStream)
    val writer = new BufferedWriter(streamWriter)
    val csvWriter = CountingWriter(writer)

    val someData =
      List(
        "Test string for spark binary".getBytes(),
        "A second test string for spark binary".getBytes(),
        null
      )

    val someSchema = List(StructField("binaryString", BinaryType, nullable = true))

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(WriterTests.asRows(someData)),
      StructType(WriterTests.asSchema(someSchema)))

    val dfRows: Array[InternalRow] = df.queryExecution.toRdd.collect()

    RowCSVWriterUtils.writeRowAsCSV(dfRows(0), df.schema, TimeZone.getTimeZone("UTC").toZoneId, csvWriter)
    RowCSVWriterUtils.writeRowAsCSV(dfRows(1), df.schema, TimeZone.getTimeZone("UTC").toZoneId, csvWriter)
    RowCSVWriterUtils.writeRowAsCSV(dfRows(2), df.schema, TimeZone.getTimeZone("UTC").toZoneId, csvWriter)

    writer.flush()
    val res1 = byteArrayOutputStream.toString
    res1 shouldEqual "\"VGVzdCBzdHJpbmcgZm9yIHNwYXJrIGJpbmFyeQ==\"" + lineSep + "\"QSBzZWNvbmQgdGVzdCBzdHJpbmcgZm9yIHNwYXJrIGJpbmFyeQ==\"" + lineSep + lineSep
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
    val ingestionStorageParam =
      new IngestionStorageParameters(storageContainerUrl, containerName, "", "")
    val sas = ContainerProvider.getUserDelegatedSas(
      listPermissions = true,
      cacheExpirySeconds = 1 * 60 * 60,
      ingestionStorageParameter = ingestionStorageParam)
    sas
  }
}
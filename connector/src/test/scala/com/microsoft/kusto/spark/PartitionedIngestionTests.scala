// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.microsoft.kusto.spark.datasink._
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.TimeZone

class PartitionedIngestionTests extends AnyFlatSpec with Matchers {

  val lineSep: String = System.lineSeparator()
  val sparkConf: SparkConf = new SparkConf()
    .set("spark.testing", "true")
    .set("spark.ui.enabled", "false")
    .setAppName("PartitionedIngestionTest")
    .setMaster("local[*]")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._

  private def createTestDF(): DataFrame = {
    val data = Seq(
      ("Tenant1", "Alice", 10),
      ("Tenant2", "Bob", 20),
      ("Tenant3", "Charlie", 30),
      ("Tenant1", "Diana", 40))
    data.toDF("Tenant", "Name", "Value")
  }

  "repartition by partition column" should "co-locate rows with the same key" in {
    val df = createTestDF()
    val repartitioned = df.repartition(df.col("Tenant"))
    val rdd = repartitioned.queryExecution.toRdd

    // Collect rows grouped by Spark partition
    val schema = repartitioned.schema
    val tenantIdx = schema.fieldIndex("Tenant")
    val partitionContents = rdd
      .mapPartitionsWithIndex { (idx, iter) =>
        iter.map(r => (idx, r.getUTF8String(tenantIdx).toString))
      }
      .collect()

    // Each tenant's rows should all be in the same partition (no splitting across partitions)
    val tenantToPartitions = partitionContents.groupBy(_._2).map { case (tenant, entries) =>
      (tenant, entries.map(_._1).distinct)
    }
    for ((tenant, partitions) <- tenantToPartitions) {
      withClue(s"Tenant '$tenant' should be in exactly 1 partition: ") {
        partitions.length shouldBe 1
      }
    }

    // Should have all 3 tenants
    tenantToPartitions.size shouldBe 3
  }

  "partitioned CSV serialization" should "produce separate output per key value" in {
    val df = createTestDF()
    val repartitioned = df.repartition(df.col("Tenant"))
    val schema = repartitioned.schema
    val tenantIdx = schema.fieldIndex("Tenant")
    val timeZone = TimeZone.getTimeZone("UTC").toZoneId

    // Collect all rows across all partitions
    val allRows = repartitioned.queryExecution.toRdd.collect()

    // Simulate per-key CSV writing (same logic as ingestRowsPartitioned)
    val keyOutputs = scala.collection.mutable.HashMap[String, ByteArrayOutputStream]()
    val keyWriters = scala.collection.mutable.HashMap[String, CountingWriter]()
    val keyBufferedWriters = scala.collection.mutable.HashMap[String, BufferedWriter]()

    for (row <- allRows) {
      val key = row.getUTF8String(tenantIdx).toString

      if (!keyOutputs.contains(key)) {
        val baos = new ByteArrayOutputStream()
        val bw = new BufferedWriter(new OutputStreamWriter(baos, StandardCharsets.UTF_8))
        val csvWriter = CountingWriter(bw)
        keyOutputs.put(key, baos)
        keyWriters.put(key, csvWriter)
        keyBufferedWriters.put(key, bw)
      }

      RowCSVWriterUtils.writeRowAsCSV(row, schema, timeZone, keyWriters(key))
    }

    // Flush all writers
    keyBufferedWriters.values.foreach(_.flush())

    // Should have 3 distinct keys
    keyOutputs.size shouldBe 3
    keyOutputs.keys.toSet shouldBe Set("Tenant1", "Tenant2", "Tenant3")

    // Tenant1 should have 2 rows, others 1
    val tenant1Lines =
      keyOutputs("Tenant1").toString(StandardCharsets.UTF_8.name()).split(lineSep).length
    val tenant2Lines =
      keyOutputs("Tenant2").toString(StandardCharsets.UTF_8.name()).split(lineSep).length
    val tenant3Lines =
      keyOutputs("Tenant3").toString(StandardCharsets.UTF_8.name()).split(lineSep).length

    tenant1Lines shouldBe 2
    tenant2Lines shouldBe 1
    tenant3Lines shouldBe 1
  }

  "partitioned CSV serialization" should "handle null partition key values" in {
    val schema = StructType(
      Array(
        StructField("Tenant", StringType, nullable = true),
        StructField("Value", IntegerType, nullable = true)))

    val data = Seq(Row("Tenant1", 10), Row(null, 20), Row("Tenant1", 30))
    val rdd = sparkSession.sparkContext.parallelize(data)
    val df = sparkSession.createDataFrame(rdd, schema)

    val internalRows = df.queryExecution.toRdd.collect()
    val tenantIdx = schema.fieldIndex("Tenant")
    val timeZone = TimeZone.getTimeZone("UTC").toZoneId

    val keyOutputs = scala.collection.mutable.HashMap[String, ByteArrayOutputStream]()
    val keyWriters = scala.collection.mutable.HashMap[String, CountingWriter]()
    val keyBufferedWriters = scala.collection.mutable.HashMap[String, BufferedWriter]()

    for (row <- internalRows) {
      val key =
        if (row.isNullAt(tenantIdx)) "_null_"
        else row.getUTF8String(tenantIdx).toString

      if (!keyOutputs.contains(key)) {
        val baos = new ByteArrayOutputStream()
        val bw = new BufferedWriter(new OutputStreamWriter(baos, StandardCharsets.UTF_8))
        val csvWriter = CountingWriter(bw)
        keyOutputs.put(key, baos)
        keyWriters.put(key, csvWriter)
        keyBufferedWriters.put(key, bw)
      }

      RowCSVWriterUtils.writeRowAsCSV(row, schema, timeZone, keyWriters(key))
    }

    keyBufferedWriters.values.foreach(_.flush())

    keyOutputs.size shouldBe 2
    keyOutputs.keys.toSet shouldBe Set("Tenant1", "_null_")

    val tenant1Lines =
      keyOutputs("Tenant1").toString(StandardCharsets.UTF_8.name()).split(lineSep).length
    val nullLines =
      keyOutputs("_null_").toString(StandardCharsets.UTF_8.name()).split(lineSep).length

    tenant1Lines shouldBe 2
    nullLines shouldBe 1
  }

  "partitioned ingestion" should "support integer partition keys" in {
    val data = Seq((1, "Alice"), (2, "Bob"), (1, "Charlie"))
    val df = data.toDF("Region", "Name")
    val repartitioned = df.repartition(df.col("Region"))
    val schema = repartitioned.schema
    val regionIdx = schema.fieldIndex("Region")
    val timeZone = TimeZone.getTimeZone("UTC").toZoneId

    val allRows = repartitioned.queryExecution.toRdd.collect()
    val keyOutputs = scala.collection.mutable.HashMap[String, ByteArrayOutputStream]()
    val keyWriters = scala.collection.mutable.HashMap[String, CountingWriter]()
    val keyBufferedWriters = scala.collection.mutable.HashMap[String, BufferedWriter]()

    for (row <- allRows) {
      val key = row.getInt(regionIdx).toString

      if (!keyOutputs.contains(key)) {
        val baos = new ByteArrayOutputStream()
        val bw = new BufferedWriter(new OutputStreamWriter(baos, StandardCharsets.UTF_8))
        val csvWriter = CountingWriter(bw)
        keyOutputs.put(key, baos)
        keyWriters.put(key, csvWriter)
        keyBufferedWriters.put(key, bw)
      }

      RowCSVWriterUtils.writeRowAsCSV(row, schema, timeZone, keyWriters(key))
    }

    keyBufferedWriters.values.foreach(_.flush())

    keyOutputs.size shouldBe 2
    keyOutputs.keys.toSet shouldBe Set("1", "2")
  }
}

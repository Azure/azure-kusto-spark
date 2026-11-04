// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.{Client, ClientFactory}
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.KustoTestUtils.getSystemTestOptions
import com.microsoft.kusto.spark.common.KustoDebugOptions
import com.microsoft.kusto.spark.datasink.v2.IngestV2TestHooks
import com.microsoft.kusto.spark.datasink.{
  KustoSinkOptions,
  SchemaAdjustmentMode,
  SinkTableCreationMode,
  SparkIngestionProperties,
  WriteMode
}
import com.microsoft.kusto.spark.utils.KustoQueryUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * End-to-end tests for the ingest-v2 write path (hidden `enableIngestV2` option).
 *
 * The test cluster's configuration asks for queued ingestion into storage containers, which would
 * silently select the existing path. The suite therefore forces the service's preferred ingestion
 * method to REST and makes any fallback to the existing path fail the test, so every write below
 * really goes through ingest-v2 (except where a test turns that off on purpose). Results are
 * compared with what the existing path writes for the same data.
 *
 * Requires the environment variables kustoCluster, kustoDatabase and kustoAadAuthorityID (an az
 * CLI token is used when accessToken is not set). Run with: mvn test -pl connector
 * -Dsuites=com.microsoft.kusto.spark.KustoSinkIngestV2E2E -DtagsToExclude=none
 */
class KustoSinkIngestV2E2E extends AnyFlatSpec with BeforeAndAfterAll {
  private val waitTimeout: FiniteDuration = 10.minutes
  private val prefix = "IngestV2E2E"

  private val spark: SparkSession = SparkSession
    .builder()
    .appName("KustoSinkIngestV2E2E")
    .master("local[4]")
    .getOrCreate()

  private lazy val conn = getSystemTestOptions
  private lazy val adminClient: Client = ClientFactory.createClient(
    ConnectionStringBuilder.createWithAadAccessTokenAuthentication(conn.cluster, conn.accessToken))
  private val createdTables = mutable.Set[String]()

  override def beforeAll(): Unit = {
    super.beforeAll()
    forceIngestV2()
  }

  private def forceIngestV2(): Unit = {
    IngestV2TestHooks.configOverride =
      Some(config => config.copy(preferredIngestionMethod = Some("Rest")))
    IngestV2TestHooks.failInsteadOfFallback = true
  }

  override def afterAll(): Unit = {
    try {
      IngestV2TestHooks.reset()
      createdTables.synchronized(createdTables.toList).grouped(20).foreach { tables =>
        scala.util.Try(
          adminClient.executeMgmt(conn.database, s".drop tables (${tables.mkString(",")}) ifexists"))
      }
    } finally {
      super.afterAll()
      spark.stop()
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private def newTable(name: String): String = {
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${name}_${UUID.randomUUID()}")
    createdTables.synchronized(createdTables += table)
    table
  }

  private def write(
      df: DataFrame,
      table: String,
      mode: WriteMode.Value,
      ingestV2: Boolean = true,
      extra: Map[String, String] = Map.empty): Unit = {
    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, conn.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, conn.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, conn.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, mode.toString)
      .option(KustoDebugOptions.KUSTO_ENABLE_INGEST_V2, ingestV2.toString)
      .options(extra)
      .mode(SaveMode.Append)
      .save()
  }

  private def mgmt(command: String): Unit = adminClient.executeMgmt(conn.database, command)

  private def queryLong(query: String): Long = {
    val result = adminClient.executeQuery(conn.database, query).getPrimaryResults
    assert(result.next(), s"no result for: $query")
    result.getLong(0)
  }

  private def queryStrings(query: String): Seq[String] = {
    val result = adminClient.executeQuery(conn.database, query).getPrimaryResults
    val values = mutable.ArrayBuffer[String]()
    while (result.next()) values += result.getString(0)
    values.toList
  }

  private def mgmtStrings(command: String): Seq[String] = {
    val result = adminClient.executeMgmt(conn.database, command).getPrimaryResults
    val values = mutable.ArrayBuffer[String]()
    while (result.next()) values += result.getString(0)
    values.toList
  }

  private def count(table: String): Long = queryLong(s"['$table'] | count")

  private def waitForCount(table: String, expected: Long): Unit = {
    val deadline = waitTimeout.fromNow
    var current = count(table)
    while (current < expected && deadline.hasTimeLeft()) {
      Thread.sleep(10000)
      current = count(table)
    }
    assert(current == expected, s"table $table has $current rows, expected $expected")
  }

  /** Every row as text, with how many times it appears. */
  private def rowsOf(table: String): Map[String, Int] =
    queryStrings(s"['$table'] | project r = tostring(pack_all())")
      .groupBy(identity)
      .map { case (row, copies) => row -> copies.size }

  private def assertSameRows(expectedTable: String, actualTable: String): Unit = {
    val expected = rowsOf(expectedTable)
    val actual = rowsOf(actualTable)
    val missing = expected.toSeq.filter { case (r, n) => actual.getOrElse(r, 0) != n }
    val unexpected = actual.keySet.diff(expected.keySet)
    assert(
      missing.isEmpty && unexpected.isEmpty,
      s"$actualTable differs from $expectedTable: missing/miscounted ${missing.take(3)}, " +
        s"unexpected ${unexpected.take(3)}")
  }

  private def ids(from: Long, to: Long, partitions: Int): DataFrame =
    spark
      .range(from, to, 1, partitions)
      .select(col("id"), concat(lit("value-"), col("id")).as("name"))

  private def assertIds(table: String, from: Long, to: Long): Unit = {
    val n = to - from
    waitForCount(table, n)
    assert(queryLong(s"['$table'] | summarize count_distinct(id)") == n)
    assert(queryLong(s"['$table'] | summarize sum(id)") == (from until to).sum)
  }

  private val allTypesSchema = StructType(
    Seq(
      StructField("id", LongType),
      StructField("text", StringType),
      StructField("int", IntegerType),
      StructField("long", LongType),
      StructField("double", DoubleType),
      StructField("bool", BooleanType),
      StructField("ts", TimestampType),
      StructField("date", DateType),
      StructField("dec", DecimalType(20, 5)),
      StructField("arr", ArrayType(IntegerType)),
      StructField("map", MapType(StringType, StringType)),
      StructField(
        "struct",
        StructType(Seq(StructField("a", IntegerType), StructField("b", StringType))))))

  /** Values that are easy to get wrong in CSV: separators, quotes, line breaks, nulls, unicode. */
  private lazy val allTypes: DataFrame = {
    val texts = Seq(
      "plain",
      "comma, inside",
      "quote \" inside",
      "line\nbreak",
      "crlf\r\nbreak",
      "unicode ü 日本 🙂",
      "",
      null,
      "  spaces  ",
      "tab\tinside")
    val rows = (0 until 2000).map { i =>
      val nullable = i % 7 == 0
      Row(
        i.toLong,
        texts(i % texts.size),
        if (nullable) null else i * 3,
        if (nullable) null else i.toLong * 1000000007L,
        if (nullable) null else i / 7.0 - 100,
        if (nullable) null else i % 2 == 0,
        if (nullable) null else Timestamp.from(Instant.parse("2024-02-29T23:59:59.123Z").plusSeconds(i)),
        if (nullable) null else Date.valueOf("2020-01-01"),
        if (nullable) null else new JBigDecimal(s"$i.12345"),
        if (nullable) null else Seq(i, i + 1),
        if (nullable) null else Map("k" -> s"v,$i", "q" -> "\""),
        if (nullable) null else Row(i, s"s$i"))
    }
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 8), allTypesSchema)
  }

  private lazy val existingPathTable: String = {
    val table = newTable("existing_path_all_types")
    write(allTypes, table, WriteMode.Transactional, ingestV2 = false)
    waitForCount(table, 2000)
    table
  }

  private def ingestionProperties(build: SparkIngestionProperties => Unit): Map[String, String] = {
    val props = new SparkIngestionProperties()
    build(props)
    Map(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON -> props.toString)
  }

  // ---------------------------------------------------------------------------------------------
  // Data correctness compared with the existing path
  // ---------------------------------------------------------------------------------------------

  "ingest-v2 transactional write" should "store exactly the same rows as the existing path" taggedAs KustoE2E in {
    val table = newTable("txn_all_types")
    write(allTypes, table, WriteMode.Transactional)
    assert(count(table) == 2000, "a transactional write must be complete when it returns")
    assertSameRows(existingPathTable, table)
  }

  "ingest-v2 queued write" should "store exactly the same rows as the existing path" taggedAs KustoE2E in {
    val table = newTable("queued_all_types")
    write(allTypes, table, WriteMode.Queued)
    waitForCount(table, 2000)
    assertSameRows(existingPathTable, table)
  }

  // ---------------------------------------------------------------------------------------------
  // Batching and scale
  // ---------------------------------------------------------------------------------------------

  "ingest-v2" should "split partitions into many blobs and batches without losing rows" taggedAs KustoE2E in {
    val table = newTable("many_blobs")
    // 1 MB blobs: about 30 blobs per partition, more than one batch (maxBlobsPerBatch is 20).
    val df = spark
      .range(0, 400000, 1, 4)
      .select(col("id"), concat(lit("x" * 60), col("id")).as("name"))
    write(df, table, WriteMode.Transactional, extra = Map(KustoSinkOptions.KUSTO_CLIENT_BATCHING_LIMIT -> "1"))
    assertIds(table, 0, 400000)
  }

  it should "commit a transactional write with 200 partitions" taggedAs KustoE2E in {
    val table = newTable("many_partitions")
    write(ids(0, 200000, 200), table, WriteMode.Transactional)
    assert(count(table) == 200000)
    assertIds(table, 0, 200000)
  }

  it should "handle concurrent writes into the same table" taggedAs KustoE2E in {
    val table = newTable("concurrent")
    mgmt(s".create table ['$table'] (id:long, name:string)")
    val pool = Executors.newFixedThreadPool(4)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    try {
      val writes = (0 until 4).map { w =>
        val mode = if (w % 2 == 0) WriteMode.Transactional else WriteMode.Queued
        Future(write(ids(w * 25000L, (w + 1) * 25000L, 8), table, mode))
      }
      Await.result(Future.sequence(writes), 20.minutes)
    } finally pool.shutdown()
    assertIds(table, 0, 100000)
  }

  it should "write an empty DataFrame without failing" taggedAs KustoE2E in {
    val txn = newTable("empty_txn")
    val queued = newTable("empty_queued")
    write(ids(0, 0, 4), txn, WriteMode.Transactional)
    write(ids(0, 0, 4), queued, WriteMode.Queued)
    assert(count(txn) == 0)
    assert(count(queued) == 0)
  }

  it should "perform comparably to the existing path for a large write" taggedAs KustoE2E in {
    val rows = 3000000L
    val df = spark
      .range(0, rows, 1, 8)
      .select(
        col("id"),
        concat(lit("payload-"), col("id"), lit("-"), lit("y" * 40)).as("name"),
        (col("id") * 1.5).as("value"))
    def timed(table: String, ingestV2: Boolean): Long = {
      val start = System.nanoTime()
      write(df, table, WriteMode.Transactional, ingestV2 = ingestV2)
      (System.nanoTime() - start) / 1000000
    }
    val existingTable = newTable("perf_existing")
    val v2Table = newTable("perf_v2")
    val existingMs = timed(existingTable, ingestV2 = false)
    val v2Ms = timed(v2Table, ingestV2 = true)
    info(s"$rows rows: existing path ${existingMs}ms, ingest-v2 ${v2Ms}ms")
    assert(count(v2Table) == rows)
    assert(count(existingTable) == rows)
    assert(v2Ms <= existingMs * 2 + 60000, s"ingest-v2 took ${v2Ms}ms vs ${existingMs}ms")
  }

  // ---------------------------------------------------------------------------------------------
  // Ingestion properties and schema handling
  // ---------------------------------------------------------------------------------------------

  it should "apply ingest-by and additional tags" taggedAs KustoE2E in {
    val table = newTable("tags")
    val tag = s"batch-${UUID.randomUUID()}"
    write(
      ids(0, 1000, 4),
      table,
      WriteMode.Transactional,
      extra = ingestionProperties { p =>
        p.ingestByTags = List(tag).asJava
        p.additionalTags = List("extra-tag").asJava
      })
    assert(count(table) == 1000)
    val tags = mgmtStrings(s".show table ['$table'] extents | project tostring(Tags)").mkString(" ")
    assert(tags.contains(s"ingest-by:$tag"), tags)
    assert(tags.contains("extra-tag"), tags)
  }

  it should "keep the requested creation time" taggedAs KustoE2E in {
    val table = newTable("creation_time")
    // Within the database retention, so the extents are not dropped right away.
    val created = Instant.now().minus(java.time.Duration.ofDays(30))
    write(
      ids(0, 1000, 4),
      table,
      WriteMode.Transactional,
      extra = ingestionProperties(_.creationTime = created))
    val days = mgmtStrings(
      s".show table ['$table'] extents | project format_datetime(MinCreatedOn, 'yyyy-MM-dd')")
    assert(days.nonEmpty && days.forall(_ == created.toString.take(10)), days)
  }

  it should "map columns by name with GenerateDynamicCsvMapping" taggedAs KustoE2E in {
    val table = newTable("dynamic_mapping")
    mgmt(s".create table ['$table'] (name:string, other:string, id:long)")
    write(
      ids(0, 1000, 4),
      table,
      WriteMode.Transactional,
      extra = Map(
        KustoSinkOptions.KUSTO_ADJUST_SCHEMA -> SchemaAdjustmentMode.GenerateDynamicCsvMapping.toString))
    assert(queryLong(s"['$table'] | where name == strcat('value-', id) and isempty(other) | count") == 1000)
  }

  it should "use a named CSV mapping" taggedAs KustoE2E in {
    val table = newTable("named_mapping")
    mgmt(s".create table ['$table'] (name:string, id:long)")
    mgmt(
      s""".create table ['$table'] ingestion csv mapping "swap" '[{"column":"id","Properties":{"Ordinal":"0"}},{"column":"name","Properties":{"Ordinal":"1"}}]'""")
    write(
      ids(0, 1000, 4),
      table,
      WriteMode.Queued,
      extra = ingestionProperties(_.csvMappingNameReference = "swap"))
    waitForCount(table, 1000)
    assert(queryLong(s"['$table'] | where name == strcat('value-', id) | count") == 1000)
  }

  it should "use a user-provided staging table" taggedAs KustoE2E in {
    val table = newTable("user_staging_target")
    val staging = newTable("user_staging")
    mgmt(s".create table ['$table'] (id:long, name:string)")
    mgmt(s".create table ['$staging'] (id:long, name:string)")
    write(
      ids(0, 1000, 4),
      table,
      WriteMode.Transactional,
      extra = Map(
        KustoSinkOptions.KUSTO_TEMP_TABLE_NAME -> staging,
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS -> SinkTableCreationMode.FailIfNotExist.toString))
    assertIds(table, 0, 1000)
  }

  // ---------------------------------------------------------------------------------------------
  // Modes and switching between paths
  // ---------------------------------------------------------------------------------------------

  it should "finish an asynchronous write" taggedAs KustoE2E in {
    val table = newTable("async")
    write(
      ids(0, 5000, 4),
      table,
      WriteMode.Transactional,
      extra = Map(KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC -> "true"))
    assertIds(table, 0, 5000)
  }

  it should "write CSV when parquet is requested but the service gives storage containers" taggedAs KustoE2E in {
    val table = newTable("parquet_requested")
    write(
      ids(0, 1000, 4),
      table,
      WriteMode.Transactional,
      extra = Map(KustoSinkOptions.KUSTO_INGESTION_FORMAT -> "parquet"))
    assertIds(table, 0, 1000)
  }

  it should "leave streaming writes on the existing path" taggedAs KustoE2E in {
    val table = newTable("streaming")
    write(ids(0, 500, 2), table, WriteMode.KustoStreaming)
    assertIds(table, 0, 500)
  }

  it should "fall back to the existing path when ingest-v2 cannot be used" taggedAs KustoE2E in {
    val table = newTable("fallback")
    IngestV2TestHooks.failInsteadOfFallback = false
    IngestV2TestHooks.configOverride = Some(
      _.copy(preferredIngestionMethod = Some("Rest"), blobPaths = Nil, oneLakePaths = Nil))
    try write(ids(0, 1000, 4), table, WriteMode.Transactional)
    finally forceIngestV2()
    assertIds(table, 0, 1000)
  }
}

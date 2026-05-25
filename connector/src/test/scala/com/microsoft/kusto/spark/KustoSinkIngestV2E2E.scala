// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.KustoTestUtils.getSystemTestOptions
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SinkTableCreationMode, WriteMode}
import com.microsoft.kusto.spark.datasource.{KustoSourceOptions, ReadMode}
import com.microsoft.kusto.spark.sql.extension.SparkExtension.DataFrameReaderExtension
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.sql._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable

/**
 * End-to-end tests for the kusto-ingest-v2 SDK integration. These tests mirror KustoSinkBatchE2E
 * but use the `useIngestV2 = true` configuration to exercise the v2 write path.
 *
 * Run with: mvn test -pl connector -Dtest=KustoSinkIngestV2E2E -DKustoE2E Requires environment
 * variables: kustoCluster, kustoDatabase, storageAccountUrl
 */
class KustoSinkIngestV2E2E extends AnyFlatSpec with BeforeAndAfterAll {
  private val className = this.getClass.getSimpleName
  private val rowId = new AtomicInteger(1)
  private val timeoutMs: Int = 8 * 60 * 1000
  private def newRow(): String = s"row-${rowId.getAndIncrement()}"

  private val nofExecutors = 4
  private val spark: SparkSession = SparkSession
    .builder()
    .appName("KustoSinkIngestV2E2E")
    .master(f"local[$nofExecutors]")
    .getOrCreate()

  private lazy val kustoTestConnectionOptions = getSystemTestOptions

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }

  // =====================================================================
  // V2 Queued Ingestion (CSV format)
  // =====================================================================

  "IngestV2 Queued CSV" should "ingest data to Kusto using ingest-v2 SDK queued path" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 100
    val prefix = "IngestV2_Queued_CSV"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val df = rows.toDF("name", "value")

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Queued.toString)
      .option(KustoSinkOptions.KUSTO_USE_INGEST_V2, "true")
      .mode(SaveMode.Append)
      .save()

    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      cleanupAllTables = true,
      tableCleanupPrefix = prefix)
  }

  // =====================================================================
  // V2 Transactional Ingestion (CSV format)
  // =====================================================================

  "IngestV2 Transactional CSV" should "ingest data atomically using ingest-v2 SDK" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 100
    val prefix = "IngestV2_Txn_CSV"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val df = rows.toDF("name", "value")

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .option(KustoSinkOptions.KUSTO_USE_INGEST_V2, "true")
      .mode(SaveMode.Append)
      .save()

    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      cleanupAllTables = true,
      tableCleanupPrefix = prefix)
  }

  // =====================================================================
  // V2 Streaming Ingestion (CSV format)
  // =====================================================================

  "IngestV2 Streaming CSV" should "ingest data via managed streaming using ingest-v2 SDK" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 50
    val prefix = "IngestV2_Stream_CSV"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val df = rows.toDF("name", "value")

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.KustoStreaming.toString)
      .option(KustoSinkOptions.KUSTO_USE_INGEST_V2, "true")
      .mode(SaveMode.Append)
      .save()

    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      cleanupAllTables = true,
      tableCleanupPrefix = prefix)
  }

  // =====================================================================
  // V2 Queued Ingestion (Parquet format)
  // =====================================================================

  "IngestV2 Queued Parquet" should "ingest data as Parquet using Spark native writer" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 100
    val prefix = "IngestV2_Queued_Parquet"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val df = rows.toDF("name", "value")

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Queued.toString)
      .option(KustoSinkOptions.KUSTO_USE_INGEST_V2, "true")
      .option(KustoSinkOptions.KUSTO_INGESTION_FORMAT, "parquet")
      .mode(SaveMode.Append)
      .save()

    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      cleanupAllTables = true,
      tableCleanupPrefix = prefix)
  }

  // =====================================================================
  // V2 Transactional Ingestion (Parquet format)
  // =====================================================================

  "IngestV2 Transactional Parquet" should "ingest data atomically as Parquet" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 100
    val prefix = "IngestV2_Txn_Parquet"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val df = rows.toDF("name", "value")

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .option(KustoSinkOptions.KUSTO_USE_INGEST_V2, "true")
      .option(KustoSinkOptions.KUSTO_INGESTION_FORMAT, "parquet")
      .mode(SaveMode.Append)
      .save()

    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      cleanupAllTables = true,
      tableCleanupPrefix = prefix)
  }

  // =====================================================================
  // V2 Data Types Test (Parquet)
  // =====================================================================

  "IngestV2 Parquet DataTypes" should "ingest all Spark data types as Parquet" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 50
    val prefix = "IngestV2_Parquet_Types"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")

    val dataRows: immutable.IndexedSeq[(String, Int, Boolean, Double, Long)] =
      (1 to expectedNumberOfRows).map(v =>
        (s"row-$v", v, v % 2 == 0, 1.0 / v, v.toLong * 100000L))
    val df = dataRows.toDF("StringCol", "IntCol", "BoolCol", "DoubleCol", "LongCol")

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Queued.toString)
      .option(KustoSinkOptions.KUSTO_USE_INGEST_V2, "true")
      .option(KustoSinkOptions.KUSTO_INGESTION_FORMAT, "parquet")
      .mode(SaveMode.Append)
      .save()

    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      cleanupAllTables = true,
      tableCleanupPrefix = prefix)
  }

  // =====================================================================
  // V2 Read-back validation (write Parquet, read back and compare)
  // =====================================================================

  "IngestV2 Parquet roundtrip" should "write and read back data correctly" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 50
    val prefix = "IngestV2_Parquet_RT"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")

    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (s"roundtrip-$v", v))
    val df = rows.toDF("name", "value")

    // Write using ingest-v2 Parquet path
    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .option(KustoSinkOptions.KUSTO_USE_INGEST_V2, "true")
      .option(KustoSinkOptions.KUSTO_INGESTION_FORMAT, "parquet")
      .mode(SaveMode.Append)
      .save()

    // Wait for ingestion and then read back
    Thread.sleep(30000) // Give ingestion time to complete

    val readConf: Map[String, String] = Map(
      KustoSinkOptions.KUSTO_ACCESS_TOKEN -> kustoTestConnectionOptions.accessToken,
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceSingleMode.toString)

    val result = spark.read.kusto(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.database,
      table,
      readConf)

    assert(
      result.count() == expectedNumberOfRows,
      s"Expected $expectedNumberOfRows rows, got ${result.count()}")

    // Cleanup
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    KustoTestUtils.tryDropAllTablesByPrefix(
      kustoAdminClient,
      kustoTestConnectionOptions.database,
      prefix)
  }
}

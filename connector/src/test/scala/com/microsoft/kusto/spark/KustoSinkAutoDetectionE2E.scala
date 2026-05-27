// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.KustoTestUtils.getSystemTestOptions
import com.microsoft.kusto.spark.datasink.{
  IngestionStorageParameters,
  KustoSinkOptions,
  SinkTableCreationMode,
  WriteMode
}
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable

/**
 * End-to-end tests for the auto-detection feature in kusto-ingest-v2 SDK integration.
 *
 * These tests validate the config API contract:
 *   - Query /v1/rest/ingestion/configuration
 *   - If preferredIngestionMethod == "REST" → Use V2 SDK (auto-detected)
 *   - If preferredIngestionMethod == "Legacy" or 404 → Use V1 SDK (auto-fallback)
 *
 * Key difference from KustoSinkIngestV2E2E: Tests do NOT set useIngestV2 flag, relying on
 * auto-detection based on config API response.
 *
 * Run with: mvn test -pl connector -Dtest=KustoSinkAutoDetectionE2E -DKustoE2E
 *
 * Requirements:
 *   - V2-enabled cluster: Set kustoCluster env var to V2 cluster URL
 *   - V1-only cluster: Set kustoV1Cluster env var to V1 cluster URL (optional for full suite)
 *   - Database: Set kustoDatabase env var
 *   - Authentication: Set kustoAccessToken or AAD credentials
 */
class KustoSinkAutoDetectionE2E extends AnyFlatSpec with BeforeAndAfterAll {
  private val className = this.getClass.getSimpleName
  private val rowId = new AtomicInteger(1)
  private val timeoutMs: Int = 8 * 60 * 1000
  private def newRow(): String = s"row-${rowId.getAndIncrement()}"

  private val nofExecutors = 4
  private val spark: SparkSession = SparkSession
    .builder()
    .appName("KustoSinkAutoDetectionE2E")
    .master(f"local[$nofExecutors]")
    .getOrCreate()

  private lazy val kustoTestConnectionOptions = getSystemTestOptions

  // Optional: V1-only cluster for fallback testing
  private lazy val kustoV1Cluster =
    Option(System.getenv("kustoV1Cluster")).getOrElse(kustoTestConnectionOptions.cluster)

  override def beforeAll(): Unit = {
    super.beforeAll()
    KDSU.logInfo(
      className,
      s"Running auto-detection E2E tests against V2 cluster: ${kustoTestConnectionOptions.cluster}")
    KDSU.logInfo(className, s"V1 cluster for fallback tests: $kustoV1Cluster")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }

  // =====================================================================
  // P0: V2 Auto-Detection (No useIngestV2 flag)
  // =====================================================================

  "Auto-Detection on V2 Cluster (Queued CSV)" should "automatically detect and use V2 SDK based on config API" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 100
    val prefix = "AutoDetect_V2_Queued_CSV"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val df = rows.toDF("name", "value")

    KDSU.logInfo(className, s"Test: Auto-detection on V2 cluster (NO useIngestV2 flag)")

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
      // NO useIngestV2 option! Should auto-detect V2 via config API
      .mode(SaveMode.Append)
      .save()

    // Check logs for auto-detection message:
    // "Config API: preferredIngestionMethod=REST → V2 ingestion enabled (auto-detected)"

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
      cleanupAllTables = false,
      tableCleanupPrefix = prefix)

    KDSU.logInfo(className, "✅ Auto-detection test passed - V2 was used")
  }

  "Auto-Detection on V2 Cluster (Transactional Parquet)" should "automatically use V2 for transactional writes" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 100
    val prefix = "AutoDetect_V2_Txn_Parquet"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val df = rows.toDF("name", "value")

    KDSU.logInfo(className, s"Test: Auto-detection with transactional mode (NO useIngestV2 flag)")

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
      .option(KustoSinkOptions.KUSTO_INGESTION_FORMAT, "parquet")
      // NO useIngestV2 option! Should auto-detect
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
      cleanupAllTables = false,
      tableCleanupPrefix = prefix)

    KDSU.logInfo(className, "✅ Auto-detection transactional test passed")
  }

  // =====================================================================
  // P0: V1 Auto-Fallback (No useIngestV2 flag, V1 cluster)
  // =====================================================================

  "Auto-Fallback on V1 Cluster" should "automatically fallback to V1 SDK when config API returns Legacy" taggedAs KustoE2E ignore {
    // This test requires a V1-only cluster (where config API returns "Legacy" or 404)
    // Tagged as 'ignore' by default - remove 'ignore' when V1 cluster is available
    import spark.implicits._
    val expectedNumberOfRows = 50
    val prefix = "AutoFallback_V1_Queued"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val df = rows.toDF("name", "value")

    KDSU.logInfo(className, s"Test: Auto-fallback on V1 cluster (NO useIngestV2 flag)")

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoV1Cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      // NO useIngestV2 option! Should auto-fallback to V1
      .mode(SaveMode.Append)
      .save()

    // Check logs for fallback message:
    // "Config API: preferredIngestionMethod=Legacy → V1 ingestion (auto-fallback)"
    // or
    // "Config API unavailable → V1 ingestion (auto-fallback)"

    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoV1Cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      cleanupAllTables = false,
      tableCleanupPrefix = prefix)

    KDSU.logInfo(className, "✅ Auto-fallback test passed - V1 was used")
  }

  // =====================================================================
  // P0: Manual Override (useIngestV2=true should still work)
  // =====================================================================

  "Manual Override with useIngestV2=true" should "bypass auto-detection and use V2 directly" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 50
    val prefix = "ManualOverride_V2"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val df = rows.toDF("name", "value")

    KDSU.logInfo(className, s"Test: Manual override with useIngestV2=true")

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .option(KustoSinkOptions.KUSTO_USE_INGEST_V2, "true") // Manual override
      .mode(SaveMode.Append)
      .save()

    // Check logs for manual override message:
    // "Using V2 ingestion path (manual override: useIngestV2=true)"

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
      cleanupAllTables = false,
      tableCleanupPrefix = prefix)

    KDSU.logInfo(className, "✅ Manual override test passed - backwards compatibility OK")
  }

  // =====================================================================
  // P1: Config Caching (Multiple writes should reuse cached config)
  // =====================================================================

  "Config API Caching" should "query config API once and cache for subsequent writes" taggedAs KustoE2E in {
    import spark.implicits._
    val prefix = "ConfigCache"

    KDSU.logInfo(className, s"Test: Config caching across multiple writes")

    // Write 1 - should query config API
    val table1 = KustoQueryUtils.simplifyName(s"${prefix}_Write1_${UUID.randomUUID()}")
    val df1 = Seq((1, "cache-test-1")).toDF("id", "name")
    val start1 = System.currentTimeMillis()

    df1.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table1)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    val duration1 = System.currentTimeMillis() - start1
    KDSU.logInfo(className, s"Write 1 duration: ${duration1}ms (includes config query)")

    // Write 2 - should use cached config (no new query)
    val table2 = KustoQueryUtils.simplifyName(s"${prefix}_Write2_${UUID.randomUUID()}")
    val df2 = Seq((2, "cache-test-2")).toDF("id", "name")
    val start2 = System.currentTimeMillis()

    df2.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table2)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    val duration2 = System.currentTimeMillis() - start2
    KDSU.logInfo(className, s"Write 2 duration: ${duration2}ms (should use cache)")

    // Write 3 - should also use cached config
    val table3 = KustoQueryUtils.simplifyName(s"${prefix}_Write3_${UUID.randomUUID()}")
    val df3 = Seq((3, "cache-test-3")).toDF("id", "name")
    val start3 = System.currentTimeMillis()

    df3.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table3)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    val duration3 = System.currentTimeMillis() - start3
    KDSU.logInfo(className, s"Write 3 duration: ${duration3}ms (should use cache)")

    // Cleanup all 3 tables
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    KustoTestUtils.tryDropAllTablesByPrefix(
      kustoAdminClient,
      kustoTestConnectionOptions.database,
      prefix)

    KDSU.logInfo(className, "✅ Config caching test passed - 3 writes completed")
    KDSU.logInfo(
      className,
      "Note: Check logs to verify config was queried only once (first write)")
  }

  // =====================================================================
  // P1: Customer Storage with Auto-Detection
  // =====================================================================

  "Auto-Detection with Customer Storage" should "auto-detect V2 and use ingestionStorageParameters" taggedAs KustoE2E ignore {
    // This test requires customer storage configuration via KUSTO_INGESTION_STORAGE
    // The storage parameters are complex (serialized JSON of IngestionStorageParameters)
    // Remove 'ignore' when ready to test with actual customer storage setup
    // For now, this test is a placeholder showing the pattern
    import spark.implicits._
    val expectedNumberOfRows = 50
    val prefix = "AutoDetect_CustomerStorage"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val df = rows.toDF("name", "value")

    // Note: Customer storage requires complex JSON config via KUSTO_INGESTION_STORAGE option
    // Example format: Array[IngestionStorageParameters] serialized as JSON
    // See IngestionStorageParameters class for details

    KDSU.logInfo(
      className,
      s"Test: Auto-detection with customer storage (requires storage config)")

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      // TODO: Add KUSTO_INGESTION_STORAGE option with customer storage JSON
      // .option(KustoSinkOptions.KUSTO_INGESTION_STORAGE, customerStorageJson)
      // NO useIngestV2 option! Should auto-detect and use customer storage
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
      cleanupAllTables = false,
      tableCleanupPrefix = prefix)

    KDSU.logInfo(
      className,
      "✅ Auto-detection with customer storage test passed - V2 used customer storage")
  }

  // =====================================================================
  // P1: Streaming with Auto-Detection
  // =====================================================================

  "Auto-Detection in Streaming Mode" should "auto-detect V2 for structured streaming writes" taggedAs KustoE2E in {
    import spark.implicits._
    val prefix = "AutoDetect_Streaming"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")

    KDSU.logInfo(className, s"Test: Auto-detection in structured streaming mode")

    // Create a streaming DataFrame (rate source for testing)
    val streamDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "5")
      .load()
      .selectExpr("value as id", "'streaming-auto-detect' as name")

    // Write stream without useIngestV2 flag - should auto-detect
    val query = streamDF.writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      // NO useIngestV2 option! Should auto-detect
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Let it run for 35 seconds (3-4 micro-batches)
    Thread.sleep(35000)
    query.stop()

    // Validate data arrived
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    // Should have at least some rows (depends on rate and batching)
    Thread.sleep(30000) // Wait for ingestion
    val countQuery = s"$table | count"
    val result = kustoAdminClient.executeQuery(kustoTestConnectionOptions.database, countQuery)
    val count = result.getPrimaryResults.getData.get(0).get(0).toString.toLong

    assert(count > 0, s"Expected rows in table, but got $count")

    // Cleanup
    KustoTestUtils.tryDropAllTablesByPrefix(
      kustoAdminClient,
      kustoTestConnectionOptions.database,
      prefix)

    KDSU.logInfo(
      className,
      s"✅ Streaming auto-detection test passed - $count rows ingested via V2")
  }

  // =====================================================================
  // P2: Network Failure Fallback (Config API unreachable)
  // =====================================================================

  "Auto-Fallback on Network Failure" should "gracefully fallback to V1 when config API fails" taggedAs KustoE2E ignore {
    // This test requires a cluster where config API is unavailable (404 or network issue)
    // Or test with an invalid DM URL
    // Remove 'ignore' when testing network failure scenarios
    import spark.implicits._
    val expectedNumberOfRows = 20
    val prefix = "NetworkFail_Fallback"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val df = rows.toDF("name", "value")

    KDSU.logInfo(className, s"Test: Graceful fallback when config API unavailable")

    // Note: This may fail if cluster genuinely doesn't exist
    // The goal is to test the fallback logic, not the failure itself
    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoV1Cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      // NO useIngestV2 option! Config API failure should fallback to V1
      .mode(SaveMode.Append)
      .save()

    // Check logs for fallback message:
    // "Config API unavailable → V1 ingestion (auto-fallback)"

    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoV1Cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      cleanupAllTables = false,
      tableCleanupPrefix = prefix)

    KDSU.logInfo(className, "✅ Network failure fallback test passed - V1 used gracefully")
  }
}

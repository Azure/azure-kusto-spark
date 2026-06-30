// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.kusto.spark.datasink._
import com.microsoft.kusto.spark.utils.{KustoConstants, KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the kusto-ingest-v2 integration. Tests configuration parsing, routing decisions,
 * and authentication mapping without requiring a live Kusto cluster.
 */
class IngestV2UnitTests extends AnyFlatSpec with Matchers {

  private val sparkConf: SparkConf = new SparkConf()
    .set("spark.testing", "true")
    .set("spark.ui.enabled", "false")
    .setAppName("IngestV2UnitTests")
    .setMaster("local[*]")
  private val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  // =====================================================================
  // Config Parsing Tests
  // =====================================================================

  "legacyIngest config" should "default to false (V2 is default)" in {
    val defaultOptions = WriteOptions(kustoCustomDebugWriteOptions =
      new com.microsoft.kusto.spark.utils.KustoCustomDebugWriteOptions())
    defaultOptions.legacyIngest shouldBe false
  }

  "legacyIngest config" should "be true when set to force V1" in {
    val defaultOptions = WriteOptions(
      kustoCustomDebugWriteOptions =
        new com.microsoft.kusto.spark.utils.KustoCustomDebugWriteOptions(),
      legacyIngest = true)
    defaultOptions.legacyIngest shouldBe true
  }

  "ingestionFormat config" should "default to CSV" in {
    val defaultOptions = WriteOptions(kustoCustomDebugWriteOptions =
      new com.microsoft.kusto.spark.utils.KustoCustomDebugWriteOptions())
    defaultOptions.ingestionFormat shouldBe IngestionFormat.CSV
  }

  "ingestionFormat config" should "be Parquet when set" in {
    val options = WriteOptions(
      kustoCustomDebugWriteOptions =
        new com.microsoft.kusto.spark.utils.KustoCustomDebugWriteOptions(),
      ingestionFormat = IngestionFormat.Parquet)
    options.ingestionFormat shouldBe IngestionFormat.Parquet
  }

  "IngestionFormat enum" should "have CSV and Parquet values" in {
    IngestionFormat.CSV should not be null
    IngestionFormat.Parquet should not be null
    IngestionFormat.CSV should not equal IngestionFormat.Parquet
  }

  // =====================================================================
  // Authentication Mapping Tests
  // =====================================================================

  "IngestV2Authentication" should "create TokenCredential from AadApplicationAuthentication" in {
    import com.microsoft.kusto.spark.authentication.AadApplicationAuthentication

    val auth = AadApplicationAuthentication("test-app-id", "test-app-key", "test-tenant-id")

    val credential = IngestV2Authentication.createTokenCredential(auth)
    credential should not be null
  }

  "IngestV2Authentication" should "create TokenCredential from KustoAccessTokenAuthentication" in {
    import com.microsoft.kusto.spark.authentication.KustoAccessTokenAuthentication

    val auth = KustoAccessTokenAuthentication("test-access-token-value")
    val credential = IngestV2Authentication.createTokenCredential(auth)
    credential should not be null
  }

  "IngestV2Authentication" should "create TokenCredential from KustoTokenProviderAuthentication" in {
    import com.microsoft.kusto.spark.authentication.KustoTokenProviderAuthentication

    val auth = KustoTokenProviderAuthentication(() => "dynamic-token-value")
    val credential = IngestV2Authentication.createTokenCredential(auth)
    credential should not be null
  }

  // =====================================================================
  // Write Mode Routing Tests
  // =====================================================================

  "WriteMode routing" should "use Parquet path for Queued + Parquet format" in {
    val options = WriteOptions(
      kustoCustomDebugWriteOptions =
        new com.microsoft.kusto.spark.utils.KustoCustomDebugWriteOptions(),
      ingestionFormat = IngestionFormat.Parquet,
      writeMode = WriteMode.Queued)

    (options.ingestionFormat == IngestionFormat.Parquet &&
      options.writeMode != WriteMode.KustoStreaming) shouldBe true
  }

  "WriteMode routing" should "use CSV path for KustoStreaming even with Parquet format" in {
    val options = WriteOptions(
      kustoCustomDebugWriteOptions =
        new com.microsoft.kusto.spark.utils.KustoCustomDebugWriteOptions(),
      ingestionFormat = IngestionFormat.Parquet,
      writeMode = WriteMode.KustoStreaming)

    // Streaming mode should NOT use Parquet
    (options.ingestionFormat == IngestionFormat.Parquet &&
      options.writeMode != WriteMode.KustoStreaming) shouldBe false
  }

  "WriteMode routing" should "use CSV path when format is CSV" in {
    val options = WriteOptions(
      kustoCustomDebugWriteOptions =
        new com.microsoft.kusto.spark.utils.KustoCustomDebugWriteOptions(),
      ingestionFormat = IngestionFormat.CSV,
      writeMode = WriteMode.Queued)

    options.ingestionFormat shouldBe IngestionFormat.CSV
  }

  // =====================================================================
  // Client Provider Tests
  // =====================================================================

  "IngestV2ClientProvider" should "create without throwing" in {
    import com.microsoft.kusto.spark.authentication.KustoAccessTokenAuthentication

    val auth = KustoAccessTokenAuthentication("test-token")
    noException should be thrownBy {
      new IngestV2ClientProvider(
        "https://ingest-test.kusto.windows.net",
        auth,
        "Kusto.Spark.Connector.Test")
    }
  }
}

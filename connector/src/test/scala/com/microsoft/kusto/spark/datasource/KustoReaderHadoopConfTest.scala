// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import com.microsoft.kusto.spark.utils.{KustoConstants => KCONST}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Tests for the setHadoopConf dual-write behaviour: every Hadoop configuration key should
 * be written to both the Hadoop Configuration object AND the Spark RuntimeConfig (with the
 * spark.hadoop. prefix) so that engines like Gluten/Velox that read from Spark session conf
 * also pick them up.
 */
class KustoReaderHadoopConfTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  private val sparkHadoopPrefix = "spark.hadoop."
  private var spark: SparkSession = _
  private var sparkConf: RuntimeConfig = _
  private var hadoopConfig: Configuration = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .appName("KustoReaderHadoopConfTest")
      .master("local[1]")
      .getOrCreate()
    sparkConf = spark.conf
    hadoopConfig = new Configuration()
  }

  // ---------------------------------------------------------------------------
  // WASBS + SAS  —  dual-write to both HadoopConf and SparkConf
  // ---------------------------------------------------------------------------
  "setHadoopAuth (WASBS + SAS)" should "write SAS token to both HadoopConf and SparkConf" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams("wsbsas1", "container1", "sv=2021-01-01&sig=test1")

    KustoReader.setHadoopAuth(
      storageParams, KCONST.storageProtocolWasbs, hadoopConfig, sparkConf, now, useAbfs = false)

    val expectedKey = "fs.azure.sas.container1.wsbsas1.blob.core.windows.net"
    hadoopConfig.get(expectedKey) should not be null
    hadoopConfig.get(expectedKey) should include("sv=2021-01-01")
    sparkConf.getOption(s"$sparkHadoopPrefix$expectedKey") shouldBe defined
    sparkConf.get(s"$sparkHadoopPrefix$expectedKey") should include("sv=2021-01-01")
  }

  it should "strip leading '?' from WASBS SAS token" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams("wsbstrip", "cstrip", "sv=2022-01-01&sig=stripped")

    KustoReader.setHadoopAuth(
      storageParams, KCONST.storageProtocolWasbs, hadoopConfig, sparkConf, now, useAbfs = false)

    val expectedKey = "fs.azure.sas.cstrip.wsbstrip.blob.core.windows.net"
    hadoopConfig.get(expectedKey) should startWith("sv=")
    hadoopConfig.get(expectedKey) should not startWith "?"
    sparkConf.get(s"$sparkHadoopPrefix$expectedKey") should startWith("sv=")
  }

  // ---------------------------------------------------------------------------
  // WASBS + Account Key  —  dual-write
  // ---------------------------------------------------------------------------
  "setHadoopAuth (WASBS + Key)" should "write account key to both HadoopConf and SparkConf" in {
    val now = freshTimestamp()
    val storageParams = keyStorageParams("wsbkey1", "mykey123", "ckey1")

    KustoReader.setHadoopAuth(
      storageParams, KCONST.storageProtocolWasbs, hadoopConfig, sparkConf, now, useAbfs = false)

    val expectedKey = "fs.azure.account.key.wsbkey1.blob.core.windows.net"
    hadoopConfig.get(expectedKey) shouldBe "mykey123"
    sparkConf.get(s"$sparkHadoopPrefix$expectedKey") shouldBe "mykey123"
  }

  // ---------------------------------------------------------------------------
  // ABFS + SAS  —  dual-write of auth type, HNS setting, and SAS token
  // ---------------------------------------------------------------------------
  "setHadoopAuth (ABFS + SAS)" should "write auth type to both HadoopConf and SparkConf" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams("abfsas1", "cabf1", "sv=2021-01-01&sig=abftest")

    KustoReader.setHadoopAuth(
      storageParams, KCONST.storageProtocolAbfs, hadoopConfig, sparkConf, now, useAbfs = true)

    // fs.azure.account.auth.type
    hadoopConfig.get("fs.azure.account.auth.type") shouldBe "SAS"
    sparkConf.get(s"${sparkHadoopPrefix}fs.azure.account.auth.type") shouldBe "SAS"
  }

  it should "write HNS enabled setting to both HadoopConf and SparkConf" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams("abfhns1", "chns1", "sv=2021-01-01&sig=hnstest")

    KustoReader.setHadoopAuth(
      storageParams, KCONST.storageProtocolAbfs, hadoopConfig, sparkConf, now, useAbfs = true)

    val expectedKey = "fs.azure.account.hns.enabled.abfhns1.blob.core.windows.net"
    hadoopConfig.get(expectedKey) shouldBe "false"
    sparkConf.get(s"$sparkHadoopPrefix$expectedKey") shouldBe "false"
  }

  it should "write SAS fixed token to both HadoopConf and SparkConf" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams("abftok1", "ctok1", "sv=2021-01-01&sig=toktest")

    KustoReader.setHadoopAuth(
      storageParams, KCONST.storageProtocolAbfs, hadoopConfig, sparkConf, now, useAbfs = true)

    val expectedKey = "fs.azure.sas.fixed.token.ctok1.abftok1.blob.core.windows.net"
    hadoopConfig.get(expectedKey) should include("sv=2021-01-01")
    sparkConf.get(s"$sparkHadoopPrefix$expectedKey") should include("sv=2021-01-01")
  }

  // ---------------------------------------------------------------------------
  // ABFS + Account Key  —  should throw
  // ---------------------------------------------------------------------------
  "setHadoopAuth (ABFS + Key)" should "throw InvalidParameterException" in {
    val now = freshTimestamp()
    val storageParams = keyStorageParams("abfkey1", "somekey", "ckeyabf1")

    val ex = intercept[java.security.InvalidParameterException] {
      KustoReader.setHadoopAuth(
        storageParams, KCONST.storageProtocolAbfs, hadoopConfig, sparkConf, now, useAbfs = true)
    }
    ex.getMessage should include("not supported")
  }

  // ---------------------------------------------------------------------------
  // ABFS whitelist endpoint  —  dual-write
  // ---------------------------------------------------------------------------
  "setHadoopAuth (ABFS whitelist)" should "write valid endpoints to both HadoopConf and SparkConf" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams("abfwl1", "cwl1", "sv=2021-01-01&sig=wltest",
      endpointSuffix = "custom.domain.net")

    KustoReader.setHadoopAuth(
      storageParams, KCONST.storageProtocolAbfs, hadoopConfig, sparkConf, now, useAbfs = true)

    val endpointKey = "fs.azure.abfs.valid.endpoints"
    hadoopConfig.get(endpointKey) should include("custom.domain.net")
    sparkConf.get(s"$sparkHadoopPrefix$endpointKey") should include("custom.domain.net")
  }

  it should "append to existing endpoints rather than overwrite" in {
    val now = freshTimestamp()
    // Pre-populate an existing endpoint
    hadoopConfig.set("fs.azure.abfs.valid.endpoints", "existing.domain.net")

    val storageParams = sasStorageParams("abfwl2", "cwl2", "sv=2021-01-01&sig=wltest2",
      endpointSuffix = "new.domain.net")

    KustoReader.setHadoopAuth(
      storageParams, KCONST.storageProtocolAbfs, hadoopConfig, sparkConf, now, useAbfs = true)

    val endpoints = hadoopConfig.get("fs.azure.abfs.valid.endpoints")
    endpoints should include("existing.domain.net")
    endpoints should include("new.domain.net")
  }

  it should "not duplicate an already-whitelisted domain" in {
    val now = freshTimestamp()
    hadoopConfig.set("fs.azure.abfs.valid.endpoints", "same.domain.net")

    val storageParams = sasStorageParams("abfwl3", "cwl3", "sv=2021-01-01&sig=wltest3",
      endpointSuffix = "same.domain.net")

    KustoReader.setHadoopAuth(
      storageParams, KCONST.storageProtocolAbfs, hadoopConfig, sparkConf, now, useAbfs = true)

    // Should appear exactly once
    val endpoints = hadoopConfig.get("fs.azure.abfs.valid.endpoints")
    endpoints shouldBe "same.domain.net"
  }

  // ---------------------------------------------------------------------------
  // Caching behaviour  — configs should NOT be re-written when cached
  // ---------------------------------------------------------------------------
  "setHadoopAuth caching" should "not overwrite SparkConf on second call within cache window" in {
    val now = Instant.now()
    val storageParams = sasStorageParams("cache1", "cc1", "sv=2021-01-01&sig=cachetest")

    // First call — populates cache
    KustoReader.setHadoopAuth(
      storageParams, KCONST.storageProtocolWasbs, hadoopConfig, sparkConf, now, useAbfs = false)

    val expectedKey = "fs.azure.sas.cc1.cache1.blob.core.windows.net"
    val firstValue = hadoopConfig.get(expectedKey)
    firstValue should not be null

    // Mutate the hadoop config to detect if it gets overwritten
    hadoopConfig.set(expectedKey, "MUTATED")

    // Second call with same timestamp — should be cached, not overwritten
    KustoReader.setHadoopAuth(
      storageParams, KCONST.storageProtocolWasbs, hadoopConfig, sparkConf, now, useAbfs = false)

    hadoopConfig.get(expectedKey) shouldBe "MUTATED"
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Return a timestamp far enough in the past to bypass the KustoAzureFsSetupCache. */
  private def freshTimestamp(): Instant =
    Instant.now().minus(3 * KCONST.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES)

  /** Build TransientStorageParameters from a SAS URL. */
  private def sasStorageParams(
      account: String,
      container: String,
      sasToken: String,
      endpointSuffix: String = "core.windows.net"): TransientStorageParameters = {
    new TransientStorageParameters(
      Array(new TransientStorageCredentials(
        s"https://$account.blob.$endpointSuffix/$container?$sasToken")),
      endpointSuffix)
  }

  /** Build TransientStorageParameters from an account key. */
  private def keyStorageParams(
      account: String,
      key: String,
      container: String,
      endpointSuffix: String = "core.windows.net"): TransientStorageParameters = {
    new TransientStorageParameters(
      Array(new TransientStorageCredentials(account, key, container)),
      endpointSuffix)
  }
}

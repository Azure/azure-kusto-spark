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
 * Tests for the setHadoopConf dual-write behaviour: every Hadoop configuration key should be
 * written to both the Hadoop Configuration object AND the Spark RuntimeConfig (with the
 * spark.hadoop. prefix) so that engines like Gluten/Velox that read from Spark session conf also
 * pick them up.
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
    // The Spark RuntimeConfig is shared with every other suite in this JVM - make sure the
    // account agnostic key cannot leak in from elsewhere before asserting on its absence.
    sparkConf.unset(s"${sparkHadoopPrefix}fs.azure.account.auth.type")
  }

  // ---------------------------------------------------------------------------
  // WASBS + SAS  —  dual-write to both HadoopConf and SparkConf
  // ---------------------------------------------------------------------------
  "setHadoopAuth (WASBS + SAS)" should "write SAS token to both HadoopConf and SparkConf" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams("wsbsas1", "container1", "sv=2021-01-01&sig=test1")

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolWasbs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = false)

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
      storageParams,
      KCONST.storageProtocolWasbs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = false)

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
      storageParams,
      KCONST.storageProtocolWasbs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = false)

    val expectedKey = "fs.azure.account.key.wsbkey1.blob.core.windows.net"
    hadoopConfig.get(expectedKey) shouldBe "mykey123"
    sparkConf.get(s"$sparkHadoopPrefix$expectedKey") shouldBe "mykey123"
  }

  // ---------------------------------------------------------------------------
  // ABFS + SAS  —  dual-write of auth type, HNS setting, and SAS token
  // ---------------------------------------------------------------------------
  "setHadoopAuth (ABFS + SAS)" should "scope auth type to the storage account only" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams("abfsas1", "cabf1", "sv=2021-01-01&sig=abftest")

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolAbfs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = true)

    val accountScopedKey = "fs.azure.account.auth.type.abfsas1.blob.core.windows.net"
    hadoopConfig.get(accountScopedKey) shouldBe "SAS"
    sparkConf.get(s"$sparkHadoopPrefix$accountScopedKey") shouldBe "SAS"

    // The account agnostic key must never be written - it would force every other ABFS
    // account in the session (OneLake, lakehouse, customer storage) into SAS auth.
    hadoopConfig.get("fs.azure.account.auth.type") shouldBe null
    sparkConf.getOption(s"${sparkHadoopPrefix}fs.azure.account.auth.type") shouldBe empty
  }

  it should "write HNS enabled setting to both HadoopConf and SparkConf" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams("abfhns1", "chns1", "sv=2021-01-01&sig=hnstest")

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolAbfs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = true)

    val expectedKey = "fs.azure.account.hns.enabled.abfhns1.blob.core.windows.net"
    hadoopConfig.get(expectedKey) shouldBe "false"
    sparkConf.get(s"$sparkHadoopPrefix$expectedKey") shouldBe "false"
  }

  it should "write SAS fixed token under the account scoped key" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams("abftok1", "ctok1", "sv=2021-01-01&sig=toktest")

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolAbfs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = true)

    // Hadoop's AbfsConfiguration and the Gluten/Velox reader both resolve '<key>.<account>'.
    val expectedKey = "fs.azure.sas.fixed.token.abftok1.blob.core.windows.net"
    hadoopConfig.get(expectedKey) should include("sv=2021-01-01")
    sparkConf.get(s"$sparkHadoopPrefix$expectedKey") should include("sv=2021-01-01")

    // The container qualified key is kept for backwards compatibility.
    val legacyKey = "fs.azure.sas.fixed.token.ctok1.abftok1.blob.core.windows.net"
    hadoopConfig.get(legacyKey) should include("sv=2021-01-01")
  }

  it should "strip a leading '?' from the ABFS SAS token" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams("abfqm1", "cqm1", "sv=2023-01-01&sig=questionmark")

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolAbfss,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = true)

    val expectedKey = "fs.azure.sas.fixed.token.abfqm1.blob.core.windows.net"
    hadoopConfig.get(expectedKey) should startWith("sv=")
    hadoopConfig.get(expectedKey) should not startWith "?"
    sparkConf.get(s"$sparkHadoopPrefix$expectedKey") should not startWith "?"
  }

  it should "reinstall the account scoped token even when the same container was seen before" in {
    val now = Instant.now()
    val storageParams = sasStorageParams("nocache1", "cnc1", "sv=2021-01-01&sig=nocache")
    val expectedKey = "fs.azure.sas.fixed.token.nocache1.blob.core.windows.net"

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolAbfss,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = true)
    hadoopConfig.get(expectedKey) should include("sig=nocache")

    // A brand new Configuration (e.g. a second Spark session) must still be populated:
    // the ABFS keys are account scoped so cache-based write suppression is not safe.
    val freshConfig = new Configuration()
    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolAbfss,
      freshConfig,
      sparkConf,
      now,
      useAbfs = true)
    freshConfig.get(expectedKey) should include("sig=nocache")
    freshConfig.get("fs.azure.account.auth.type.nocache1.blob.core.windows.net") shouldBe "SAS"
  }

  it should "install the token of the container being read when sibling containers share an account" in {
    val now = Instant.now()
    val first = sasStorageParams("shared1", "cfirst", "sv=2021-01-01&sig=first")
    val second = sasStorageParams("shared1", "csecond", "sv=2021-01-01&sig=second")
    val accountKey = "fs.azure.sas.fixed.token.shared1.blob.core.windows.net"

    KustoReader.setHadoopAuth(
      first,
      KCONST.storageProtocolAbfss,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = true)
    hadoopConfig.get(accountKey) should include("sig=first")

    KustoReader.setHadoopAuth(
      second,
      KCONST.storageProtocolAbfss,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = true)
    hadoopConfig.get(accountKey) should include("sig=second")

    // Re-selecting the first container must reinstall its own token rather than being
    // considered cached because of the per-container cache entry.
    KustoReader.setHadoopAuth(
      first,
      KCONST.storageProtocolAbfss,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = true)
    hadoopConfig.get(accountKey) should include("sig=first")
  }

  // ---------------------------------------------------------------------------
  // ABFS + Account Key  —  should throw
  // ---------------------------------------------------------------------------
  "setHadoopAuth (ABFS + Key)" should "throw InvalidParameterException" in {
    val now = freshTimestamp()
    val storageParams = keyStorageParams("abfkey1", "somekey", "ckeyabf1")

    val ex = intercept[java.security.InvalidParameterException] {
      KustoReader.setHadoopAuth(
        storageParams,
        KCONST.storageProtocolAbfs,
        hadoopConfig,
        sparkConf,
        now,
        useAbfs = true)
    }
    ex.getMessage should include("not supported")
  }

  it should "throw even when the same account was already configured for WASBS" in {
    val now = Instant.now()
    val storageParams = keyStorageParams("abfkey2", "samekey", "ckeyabf2")

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolWasbs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = false)

    val ex = intercept[java.security.InvalidParameterException] {
      KustoReader.setHadoopAuth(
        storageParams,
        KCONST.storageProtocolAbfss,
        hadoopConfig,
        sparkConf,
        now,
        useAbfs = true)
    }
    ex.getMessage should include("not supported")
  }

  // ---------------------------------------------------------------------------
  // ABFS whitelist endpoint  —  dual-write
  // ---------------------------------------------------------------------------
  "setHadoopAuth (ABFS whitelist)" should "write valid endpoints to both HadoopConf and SparkConf" in {
    val now = freshTimestamp()
    val storageParams = sasStorageParams(
      "abfwl1",
      "cwl1",
      "sv=2021-01-01&sig=wltest",
      endpointSuffix = "custom.domain.net")

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolAbfs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = true)

    val endpointKey = "fs.azure.abfs.valid.endpoints"
    hadoopConfig.get(endpointKey) should include("custom.domain.net")
    sparkConf.get(s"$sparkHadoopPrefix$endpointKey") should include("custom.domain.net")
  }

  it should "append to existing endpoints rather than overwrite" in {
    val now = freshTimestamp()
    // Pre-populate an existing endpoint
    hadoopConfig.set("fs.azure.abfs.valid.endpoints", "existing.domain.net")

    val storageParams = sasStorageParams(
      "abfwl2",
      "cwl2",
      "sv=2021-01-01&sig=wltest2",
      endpointSuffix = "new.domain.net")

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolAbfs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = true)

    val endpoints = hadoopConfig.get("fs.azure.abfs.valid.endpoints")
    endpoints should include("existing.domain.net")
    endpoints should include("new.domain.net")
  }

  it should "not duplicate an already-whitelisted domain" in {
    val now = freshTimestamp()
    hadoopConfig.set("fs.azure.abfs.valid.endpoints", "same.domain.net")

    val storageParams = sasStorageParams(
      "abfwl3",
      "cwl3",
      "sv=2021-01-01&sig=wltest3",
      endpointSuffix = "same.domain.net")

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolAbfs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = true)

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
      storageParams,
      KCONST.storageProtocolWasbs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = false)

    val expectedKey = "fs.azure.sas.cc1.cache1.blob.core.windows.net"
    val firstValue = hadoopConfig.get(expectedKey)
    firstValue should not be null

    // Mutate the hadoop config to detect if it gets overwritten
    hadoopConfig.set(expectedKey, "MUTATED")

    // Second call with same timestamp — should be cached, not overwritten
    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolWasbs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = false)

    hadoopConfig.get(expectedKey) shouldBe "MUTATED"
  }

  it should "reinstall WASBS credentials when forceRefresh is set (transient cache hit)" in {
    val now = Instant.now()
    val storageParams = sasStorageParams("cache2", "cc2", "sv=2021-01-01&sig=forced")
    val expectedKey = "fs.azure.sas.cc2.cache2.blob.core.windows.net"

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolWasbs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = false)
    hadoopConfig.get(expectedKey) should include("sig=forced")

    // A cached export re-read from another Spark session gets a pristine Configuration; the
    // credentials must be installed again even though the process wide cache reports a hit.
    val freshConfig = new Configuration()
    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolWasbs,
      freshConfig,
      sparkConf,
      now,
      useAbfs = false,
      forceRefresh = true)
    freshConfig.get(expectedKey) should include("sig=forced")
  }

  it should "reinstall account key credentials when forceRefresh is set" in {
    val now = Instant.now()
    val storageParams = keyStorageParams("cache3", "forcedkey", "cc3")
    val expectedKey = "fs.azure.account.key.cache3.blob.core.windows.net"

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolWasbs,
      hadoopConfig,
      sparkConf,
      now,
      useAbfs = false)
    hadoopConfig.get(expectedKey) shouldBe "forcedkey"

    val freshConfig = new Configuration()
    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolWasbs,
      freshConfig,
      sparkConf,
      now,
      useAbfs = false,
      forceRefresh = true)
    freshConfig.get(expectedKey) shouldBe "forcedkey"
  }

  // ---------------------------------------------------------------------------
  // Conflicting SAS credentials on the same storage account
  // ---------------------------------------------------------------------------
  "dedupeConflictingCredentials" should "drop sibling containers with a different token" in {
    val params = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials("https://dedup1.blob.core.windows.net/c1?sig=one"),
        new TransientStorageCredentials("https://dedup1.blob.core.windows.net/c2?sig=two"),
        new TransientStorageCredentials("https://dedup2.blob.core.windows.net/c3?sig=three")),
      "core.windows.net")

    val result = KustoReader.dedupeConflictingCredentials(params)
    result.storageCredentials.map(_.blobContainer).toSeq shouldBe Seq("c1", "c3")
    result.endpointSuffix shouldBe "core.windows.net"
  }

  it should "not drop containers whose tokens differ only by a leading '?'" in {
    val withQuestionMark =
      new TransientStorageCredentials("https://dedup4.blob.core.windows.net/c1?sig=same")
    val withoutQuestionMark =
      new TransientStorageCredentials("https://dedup4.blob.core.windows.net/c2?sig=same")
    withoutQuestionMark.sasKey = withoutQuestionMark.sasKey.stripPrefix("?")

    val params = new TransientStorageParameters(
      Array(withQuestionMark, withoutQuestionMark),
      "core.windows.net")

    KustoReader
      .dedupeConflictingCredentials(params)
      .storageCredentials should have length 2
  }

  it should "keep sibling containers that share the same token" in {
    val params = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials("https://dedup3.blob.core.windows.net/c1?sig=same"),
        new TransientStorageCredentials("https://dedup3.blob.core.windows.net/c2?sig=same")),
      "core.windows.net")

    val result = KustoReader.dedupeConflictingCredentials(params)
    result.storageCredentials should have length 2
    result should be theSameInstanceAs params
  }

  it should "leave non-SAS credentials untouched" in {
    val params = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials("keyacct", "key1", "c1"),
        new TransientStorageCredentials("keyacct", "key2", "c2")),
      "core.windows.net")

    val result = KustoReader.dedupeConflictingCredentials(params)
    result.storageCredentials should have length 2
  }

  it should "drop an impersonation container that shares an account with a SAS container" in {
    val params = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials("https://dedup5.blob.core.windows.net/c1?sig=one"),
        new TransientStorageCredentials("https://dedup5.blob.core.windows.net/c2;impersonate")),
      "core.windows.net")

    val result = KustoReader.dedupeConflictingCredentials(params)
    result.storageCredentials.map(_.blobContainer).toSeq shouldBe Seq("c1")
  }

  it should "keep the SAS container even when impersonation is returned first" in {
    val params = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials("https://dedup6.blob.core.windows.net/c1;impersonate"),
        new TransientStorageCredentials("https://dedup6.blob.core.windows.net/c2?sig=one"),
        new TransientStorageCredentials("https://dedup6.blob.core.windows.net/c3?sig=two")),
      "core.windows.net")

    val result = KustoReader.dedupeConflictingCredentials(params)
    result.storageCredentials.map(_.blobContainer).toSeq shouldBe Seq("c2")
  }

  it should "keep every container of an account that only uses impersonation" in {
    val params = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials("https://dedup7.blob.core.windows.net/c1;impersonate"),
        new TransientStorageCredentials("https://dedup7.blob.core.windows.net/c2;impersonate")),
      "core.windows.net")

    val result = KustoReader.dedupeConflictingCredentials(params)
    result.storageCredentials.map(_.blobContainer).toSeq shouldBe Seq("c1", "c2")
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
      Array(
        new TransientStorageCredentials(
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

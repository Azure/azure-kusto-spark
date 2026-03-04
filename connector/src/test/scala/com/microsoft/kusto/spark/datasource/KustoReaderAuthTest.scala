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

class KustoReaderAuthTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  private val DefaultEndpointSuffix = "core.windows.net"
  private val TestKey = "testkey123"
  private var sparkConf: RuntimeConfig = _
  override def beforeEach(): Unit = {
    sparkConf = SparkSession
      .builder()
      .appName("KustoReaderAuthTest")
      .master(f"local[1]")
      .getOrCreate()
      .conf
  }

  "setHadoopAuth" should "configure WASBS with SAS token correctly" in {
    val config = new Configuration()
    val now = Instant.now()
    val storageParams = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials(
          "https://testaccount.blob.core.windows.net/testcontainer?sv=2021-01-01&sig=test")),
      DefaultEndpointSuffix)

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolWasbs,
      config,
      sparkConf,
      now,
      useAbfs = false)

    val sasKey = config.get("fs.azure.sas.testcontainer.testaccount.blob.core.windows.net")
    sasKey should not be null
    sasKey should include("sv=2021-01-01")
  }

  it should "configure WASBS with Account Key correctly" in {
    val config = new Configuration()
    val now = Instant.now()

    val storageParams = new TransientStorageParameters(
      Array(new TransientStorageCredentials("testaccount", TestKey, "testcontainer")),
      DefaultEndpointSuffix)

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolWasbs,
      config,
      sparkConf,
      now,
      useAbfs = false)

    val accountKey = config.get("fs.azure.account.key.testaccount.blob.core.windows.net")
    accountKey shouldBe TestKey
  }

  it should "configure ABFS with SAS token correctly" in {
    val config = new Configuration()
    // Use an expired timestamp to force cache refresh
    val now = Instant.now().minus(3 * KCONST.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES)

    val storageParams = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials(
          "https://testaccount2.blob.core.windows.net/testcontainer2?sv=2021-01-01&sig=test")),
      DefaultEndpointSuffix)

    // Set up expectations before calling the method

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolAbfs,
      config,
      sparkConf,
      now,
      useAbfs = true)
  }

  it should "throw exception for ABFS with Account Key" in {
    val config = new Configuration()
    // Use an expired timestamp to force cache refresh
    val now = Instant.now().minus(3 * KCONST.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES)

    val storageParams = new TransientStorageParameters(
      Array(new TransientStorageCredentials("testaccount3", TestKey, "testcontainer3")),
      DefaultEndpointSuffix)

    val exception = intercept[java.security.InvalidParameterException] {
      KustoReader.setHadoopAuth(
        storageParams,
        KCONST.storageProtocolAbfs,
        config,
        sparkConf,
        now,
        useAbfs = true)
    }

    exception.getMessage should include("not supported")
  }

  it should "whitelist storage domain for ABFS" in {
    val config = new Configuration()
    // Use an expired timestamp to force cache refresh
    val now = Instant.now().minus(3 * KCONST.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES)

    val storageParams = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials(
          "https://testaccount4.blob.customdomain.net/testcontainer4?sv=test")),
      "customdomain.net")

    KustoReader.setHadoopAuth(
      storageParams,
      KCONST.storageProtocolAbfs,
      config,
      sparkConf,
      now,
      useAbfs = true)

    val endpoints = config.get("fs.azure.abfs.valid.endpoints")
    endpoints should include("customdomain.net")
  }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.azure.core.credential.{AccessToken, TokenRequestContext}
import com.azure.identity.AzureCliCredentialBuilder
import com.microsoft.azure.kusto.data.StringUtils
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.common.KustoDebugOptions
import com.microsoft.kusto.spark.KustoTestUtils.{KustoConnectionOptions, getSystemTestOptions}
import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.datasource.{KustoSourceOptions, ReadMode}
import com.microsoft.kusto.spark.utils.{
  ContainerProvider,
  DmExportStorageClient,
  ExportStorageMode
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, max, min, sum}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.{Collections, Date}

/** Live validation of the feature-flagged `Rest/Lake` distributed-read path. */
class KustoLakeExportStorageE2E extends AnyFlatSpec with BeforeAndAfterAll with Matchers {
  private val expectedRows =
    Option(System.getProperty("lakeExportExpectedRows")).map(_.toInt).getOrElse(200)
  private val ingestionUri =
    KustoTestUtils.getSystemVariable(KustoSinkOptions.KUSTO_INGESTION_URI)
  private lazy val options: KustoConnectionOptions = getSystemTestOptions
  private var skipReason: Option[String] = None

  private val spark = SparkSession
    .builder()
    .appName("KustoLakeExportStorageE2E")
    .master("local[4]")
    .getOrCreate()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (StringUtils.isBlank(ingestionUri)) {
      skipReason = Some(s"${KustoSinkOptions.KUSTO_INGESTION_URI} is required")
      return
    }

    val dmClient = new DmExportStorageClient(
      ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
        ingestionUri,
        options.accessToken),
      clusterAlias = "lake-export-e2e")

    if (dmClient.exportStorageMode != ExportStorageMode.LakeFolders) {
      skipReason = Some("The target cluster is not configured for Rest/Lake")
      return
    }

    val targets =
      dmClient.getExportStorage.getOrElse(fail("ExportStorage returned no Rest/Lake targets"))
    targets.containers shouldBe empty
    val folders = targets.lakeFolders.flatMap(ContainerProvider.parseValidApiLakeFolder)
    folders should have size targets.lakeFolders.size
    folders.foreach { folder =>
      val endpoint =
        com.microsoft.kusto.spark.utils.ExtendedKustoClient
          .toTransientStorageCredentials(folder)
          .oneLakeEndpoint
      spark.sparkContext.hadoopConfiguration
        .set(s"fs.azure.account.auth.type.$endpoint", "Custom")
      spark.sparkContext.hadoopConfiguration.set(
        s"fs.azure.account.oauth.provider.type.$endpoint",
        classOf[LakeExportAzCliTokenProvider].getName)
    }
  }

  override protected def afterAll(): Unit = {
    try spark.stop()
    finally super.afterAll()
  }

  "Rest/Lake distributed read" should
    "export through an API-provided OneLake folder and read with Spark" taggedAs KustoE2E in {
      assume(skipReason.isEmpty, skipReason.getOrElse(""))
      val query =
        s"range Id from 1 to $expectedRows step 1 | extend Value=strcat('rest-lake-', Id)"

      val result = spark.read
        .format("com.microsoft.kusto.spark.datasource")
        .option(KustoSourceOptions.KUSTO_CLUSTER, options.cluster)
        .option(KustoSourceOptions.KUSTO_INGESTION_URI, ingestionUri)
        .option(KustoSourceOptions.KUSTO_DATABASE, options.database)
        .option(KustoSourceOptions.KUSTO_QUERY, query)
        .option(KustoSourceOptions.KUSTO_ACCESS_TOKEN, options.accessToken)
        .option(KustoSourceOptions.KUSTO_READ_MODE, ReadMode.ForceDistributedMode.toString)
        .option(KustoDebugOptions.KUSTO_ENABLE_EXPORT_STORAGE_API, true.toString)
        .option(KustoSourceOptions.KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE, false.toString)
        .load()

      val summary = result
        .agg(
          count("*").as("rowCount"),
          min("Id").as("minimumId"),
          max("Id").as("maximumId"),
          sum("Id").as("idSum"))
        .first()

      summary.getLong(0) shouldBe expectedRows.toLong
      summary.getLong(1) shouldBe 1L
      summary.getLong(2) shouldBe expectedRows.toLong
      summary.getLong(3) shouldBe expectedRows.toLong * (expectedRows + 1L) / 2L
    }
}

class LakeExportAzCliTokenProvider extends CustomTokenProviderAdaptee {
  @volatile private var cachedToken: String = _
  @volatile private var cachedExpiry = new Date(0L)

  override def initialize(configuration: Configuration, accountName: String): Unit = {}

  override def getAccessToken: String = {
    if (cachedToken == null || cachedExpiry.getTime - System.currentTimeMillis() < 60000L) {
      refresh()
    }
    cachedToken
  }

  override def getExpiryTime: Date = cachedExpiry

  private def refresh(): Unit = synchronized {
    val context = new TokenRequestContext()
      .setScopes(Collections.singletonList("https://storage.azure.com/.default"))
    val token: AccessToken = new AzureCliCredentialBuilder().build().getToken(context).block()
    cachedToken = token.getToken
    cachedExpiry = new Date(token.getExpiresAt.toInstant.toEpochMilli)
  }
}

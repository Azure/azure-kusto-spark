// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.StringUtils
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.common.KustoDebugOptions
import com.microsoft.kusto.spark.KustoTestUtils.{KustoConnectionOptions, getSystemTestOptions}
import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.datasource.{KustoSourceOptions, ReadMode}
import com.microsoft.kusto.spark.utils.{
  ContainerProvider,
  DmExportStorageClient,
  ExportStorageMode,
  KustoDataSourceUtils => KDSU
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, max, min, sum}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Live validation of the feature-flagged `Rest/Storage` distributed-read path. */
class KustoRestStorageExportE2E extends AnyFlatSpec with BeforeAndAfterAll with Matchers {
  private val expectedRows =
    Option(System.getProperty("restStorageExpectedRows")).map(_.toInt).getOrElse(200)
  private val ingestionUri =
    KustoTestUtils.getSystemVariable(KustoSinkOptions.KUSTO_INGESTION_URI)
  private lazy val kustoConnectionOptions: KustoConnectionOptions = getSystemTestOptions
  private var skipReason: Option[String] = None

  private val spark = SparkSession
    .builder()
    .appName("KustoRestStorageExportE2E")
    .master("local[4]")
    .getOrCreate()

  private val query =
    s"range Id from 1 to $expectedRows step 1 | extend Value=strcat('rest-storage-', Id)"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (StringUtils.isBlank(ingestionUri)) {
      skipReason = Some(s"${KustoSinkOptions.KUSTO_INGESTION_URI} is required")
      return
    }

    val dmClient = new DmExportStorageClient(
      ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
        ingestionUri,
        kustoConnectionOptions.accessToken),
      clusterAlias = "rest-storage-e2e")

    if (dmClient.exportStorageMode != ExportStorageMode.BlobContainers) {
      skipReason = Some("The target cluster is not configured for Rest/Storage")
      return
    }

    val targets = dmClient.getExportStorage.getOrElse(
      fail("The Rest/Storage export storage API returned no usable blob targets"))
    targets.lakeFolders shouldBe empty
    targets.containers should not be empty
    targets.containers.foreach { target =>
      withClue("ExportStorage returned an invalid or SAS-less blob target") {
        ContainerProvider.parseValidApiContainerWithSas(target) shouldBe defined
      }
    }
  }

  override protected def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
  }

  "Rest/Storage distributed read" should
    "export through an API-provided SAS container and read the parquet with Spark" taggedAs KustoE2E in {
      assume(skipReason.isEmpty, skipReason.getOrElse(""))
      val requestId = s"KustoRestStorageExportE2E-${java.util.UUID.randomUUID()}"
      KDSU.logInfo(getClass.getSimpleName, s"Starting live Rest/Storage read, id: $requestId")

      val result = spark.read
        .format("com.microsoft.kusto.spark.datasource")
        .option(KustoSourceOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
        .option(KustoSourceOptions.KUSTO_INGESTION_URI, ingestionUri)
        .option(KustoSourceOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
        .option(KustoSourceOptions.KUSTO_QUERY, query)
        .option(KustoSourceOptions.KUSTO_ACCESS_TOKEN, kustoConnectionOptions.accessToken)
        .option(KustoSourceOptions.KUSTO_READ_MODE, ReadMode.ForceDistributedMode.toString)
        .option(KustoDebugOptions.KUSTO_ENABLE_EXPORT_STORAGE_API, true.toString)
        .option(KustoSourceOptions.KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE, false.toString)
        .option(KustoSourceOptions.KUSTO_REQUEST_ID, requestId)
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

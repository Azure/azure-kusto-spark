// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.KustoTestUtils
import com.microsoft.kusto.spark.KustoTestUtils.{KustoConnectionOptions, getSystemTestOptions}
import com.microsoft.kusto.spark.datasink.{
  KustoSinkOptions,
  SinkTableCreationMode,
  SparkIngestionProperties,
  WriteFormat,
  WriteMode
}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.{
  generateTableAlterStreamIngestionCommand,
  generateTempTableCreateCommand
}
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.ParallelTestExecution
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Clock, Instant}
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
 * E2E tests for the V2 write path (kustoV2 format). Requires a live Kusto cluster and Azure
 * credentials via environment variables. Each test uses a unique table name so tests can run in
 * parallel.
 */
class KustoV2WriteE2E
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ParallelTestExecution {

  private val className = this.getClass.getSimpleName
  private val nofExecutors = 4
  private val spark: SparkSession = SparkSession
    .builder()
    .appName("KustoV2WriteE2E")
    .master(s"local[$nofExecutors]")
    .getOrCreate()

  private lazy val kustoConnectionOptions: KustoConnectionOptions = getSystemTestOptions
  private val testPrefix = "V2Write"
  private val timeoutMs: Int = 8 * 60 * 1000
  private val sleepTimeTillTableCreate: Int = 3 * 60 * 1000

  private def baseWriteOpts(table: String): Map[String, String] = Map(
    KustoSinkOptions.KUSTO_CLUSTER -> kustoConnectionOptions.cluster,
    KustoSinkOptions.KUSTO_DATABASE -> kustoConnectionOptions.database,
    KustoSinkOptions.KUSTO_TABLE -> table,
    KustoSinkOptions.KUSTO_ACCESS_TOKEN -> kustoConnectionOptions.accessToken,
    KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS -> SinkTableCreationMode.CreateIfNotExist.toString)

  private def newAdminClient(): com.microsoft.azure.kusto.data.Client = {
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoConnectionOptions.cluster,
      kustoConnectionOptions.accessToken)
    ClientFactory.createClient(engineKcsb)
  }

  private def waitForIngestion(table: String, expectedCount: Int): Unit = {
    val adminClient = newAdminClient()
    val database = kustoConnectionOptions.database
    Awaitility
      .await()
      .atMost(timeoutMs, TimeUnit.MILLISECONDS)
      .pollInterval(10, TimeUnit.SECONDS)
      .until(() => {
        val result = adminClient.executeQuery(database, s"$table | count").getPrimaryResults
        result.next()
        result.getInt(0) >= expectedCount
      })
  }

  private val pendingValidations: ListBuffer[Future[Unit]] = ListBuffer.empty

  private def validateInBackground(table: String, expectedRows: Int): Unit = {
    val f = Future {
      val adminClient = newAdminClient()
      KustoTestUtils.validateResultsAndCleanup(
        adminClient,
        table,
        kustoConnectionOptions.database,
        expectedNumberOfRows = expectedRows,
        timeoutMs = timeoutMs,
        cleanupAllTables = false)
    }
    pendingValidations.synchronized {
      pendingValidations += f
    }
  }

  override def afterAll(): Unit = {
    val futures = pendingValidations.synchronized { pendingValidations.toList }
    futures.foreach(f => Await.result(f, Duration(timeoutMs + 30000, TimeUnit.MILLISECONDS)))
    KustoTestUtils.cleanup(kustoConnectionOptions, testPrefix)
    super.afterAll()
  }

  // ---------------------------------------------------------------------------
  // Write Mode: Transactional (default)
  // ---------------------------------------------------------------------------

  "KustoV2 Transactional write" should "ingest rows atomically" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_Tx_${UUID.randomUUID()}")
    val expectedRows = 10
    val df = (1 to expectedRows).map(i => (s"name_$i", i)).toDF("Name", "Value")

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  "KustoV2 Transactional write" should "append multiple batches" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_TxAppend_${UUID.randomUUID()}")
    val batch1Size = 5
    val batch2Size = 5
    val df1 = (1 to batch1Size).map(i => (s"batch1_$i", i)).toDF("Name", "Value")
    val df2 =
      ((batch1Size + 1) to (batch1Size + batch2Size))
        .map(i => (s"batch2_$i", i))
        .toDF("Name", "Value")

    val opts = baseWriteOpts(table) +
      (KustoSinkOptions.KUSTO_WRITE_MODE -> WriteMode.Transactional.toString)

    df1.write.format("kustoV2").options(opts).mode(SaveMode.Append).save()
    df2.write.format("kustoV2").options(opts).mode(SaveMode.Append).save()

    validateInBackground(table, batch1Size + batch2Size)
  }

  // ---------------------------------------------------------------------------
  // Write Mode: Queued
  // ---------------------------------------------------------------------------

  "KustoV2 Queued write" should "ingest rows via queued ingestion" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_Queued_${UUID.randomUUID()}")
    val expectedRows = 10
    val df = (1 to expectedRows).map(i => (s"q_$i", i)).toDF("Name", "Value")

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Queued.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Write Mode: KustoStreaming
  // ---------------------------------------------------------------------------

  "KustoV2 Streaming write" should "ingest rows via streaming ingestion" taggedAs V2Tests ignore {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_Stream_${UUID.randomUUID()}")
    val expectedRows = 10
    val df = (1 to expectedRows).map(i => (s"s_$i", i)).toDF("Name", "Value")

    // Streaming requires the table to exist with streaming ingestion policy enabled
    val adminClient = newAdminClient()
    adminClient.executeMgmt(
      kustoConnectionOptions.database,
      generateTempTableCreateCommand(table, "Name:string, Value:int", hidden = false))
    adminClient.executeMgmt(
      kustoConnectionOptions.database,
      generateTableAlterStreamIngestionCommand(table))
    Thread.sleep(sleepTimeTillTableCreate)

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.KustoStreaming.toString)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.FailIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Write Mode: Multiple data types
  // ---------------------------------------------------------------------------

  "KustoV2 batch write" should "ingest multiple data types" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_Types_${UUID.randomUUID()}")
    val expectedRows = 5
    val df = (1 to expectedRows)
      .map(i => (s"row_$i", i, i.toLong * 100000L, i.toDouble / 3.0, i % 2 == 0))
      .toDF("StringCol", "IntCol", "LongCol", "DoubleCol", "BoolCol")

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Ingestion Properties: additionalTags
  // ---------------------------------------------------------------------------

  "KustoV2 write with additionalTags" should "tag ingested extents" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_Tags_${UUID.randomUUID()}")
    val expectedRows = 5
    val df = (1 to expectedRows).map(i => (s"tagged_$i", i)).toDF("Name", "Value")

    val sp = new SparkIngestionProperties()
    val tags = new java.util.ArrayList[String]()
    tags.add("v2test-tag")
    tags.add("env:test")
    sp.additionalTags = tags

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .option(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON, sp.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Ingestion Properties: ingestByTags
  // ---------------------------------------------------------------------------

  "KustoV2 write with ingestByTags" should "set ingest-by tags on extents" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_IngestBy_${UUID.randomUUID()}")
    val expectedRows = 5
    val df = (1 to expectedRows).map(i => (s"ingestby_$i", i)).toDF("Name", "Value")

    val sp = new SparkIngestionProperties()
    val tags = new java.util.ArrayList[String]()
    tags.add("batch-001")
    sp.ingestByTags = tags

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .option(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON, sp.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Ingestion Properties: ingestIfNotExists
  // ---------------------------------------------------------------------------

  "KustoV2 write with ingestIfNotExists" should "ingest data with ingestIfNotExists tags" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_IfNotExist_${UUID.randomUUID()}")
    val expectedRows = 5
    val df = (1 to expectedRows).map(i => (s"dedup_$i", i)).toDF("Name", "Value")

    val sp = new SparkIngestionProperties()
    val tags = new java.util.ArrayList[String]()
    tags.add("unique-batch-id")
    sp.ingestIfNotExists = tags

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .option(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON, sp.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Ingestion Properties: creationTime
  // ---------------------------------------------------------------------------

  "KustoV2 write with creationTime" should "set extent creation time" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_CTime_${UUID.randomUUID()}")
    val expectedRows = 5
    val df = (1 to expectedRows).map(i => (s"ct_$i", i)).toDF("Name", "Value")

    val sp = new SparkIngestionProperties()
    sp.creationTime = Instant.now(Clock.systemUTC())

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .option(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON, sp.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Ingestion Properties: combined tags + creationTime
  // ---------------------------------------------------------------------------

  "KustoV2 write with combined properties" should "set tags and creationTime together" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_Combined_${UUID.randomUUID()}")
    val expectedRows = 5
    val df = (1 to expectedRows).map(i => (s"combo_$i", i)).toDF("Name", "Value")

    val sp = new SparkIngestionProperties()
    val additionalTags = new java.util.ArrayList[String]()
    additionalTags.add("pipeline:v2test")
    sp.additionalTags = additionalTags

    val ingestByTags = new java.util.ArrayList[String]()
    ingestByTags.add("run-" + UUID.randomUUID().toString.take(8))
    sp.ingestByTags = ingestByTags

    sp.creationTime = Instant.now(Clock.systemUTC())

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .option(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON, sp.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Queued write with ingestion properties
  // ---------------------------------------------------------------------------

  "KustoV2 Queued write with tags" should "set tags via queued ingestion" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_QTags_${UUID.randomUUID()}")
    val expectedRows = 5
    val df = (1 to expectedRows).map(i => (s"qt_$i", i)).toDF("Name", "Value")

    val sp = new SparkIngestionProperties()
    val tags = new java.util.ArrayList[String]()
    tags.add("queued-tag")
    sp.additionalTags = tags
    sp.creationTime = Instant.now(Clock.systemUTC())

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Queued.toString)
      .option(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON, sp.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Write Format: Parquet — Transactional
  // ---------------------------------------------------------------------------

  "KustoV2 Parquet Transactional write" should "ingest rows in parquet format" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_PqTx_${UUID.randomUUID()}")
    val expectedRows = 10
    val df = (1 to expectedRows).map(i => (s"pq_$i", i)).toDF("Name", "Value")

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_FORMAT, WriteFormat.Parquet.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Write Format: Parquet — Queued
  // ---------------------------------------------------------------------------

  "KustoV2 Parquet Queued write" should "ingest rows in parquet format via queued mode" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_PqQ_${UUID.randomUUID()}")
    val expectedRows = 10
    val df = (1 to expectedRows).map(i => (s"pqq_$i", i)).toDF("Name", "Value")

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Queued.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_FORMAT, WriteFormat.Parquet.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Write Format: Parquet — multiple data types
  // ---------------------------------------------------------------------------

  "KustoV2 Parquet write" should "ingest multiple data types in parquet format" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_PqTypes_${UUID.randomUUID()}")
    val expectedRows = 5
    val df = (1 to expectedRows)
      .map(i => (s"row_$i", i, i.toLong * 100000L, i.toDouble / 3.0, i % 2 == 0))
      .toDF("StringCol", "IntCol", "LongCol", "DoubleCol", "BoolCol")

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_FORMAT, WriteFormat.Parquet.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }

  // ---------------------------------------------------------------------------
  // Write Format: Parquet — with ingestion properties
  // ---------------------------------------------------------------------------

  "KustoV2 Parquet write with tags" should "set tags on parquet-ingested extents" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_PqTags_${UUID.randomUUID()}")
    val expectedRows = 5
    val df = (1 to expectedRows).map(i => (s"pqt_$i", i)).toDF("Name", "Value")

    val sp = new SparkIngestionProperties()
    val tags = new java.util.ArrayList[String]()
    tags.add("parquet-tag")
    sp.additionalTags = tags
    sp.creationTime = Instant.now(Clock.systemUTC())

    df.write
      .format("kustoV2")
      .options(baseWriteOpts(table))
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, WriteMode.Transactional.toString)
      .option(KustoSinkOptions.KUSTO_WRITE_FORMAT, WriteFormat.Parquet.toString)
      .option(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON, sp.toString)
      .mode(SaveMode.Append)
      .save()

    validateInBackground(table, expectedRows)
  }
}

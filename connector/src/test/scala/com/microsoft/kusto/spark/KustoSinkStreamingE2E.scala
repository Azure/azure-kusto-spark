// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.KustoTestUtils.getSystemTestOptions
import com.microsoft.kusto.spark.common.KustoDebugOptions
import com.microsoft.kusto.spark.datasink.{
  KustoSinkOptions,
  SinkTableCreationMode,
  SparkIngestionProperties,
  WriteMode
}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DataTypes.IntegerType
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.security.InvalidParameterException
import java.util.UUID

class KustoSinkStreamingE2E extends AnyFlatSpec with BeforeAndAfterAll {
  val expectedNumberOfRows: Int = 300
  val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes
  val sleepTimeTillTableCreate: Int = 3 * 60 * 1000 // 2 minutes
  val spark: SparkSession = SparkSession
    .builder()
    .appName("KustoSink")
    .master("local[4]")
    .getOrCreate()
  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = spark.sparkContext
    sqlContext = spark.sqlContext
  }

  override def afterAll(): Unit = {
    super.afterAll()
    // sc.stop()
  }
  private lazy val kustoTestConnectionOptions = getSystemTestOptions
  val csvPath: String = System.getProperty("path", "connector/src/test/resources/TestData/csv")
  val customSchema: StructType = new StructType()
    .add("colA", StringType, nullable = true)
    .add("colB", IntegerType, nullable = true)

  "KustoStreamingSinkSyncWithTableCreateAndIngestIfNotExist" should "ingest structured data to a Kusto cluster" taggedAs KustoE2E in {
    val prefix = "KustoStreamingSparkE2E_Ingest"
    val table = s"${prefix}_${UUID.randomUUID().toString.replace("-", "_")}"
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    val csvDf = spark.readStream
      .schema(customSchema)
      .csv(csvPath)

    val consoleQ = csvDf.writeStream
      .format("console")
      .trigger(Trigger.Once)
    consoleQ.start()

    val sp = new SparkIngestionProperties
    val tags = new java.util.ArrayList[String]()
    tags.add("tagytag")
    sp.ingestByTags = tags
    sp.ingestIfNotExists = tags

    spark.conf.set("spark.sql.streaming.checkpointLocation", "target/temp/checkpoint")

    val kustoQ = csvDf.writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .options(Map(
        KustoSinkOptions.KUSTO_CLUSTER -> kustoTestConnectionOptions.cluster,
        KustoSinkOptions.KUSTO_TABLE -> table,
        KustoSinkOptions.KUSTO_DATABASE -> kustoTestConnectionOptions.database,
        KustoSinkOptions.KUSTO_ACCESS_TOKEN -> kustoTestConnectionOptions.accessToken,
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS -> SinkTableCreationMode.CreateIfNotExist.toString,
        KustoDebugOptions.KUSTO_ENSURE_NO_DUPLICATED_BLOBS -> true.toString,
        KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON -> sp.toString))
      .trigger(Trigger.Once)

    kustoQ.start().awaitTermination(sleepTimeTillTableCreate)

    // Sleep util table is expected to be created
    Thread.sleep(sleepTimeTillTableCreate)
    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs - sleepTimeTillTableCreate,
      tableCleanupPrefix = prefix)
  }

  "KustoStreamingSinkAsync" should "also ingest structured data to a Kusto cluster" taggedAs KustoE2E in {
    val prefix = "KustoStreamingSparkE2EAsync_Ingest"
    val table = s"${prefix}_${UUID.randomUUID().toString.replace("-", "_")}"
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    kustoAdminClient.executeMgmt(
      kustoTestConnectionOptions.database,
      generateTempTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))

    val csvDf = spark.readStream
      .schema(customSchema)
      .csv(csvPath)

    val consoleQ = csvDf.writeStream
      .format("console")
      .trigger(Trigger.Once)

    consoleQ.start().awaitTermination()

    spark.conf.set("spark.sql.streaming.checkpointLocation", "target/temp/checkpoint")

    val kustoQ = csvDf.writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .options(Map(
        KustoSinkOptions.KUSTO_CLUSTER -> kustoTestConnectionOptions.cluster,
        KustoSinkOptions.KUSTO_TABLE -> table,
        KustoSinkOptions.KUSTO_DATABASE -> kustoTestConnectionOptions.database,
        KustoSinkOptions.KUSTO_AAD_APP_ID -> kustoTestConnectionOptions.accessToken,
        KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC -> "true"))
      .trigger(Trigger.Once)

    kustoQ.start().awaitTermination()

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      tableCleanupPrefix = prefix)
  }

  "KustoStreamingSinkStreamingIngestion" should "ingest structured data to a Kusto cluster using stream ingestion" taggedAs KustoE2E in {
    val prefix = "KustoStreamingSparkE2E_StreamIngest"
    val table = s"${prefix}_${UUID.randomUUID().toString.replace("-", "_")}"
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      s"https://${kustoTestConnectionOptions.cluster}.kusto.windows.net",
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.executeMgmt(
      kustoTestConnectionOptions.database,
      generateTempTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))
    kustoAdminClient.executeMgmt(
      kustoTestConnectionOptions.database,
      generateTableAlterStreamIngestionCommand(table))
    kustoAdminClient.executeMgmt(
      kustoTestConnectionOptions.database,
      generateClearStreamingIngestionCacheCommand(table))

    val csvDf = spark.readStream
      .schema(customSchema)
      .csv(csvPath)

    spark.conf.set("spark.sql.streaming.checkpointLocation", "target/temp/checkpoint")

    val kustoQ = csvDf.writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .options(Map(
        KustoSinkOptions.KUSTO_CLUSTER -> kustoTestConnectionOptions.cluster,
        KustoSinkOptions.KUSTO_TABLE -> table,
        KustoSinkOptions.KUSTO_DATABASE -> kustoTestConnectionOptions.database,
        KustoSinkOptions.KUSTO_ACCESS_TOKEN -> kustoTestConnectionOptions.accessToken,
        KustoSinkOptions.KUSTO_WRITE_MODE -> WriteMode.KustoStreaming.toString))
      .trigger(Trigger.Once)

    kustoQ.start().awaitTermination()

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      10,
      tableCleanupPrefix = prefix)
  }

  "KustoStreamingSinkStreamingIngestionWithCreate" should "ingest structured data to a Kusto cluster using stream ingestion" taggedAs KustoE2E in {
    val prefix = "KustoStreamingSparkE2E_StreamIngest"
    val table = s"${prefix}_${UUID.randomUUID().toString.replace("-", "_")}"
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      s"https://${kustoTestConnectionOptions.cluster}.kusto.windows.net",
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    val csvDf = spark.readStream
      .schema(customSchema)
      .csv(csvPath)
    spark.conf.set("spark.sql.streaming.checkpointLocation", "target/temp/checkpoint")
    val kustoQ = csvDf.writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .options(Map(
        KustoSinkOptions.KUSTO_CLUSTER -> kustoTestConnectionOptions.cluster,
        KustoSinkOptions.KUSTO_TABLE -> table,
        KustoSinkOptions.KUSTO_DATABASE -> kustoTestConnectionOptions.database,
        KustoSinkOptions.KUSTO_ACCESS_TOKEN -> kustoTestConnectionOptions.accessToken,
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS -> SinkTableCreationMode.CreateIfNotExist.toString,
        KustoSinkOptions.KUSTO_WRITE_MODE -> WriteMode.KustoStreaming.toString))
      .trigger(Trigger.Once)
    kustoQ.start().awaitTermination()
    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      10,
      tableCleanupPrefix = prefix)
  }
}

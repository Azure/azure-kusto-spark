//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.KustoTestUtils.KustoConnectionOptions
import com.microsoft.kusto.spark.common.KustoDebugOptions
import com.microsoft.kusto.spark.datasink.{
  KustoSinkOptions,
  SinkTableCreationMode,
  SparkIngestionProperties
}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DataTypes.IntegerType
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

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

    sc.stop()
  }
  private lazy val kustoConnectionOptions: KustoConnectionOptions =
    KustoTestUtils.getSystemTestOptions

  val csvPath: String = System.getProperty("path", "connector/src/test/resources/TestData/csv")
  val customSchema: StructType = new StructType()
    .add("colA", StringType, nullable = true)
    .add("colB", IntegerType, nullable = true)

  "KustoStreamingSinkSyncWithTableCreateAndIngestIfNotExist" should "ingest structured data to a Kusto cluster" taggedAs KustoE2E in {
    val prefix = "KustoStreamingSparkE2E_Ingest"
    val table = s"${prefix}_${UUID.randomUUID().toString.replace("-", "_")}"
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://${kustoConnectionOptions.cluster}.kusto.windows.net",
      kustoConnectionOptions.appId,
      kustoConnectionOptions.appKey,
      kustoConnectionOptions.authority)
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
        KustoSinkOptions.KUSTO_CLUSTER -> kustoConnectionOptions.cluster,
        KustoSinkOptions.KUSTO_TABLE -> table,
        KustoSinkOptions.KUSTO_DATABASE -> kustoConnectionOptions.database,
        KustoSinkOptions.KUSTO_AAD_APP_ID -> kustoConnectionOptions.appId,
        KustoSinkOptions.KUSTO_AAD_APP_SECRET -> kustoConnectionOptions.appKey,
        KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID -> kustoConnectionOptions.authority,
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
      kustoConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs - sleepTimeTillTableCreate,
      tableCleanupPrefix = prefix)
  }

  "KustoStreamingSinkAsync" should "also ingest structured data to a Kusto cluster" taggedAs KustoE2E in {
    val prefix = "KustoStreamingSparkE2EAsync_Ingest"
    val table = s"${prefix}_${UUID.randomUUID().toString.replace("-", "_")}"
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://${kustoConnectionOptions.cluster}.kusto.windows.net",
      kustoConnectionOptions.appId,
      kustoConnectionOptions.appKey,
      kustoConnectionOptions.authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    kustoAdminClient.execute(
      kustoConnectionOptions.database,
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
        KustoSinkOptions.KUSTO_CLUSTER -> kustoConnectionOptions.cluster,
        KustoSinkOptions.KUSTO_TABLE -> table,
        KustoSinkOptions.KUSTO_DATABASE -> kustoConnectionOptions.database,
        KustoSinkOptions.KUSTO_AAD_APP_ID -> kustoConnectionOptions.appId,
        KustoSinkOptions.KUSTO_AAD_APP_SECRET -> kustoConnectionOptions.appKey,
        KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID -> kustoConnectionOptions.authority,
        KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC -> "true"))
      .trigger(Trigger.Once)

    kustoQ.start().awaitTermination()

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      tableCleanupPrefix = prefix)
  }
}

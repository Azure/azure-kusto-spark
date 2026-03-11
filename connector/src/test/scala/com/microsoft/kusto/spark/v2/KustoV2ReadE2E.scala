// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.KustoTestUtils
import com.microsoft.kusto.spark.KustoTestUtils.{KustoConnectionOptions, getSystemTestOptions}
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SinkTableCreationMode}
import com.microsoft.kusto.spark.datasource.{KustoSourceOptions, ReadMode}
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.ParallelTestExecution
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
 * E2E tests for the V2 read path (kustoV2 format). Each test writes data via V2 write, waits for
 * ingestion, then reads it back via V2 read. Unique table names allow parallel execution.
 */
class KustoV2ReadE2E
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ParallelTestExecution {

  private val className = this.getClass.getSimpleName
  private val nofExecutors = 4
  private val spark: SparkSession = SparkSession
    .builder()
    .appName("KustoV2ReadE2E")
    .master(s"local[$nofExecutors]")
    .getOrCreate()

  private lazy val kustoConnectionOptions: KustoConnectionOptions = getSystemTestOptions
  private val testPrefix = "V2Read"
  private val timeoutMs: Int = 8 * 60 * 1000
  private val expectedRows = 10

  private val pendingAssertions: ListBuffer[Future[Unit]] = ListBuffer.empty

  private def assertInBackground(body: => Unit): Unit = {
    val f = Future(body)
    pendingAssertions.synchronized {
      pendingAssertions += f
    }
  }

  // Shared table populated in beforeAll
  private var sharedTable: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._

    sharedTable = KustoQueryUtils.simplifyName(s"${testPrefix}_Shared_${UUID.randomUUID()}")
    val df =
      (1 to expectedRows).map(i => (s"name_$i", i, i % 2 == 0)).toDF("Name", "Value", "IsEven")

    // Write seed data using V2
    df.write
      .format("kustoV2")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, sharedTable)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    // Wait for ingestion to complete
    waitForIngestion(sharedTable, expectedRows)
    KDSU.logInfo(className, s"Seed data ingested into $sharedTable")
  }

  override def afterAll(): Unit = {
    val futures = pendingAssertions.synchronized { pendingAssertions.toList }
    futures.foreach(f => Await.result(f, Duration(timeoutMs + 30000, TimeUnit.MILLISECONDS)))
    KustoTestUtils.cleanup(kustoConnectionOptions, testPrefix)
    super.afterAll()
  }

  "KustoV2 read" should "read data in single mode" taggedAs V2Tests in {
    val df = spark.read
      .format("kustoV2")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
      .option(KustoSourceOptions.KUSTO_QUERY, sharedTable)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoConnectionOptions.accessToken)
      .option(KustoSourceOptions.KUSTO_READ_MODE, ReadMode.ForceSingleMode.toString)
      .load()

    df.count() shouldEqual expectedRows
    df.columns should contain allOf ("Name", "Value", "IsEven")
  }

  "KustoV2 read" should "read data in distributed mode" taggedAs V2Tests in {
    val storageUrl = kustoConnectionOptions.storageContainerUrl
    storageUrl shouldBe defined

    val sas = KustoTestUtils.generateSasDelegationWithAzCli(storageUrl.get)

    val df = spark.read
      .format("kustoV2")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
      .option(KustoSourceOptions.KUSTO_QUERY, sharedTable)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoConnectionOptions.accessToken)
      .option(KustoSourceOptions.KUSTO_READ_MODE, ReadMode.ForceDistributedMode.toString)
      .option(KustoSourceOptions.KUSTO_TRANSIENT_STORAGE, sas)
      .load()

    df.count() shouldEqual expectedRows
  }

  "KustoV2 read" should "support column pruning" taggedAs V2Tests in {
    val df = spark.read
      .format("kustoV2")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
      .option(KustoSourceOptions.KUSTO_QUERY, sharedTable)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoConnectionOptions.accessToken)
      .option(KustoSourceOptions.KUSTO_READ_MODE, ReadMode.ForceSingleMode.toString)
      .load()
      .select("Name", "Value")

    df.columns should contain theSameElementsAs Seq("Name", "Value")
    df.columns should not contain "IsEven"
    df.count() shouldEqual expectedRows
  }

  "KustoV2 read" should "support filter pushdown" taggedAs V2Tests in {
    val df = spark.read
      .format("kustoV2")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
      .option(KustoSourceOptions.KUSTO_QUERY, sharedTable)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoConnectionOptions.accessToken)
      .option(KustoSourceOptions.KUSTO_READ_MODE, ReadMode.ForceSingleMode.toString)
      .load()
      .filter("Value > 5")

    df.count() shouldEqual 5
  }

  "KustoV2 write then read" should "round-trip data correctly" taggedAs V2Tests in {
    import spark.implicits._

    val table = KustoQueryUtils.simplifyName(s"${testPrefix}_RT_${UUID.randomUUID()}")
    val rowCount = 20
    val dfOrig = (1 to rowCount).map(i => (s"rt_$i", i * 10)).toDF("Key", "Amount")

    dfOrig.write
      .format("kustoV2")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    // Assert in background — wait for ingestion then validate read
    assertInBackground {
      waitForIngestion(table, rowCount)

      val dfRead = spark.read
        .format("kustoV2")
        .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
        .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
        .option(KustoSourceOptions.KUSTO_QUERY, table)
        .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoConnectionOptions.accessToken)
        .option(KustoSourceOptions.KUSTO_READ_MODE, ReadMode.ForceSingleMode.toString)
        .load()

      dfRead.count() shouldEqual rowCount
      dfRead.columns should contain allOf ("Key", "Amount")
    }
  }

  private def waitForIngestion(table: String, expectedCount: Int): Unit = {
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoConnectionOptions.cluster,
      kustoConnectionOptions.accessToken)
    val adminClient = ClientFactory.createClient(engineKcsb)
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
}

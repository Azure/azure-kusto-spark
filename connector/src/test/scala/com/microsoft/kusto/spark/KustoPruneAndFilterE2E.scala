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
import com.microsoft.kusto.spark.common.KustoDebugOptions
import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.datasource.{
  KustoSourceOptions,
  ReadMode,
  TransientStorageCredentials,
  TransientStorageParameters
}
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable

class KustoPruneAndFilterE2E extends AnyFlatSpec with BeforeAndAfterAll {
  private val nofExecutors = 4
  private val spark: SparkSession = SparkSession
    .builder()
    .appName("KustoSink")
    .master(f"local[$nofExecutors]")
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

  val appId: String = System.getProperty(KustoSourceOptions.KUSTO_AAD_APP_ID)
  val appKey: String = System.getProperty(KustoSourceOptions.KUSTO_AAD_APP_SECRET)
  val authority: String =
    System.getProperty(KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com")
  val cluster: String = System.getProperty(KustoSourceOptions.KUSTO_CLUSTER)
  val database: String = System.getProperty(KustoSourceOptions.KUSTO_DATABASE)

  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  "KustoSource" should "apply pruning and filtering when reading in single mode" taggedAs KustoE2E in {
    val table: String = System.getProperty(KustoSinkOptions.KUSTO_TABLE)
    val query: String = System.getProperty(
      KustoSourceOptions.KUSTO_QUERY,
      s"$table | where (toint(ColB) % 1000 == 0) ")

    val conf = Map(
      KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceSingleMode.toString)

    val df = spark.read.kusto(cluster, database, query, conf).select("ColA")
    df.show()
  }

  "KustoSource" should "apply pruning and filtering when reading in scale mode" taggedAs KustoE2E in {
    val table: String = System.getProperty(KustoSinkOptions.KUSTO_TABLE)
    val query: String =
      System.getProperty(KustoSourceOptions.KUSTO_QUERY, s"$table | where (toint(ColB) % 1 == 0)")

    val storageAccount: String = System.getProperty("storageAccount")
    val container: String = System.getProperty("container")
    val blobKey: String = System.getProperty("blobKey")
    val blobSas: String = System.getProperty("blobSas")
    val conf = if (blobSas == null) {
      val storage = new TransientStorageParameters(
        Array(new TransientStorageCredentials(storageAccount, blobKey, container)))

      Map(
        KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
        KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
        KustoSourceOptions.KUSTO_TRANSIENT_STORAGE -> storage.toString)
    } else {
      val storage =
        new TransientStorageParameters(Array(new TransientStorageCredentials(blobSas))) //          ,
      Map(
        KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
        KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
        KustoSourceOptions.KUSTO_TRANSIENT_STORAGE -> storage.toString
        // KustoDebugOptions.KUSTO_DBG_BLOB_FORCE_KEEP -> "true"
      )
    }

    val df = spark.read.kusto(cluster, database, query, conf)

    df.show(20)
  }

  "KustoConnector" should "write to a kusto table and read it back in scale mode with pruning and filtering" taggedAs KustoE2E in {
    import spark.implicits._

    val rowId = new AtomicInteger(1)
    def newRow(): String = s"row-${rowId.getAndIncrement()}"
    val expectedNumberOfRows: Int = 100
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val dfOrig = rows.toDF("name", "value")
    val query =
      KustoQueryUtils.simplifyName(s"KustoSparkReadWriteWithFiltersTest_${UUID.randomUUID()}")

    // Storage account parameters
    val storageAccount: String = System.getProperty("storageAccount")
    val container: String = System.getProperty("container")
    val blobKey: String = System.getProperty("blobKey")
    val blobSas: String = System.getProperty("blobSas")

    // Create a new table.
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://$cluster.kusto.windows.net",
      appId,
      appKey,
      authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(
      database,
      generateTempTableCreateCommand(query, columnsTypesAndNames = "ColA:string, ColB:int"))

    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, database)
      .option(KustoSinkOptions.KUSTO_TABLE, query)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, appId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, appKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .mode(SaveMode.Append)
      .save()

    val conf = if (blobSas != null) {
      val storage = new TransientStorageParameters(
        Array(new TransientStorageCredentials(storageAccount, blobKey, container)))
      Map(
        KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
        KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
        KustoSourceOptions.KUSTO_TRANSIENT_STORAGE -> storage.toString,
        KustoDebugOptions.KUSTO_DBG_BLOB_COMPRESS_ON_EXPORT -> "false"
      ) // Just to test this option
    } else {
      val storage =
        new TransientStorageParameters(Array(new TransientStorageCredentials(blobSas))) //          ,

      Map(
        KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
        KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
        KustoSourceOptions.KUSTO_TRANSIENT_STORAGE -> storage.toString,
        KustoDebugOptions.KUSTO_DBG_BLOB_COMPRESS_ON_EXPORT -> "false"
      ) // Just to test this option
    }

    val dfResult = spark.read.kusto(cluster, database, query, conf)

    val orig = dfOrig
      .select("name", "value")
      .rdd
      .map(x => (x.getString(0), x.getInt(1)))
      .collect()
      .sortBy(_._2)
    val result = dfResult
      .select("ColA", "ColB")
      .rdd
      .map(x => (x.getString(0), x.getInt(1)))
      .collect()
      .sortBy(_._2)

    // Verify correctness, without pruning and filtering
    assert(orig.deep == result.deep)

    val dfResultPruned = spark.read
      .kusto(cluster, database, query, conf)
      .select("ColA")
      .sort("ColA")
      .collect()
      .map(x => x.getString(0))
      .sorted

    val origPruned = orig.map(x => x._1).sorted

    assert(dfResultPruned.length == origPruned.length)
    assert(origPruned.deep == dfResultPruned.deep)

    // Cleanup
    KustoTestUtils.tryDropAllTablesByPrefix(
      kustoAdminClient,
      database,
      "KustoSparkReadWriteWithFiltersTest")
  }

  "KustoConnector" should "write to a kusto table and read it back in distributed mode with filtering" taggedAs KustoE2E in {
    import spark.implicits._

    val rowId = new AtomicInteger(1)
    def newRow(): String = s"row-${rowId.getAndIncrement()}"
    val expectedNumberOfRows: Int = 100
    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val dfOrig = rows.toDF("name", "value")
    val query =
      KustoQueryUtils.simplifyName(s"KustoSparkReadWriteWithFiltersTest_${UUID.randomUUID()}")

    // Storage account parameters
    val storageAccount: String = System.getProperty("storageAccount")
    val container: String = System.getProperty("container")
    val blobKey: String = System.getProperty("blobKey")
    val blobSas: String = System.getProperty("blobSas")

    // Create a new table.
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://$cluster.kusto.windows.net",
      appId,
      appKey,
      authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(
      database,
      generateTempTableCreateCommand(query, columnsTypesAndNames = "ColA:string, ColB:int"))

    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, database)
      .option(KustoSinkOptions.KUSTO_TABLE, query)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, appId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, appKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .mode(SaveMode.Append)
      .save()

    val conf = if (blobSas == null) {
      val storage = new TransientStorageParameters(
        Array(new TransientStorageCredentials(storageAccount, blobKey, container)))
      Map(
        KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
        KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
        KustoSourceOptions.KUSTO_TRANSIENT_STORAGE -> storage.toString)

    } else {
      val storage =
        new TransientStorageParameters(Array(new TransientStorageCredentials(blobSas))) //          ,

      Map(
        KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
        KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
        KustoSourceOptions.KUSTO_TRANSIENT_STORAGE -> storage.toString)
    }

    val dfResult = spark.read.kusto(cluster, database, query, conf)
    val dfFiltered = dfResult
      .where(dfResult.col("ColA']").startsWith("row-2"))
      .filter("ColB > 12")
      .filter("ColB <= 21")
      .collect()
      .sortBy(x => x.getAs[Int](1))

    val expected = Array(Row("row-20", 20), Row("row-21", 21))
    assert(dfFiltered.deep == expected.deep)

    // Cleanup
    KustoTestUtils.tryDropAllTablesByPrefix(
      kustoAdminClient,
      database,
      "KustoSparkReadWriteWithFiltersTest")
  }
}

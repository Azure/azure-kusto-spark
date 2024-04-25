// Copyright (c) 2017 Microsoft Corporation
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{Client, ClientFactory, ClientRequestProperties}
import com.microsoft.kusto.spark.KustoTestUtils.KustoConnectionOptions
import com.microsoft.kusto.spark.common.KustoDebugOptions
import com.microsoft.kusto.spark.datasink.{
  KustoSinkOptions,
  SinkTableCreationMode,
  SparkIngestionProperties
}
import com.microsoft.kusto.spark.datasource.{
  KustoSourceOptions,
  ReadMode,
  TransientStorageCredentials,
  TransientStorageParameters
}
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.hadoop.util.ComparableVersion
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable
import scala.util.{Failure, Random, Success, Try}

class KustoSourceE2E extends AnyFlatSpec with BeforeAndAfterAll {
  private val nofExecutors = 4
  private val spark: SparkSession = SparkSession
    .builder()
    .appName("KustoSink")
    .master(f"local[$nofExecutors]")
    .getOrCreate()
  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _

  private lazy val kustoConnectionOptions: KustoConnectionOptions = KustoTestUtils.getSystemTestOptions
  private val table =
    KustoQueryUtils.simplifyName(s"KustoSparkReadWriteTest_${UUID.randomUUID()}")
  private val className = this.getClass.getSimpleName

  private var kustoAdminClient: Option[Client] = None
  private var maybeKustoDmClient: Option[Client] = None
  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  loggingLevel match {
    case Some(level) => KDSU.setLoggingLevel(level)
    // default to warn for tests
    case None => KDSU.setLoggingLevel("WARN")
  }
  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = spark.sparkContext
    sqlContext = spark.sqlContext
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      KDSU.getEngineUrlFromAliasIfNeeded(kustoConnectionOptions.cluster),
      kustoConnectionOptions.accessToken)
    kustoAdminClient = Some(ClientFactory.createClient(engineKcsb))
    val ingestUrl =
      new StringBuffer(KDSU.getEngineUrlFromAliasIfNeeded(kustoConnectionOptions.cluster))
        .insert(8, "ingest-")
        .toString
    val ingestKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      ingestUrl,
      kustoConnectionOptions.accessToken)
    maybeKustoDmClient = Some(ClientFactory.createClient(ingestKcsb))
    Try(
      kustoAdminClient.get.execute(
        kustoConnectionOptions.database,
        generateAlterIngestionBatchingPolicyCommand(
          "database",
          kustoConnectionOptions.database,
          "{\"\"MaximumBatchingTimeSpan\"\":\"\"00:00:10\"\", \"\"MaximumNumberOfItems\"\": 500, \"\"MaximumRawDataSizeMB\"\": 1024}"))) match {
      case Success(_) => KDSU.logDebug(className, "Ingestion policy applied")
      case Failure(exception: Throwable) =>
        KDSU.reportExceptionAndThrow(
          className,
          exception,
          "Updating database batching policy",
          shouldNotThrow = true)
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    Try(
      // Remove table if stopping gracefully
      kustoAdminClient.get
        .execute(kustoConnectionOptions.database, generateTableDropCommand(table))) match {
      case Success(_) => KDSU.logDebug(className, "Ingestion policy applied")
      case Failure(e: Throwable) =>
        KDSU.reportExceptionAndThrow(className, e, "Dropping test table", shouldNotThrow = true)
    }
    sc.stop()
  }

  // Init dataFrame
  import spark.implicits._

  val rowId = new AtomicInteger(1)

  def newRow(): String = s"row-${rowId.getAndIncrement()}"
  val random = new Random()
  val maxBigDecimalTest: BigDecimal = 12345678901234567890.123456789012345678
  val minBigDecimalTest: BigDecimal = -12345678901234567890.123456789012345678
  /*
    This is the test we have to pass eventually when precision exceeds 34
    val maxBigDecimalTest:BigDecimal = BigDecimal("12345678901234567890.123456789012345678")
    val minBigDecimalTest:BigDecimal = BigDecimal("-12345678901234567890.123456789012345678")
   */
  val expectedNumberOfRows: Int = 100
  val rows: immutable.IndexedSeq[(String, Int, BigDecimal)] =
    (1 to expectedNumberOfRows).map(valueCol => {
      val nameCol = newRow()
      val decimalCol = if (valueCol == 1) {
        minBigDecimalTest
      } else if (valueCol == expectedNumberOfRows) {
        maxBigDecimalTest
      } else {
        BigDecimal.decimal(
          random.nextDouble() * (valueCol * 9999 - valueCol * 100) + valueCol * 100)
      }
      (nameCol, valueCol, decimalCol)
    })
  val dfOrig: DataFrame = rows.toDF("name", "value", "dec")

  "KustoConnector" should "write to a kusto table and read it back in default mode" in {
    // Create a new table.
    KDSU.logInfo("e2e", "running KustoConnector")
    val crp = new ClientRequestProperties
    crp.setTimeoutInMilliSec(60000)
    val ingestByTags = new java.util.ArrayList[String]
    val tag = "dammyTag"
    ingestByTags.add(tag)
    val sp = new SparkIngestionProperties()
    sp.ingestByTags = ingestByTags
    sp.creationTime = Instant.now(Clock.systemUTC())

    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoConnectionOptions.accessToken)
      .option(KustoSinkOptions.KUSTO_CLIENT_REQUEST_PROPERTIES_JSON, crp.toString)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .option(KustoDebugOptions.KUSTO_ENSURE_NO_DUPLICATED_BLOBS, true.toString)
      .option(KustoDebugOptions.KUSTO_DISABLE_FLUSH_IMMEDIATELY, true.toString)
      .option(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON, sp.toString)
      .mode(SaveMode.Append)
      .save()

    val instant = Instant.now.plus(1, ChronoUnit.HOURS)
    kustoAdminClient.get.execute(
      kustoConnectionOptions.database,
      generateTableAlterAutoDeletePolicy(table, instant))

    val conf: Map[String, String] =
      Map(KustoSinkOptions.KUSTO_ACCESS_TOKEN -> kustoConnectionOptions.accessToken)
    validateRead(conf)
  }

  val minimalParquetWriterVersion: String = "3.3.0"
  private def validateRead(conf: Map[String, String]) = {
    val dfResult = spark.read.kusto(
      kustoConnectionOptions.cluster,
      kustoConnectionOptions.database,
      table,
      conf)
    val orig = dfOrig
      .select("name", "value", "dec")
      .rdd
      .map(x => (x.getString(0), x.getInt(1), x.getDecimal(2)))
      .collect()
      .sortBy(_._2)
    val result = dfResult
      .select("name", "value", "dec")
      .rdd
      .map(x => (x.getString(0), x.getInt(1), x.getDecimal(2)))
      .collect()
      .sortBy(_._2)
    assert(orig.deep == result.deep)
  }

  "KustoSource" should "execute a read query on Kusto cluster in single mode" in {
    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceSingleMode.toString,
      KustoSourceOptions.KUSTO_ACCESS_TOKEN -> kustoConnectionOptions.accessToken)
    validateRead(conf)
  }

  "KustoSource" should "execute a read query on Kusto cluster in distributed mode" in {
    maybeKustoDmClient match {
      case Some(kustoIngestClient) =>
        val storageWithKey = kustoIngestClient
          .execute(kustoConnectionOptions.database, generateGetExportContainersCommand())
          .getPrimaryResults
          .getData
          .get(0)
          .get(0)
          .toString
        KDSU.logDebug(className, s"storageWithKey: $storageWithKey")

        val storage =
          new TransientStorageParameters(Array(new TransientStorageCredentials(storageWithKey)))

        val conf: Map[String, String] = Map(
          KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceDistributedMode.toString,
          KustoSourceOptions.KUSTO_TRANSIENT_STORAGE -> storage.toInsecureString,
          KustoSourceOptions.KUSTO_ACCESS_TOKEN -> kustoConnectionOptions.accessToken)
        val supportNewParquetWriter = new ComparableVersion(spark.version)
          .compareTo(new ComparableVersion(minimalParquetWriterVersion)) > 0
        if (supportNewParquetWriter) {
          validateRead(conf)
        } else {
          val dfResult = spark.read.kusto(
            kustoConnectionOptions.cluster,
            kustoConnectionOptions.database,
            table,
            conf)
          assert(dfResult.count() == expectedNumberOfRows)
        }
      case None =>
        KDSU.logError(className, s"DM client is null & tests are skipped")
        fail
    }
  }

  // TODO make this UT
  "KustoSource" should "read distributed, transient cache change the filter but execute once" taggedAs KustoE2E in {
    import spark.implicits._
    val table = KustoQueryUtils.simplifyName(s"KustoSparkReadWriteTest_${UUID.randomUUID()}")

    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceDistributedMode.toString,
      KustoSourceOptions.KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE -> true.toString,
      KustoSourceOptions.KUSTO_ACCESS_TOKEN -> kustoConnectionOptions.accessToken)

    // write
    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoConnectionOptions.accessToken)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    val df = spark.read.kusto(
      kustoConnectionOptions.cluster,
      kustoConnectionOptions.database,
      table,
      conf)

    val time = Instant.now()
    assert(df.count() == expectedNumberOfRows)
    assert(df.count() == expectedNumberOfRows)

    val df2 = df.where($"value".cast("Int") > 50)
    assert(df2.collect().length == 50)

    // Should take up to another 10 seconds for .show commands to come up
    Thread.sleep(5000 * 60)
    val res3 = kustoAdminClient.get.execute(
      s""".show commands | where StartedOn > datetime(${time.toString})  | where
                                        CommandType ==
      "DataExportToFile" | where Text has "$table"""")
    if (res3.getPrimaryResults.count() == 0) {
      KDSU.logWarn("", "")
    } else {
      assert(res3.getPrimaryResults.count() == 1)
    }
  }
}

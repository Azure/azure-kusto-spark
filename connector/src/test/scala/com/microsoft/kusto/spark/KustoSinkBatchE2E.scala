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
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SinkTableCreationMode}
import com.microsoft.kusto.spark.datasource.{KustoSourceOptions, ReadMode}
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.math.{BigDecimal, RoundingMode}
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable

class KustoSinkBatchE2E extends AnyFlatSpec with BeforeAndAfterAll {
  private val myName = this.getClass.getSimpleName
  private val rowId = new AtomicInteger(1)
  private val rowId2 = new AtomicInteger(1)

  private def newRow(): String = s"row-${rowId.getAndIncrement()}"
  private def newAllDataTypesRow(v: Int): (
      String,
      Int,
      java.sql.Date,
      Boolean,
      Short,
      Byte,
      Float,
      java.sql.Timestamp,
      Double,
      java.math.BigDecimal,
      Long) = {
    val longie = 80000000 + v.toLong * 100000000
    (
      s"row-${rowId2.getAndIncrement()}",
      v,
      new java.sql.Date(longie),
      v % 2 == 0,
      v.toShort,
      v.toByte,
      1 / v.toFloat,
      if (v % 10 == 0) null else new java.sql.Timestamp(longie),
      1 / v.toDouble,
      BigDecimal.valueOf(1 / v.toDouble),
      v)
  }

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

  val appId: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_ID)
  val appKey: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_SECRET)
  val authority: String =
    System.getProperty(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com")
  val cluster: String = System.getProperty(KustoSinkOptions.KUSTO_CLUSTER)
  val database: String = System.getProperty(KustoSinkOptions.KUSTO_DATABASE)
  val expectedNumberOfRows: Int = 1 * 1000
  val rows: immutable.IndexedSeq[(String, Int)] =
    (1 to expectedNumberOfRows).map(v => (newRow(), v))

  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  "KustoConnectorLogger" should "log to console according to the level set" taggedAs KustoE2E in {
    KDSU.logInfo(myName, "******** info ********")
    KDSU.logError(myName, "******** error ********")
    KDSU.logFatal(
      myName,
      "******** fatal  ********. Use a 'logLevel' system variable to change the logging level.")
  }

  "KustoBatchSinkDataTypesTest" should "ingest structured data of all types to a Kusto cluster" taggedAs KustoE2E in {
    import spark.implicits._

    val prefix = "KustoBatchSinkDataTypesTest_Ingest"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")

    val dataTypesRows: immutable.IndexedSeq[(
        String,
        Int,
        java.sql.Date,
        Boolean,
        Short,
        Byte,
        Float,
        java.sql.Timestamp,
        Double,
        java.math.BigDecimal,
        Long)] = (1 to expectedNumberOfRows).map(v => newAllDataTypesRow(v))
    val dfOrig = dataTypesRows.toDF(
      "String",
      "Int",
      "Date",
      "Boolean",
      "short",
      "Byte",
      "floatie",
      "Timestamp",
      "Double",
      "BigDecimal",
      "Long")
    sqlContext.sql("select current_timestamp()").show(false)

    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoSinkOptions.KUSTO_CLUSTER, cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, appId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, appKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .option(DateTimeUtils.TIMEZONE_OPTION, "GMT+4")
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    val conf: Map[String, String] = Map(
      KustoSinkOptions.KUSTO_AAD_APP_ID -> appId,
      KustoSinkOptions.KUSTO_AAD_APP_SECRET -> appKey,
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceDistributedMode.toString)

    val conf2: Map[String, String] = Map(
      KustoSinkOptions.KUSTO_AAD_APP_ID -> appId,
      KustoSinkOptions.KUSTO_AAD_APP_SECRET -> appKey,
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceSingleMode.toString)

    val dfResultDist: DataFrame = spark.read.kusto(cluster, database, table, conf)
    val dfResultSingle: DataFrame = spark.read.kusto(cluster, database, table, conf2)

    def getRowOriginal(x: Row): (
        String,
        Int,
        Date,
        Boolean,
        Short,
        Byte,
        Float,
        Timestamp,
        Double,
        BigDecimal,
        Long) = {
      (
        x.getString(0),
        x.getInt(1),
        x.getDate(2),
        x.getBoolean(3),
        x.getShort(4),
        x.getByte(5),
        x.getFloat(6),
        x.getTimestamp(7),
        x.getDouble(8),
        x.getDecimal(9),
        x.getLong(10))
    }

    def getRowFromKusto(x: Row): (
        String,
        Int,
        Timestamp,
        Boolean,
        Short,
        Byte,
        Float,
        Timestamp,
        Double,
        BigDecimal,
        Long) = {
      (
        x.getString(0),
        x.getInt(1),
        x.getTimestamp(2),
        x.getBoolean(3),
        x.getInt(4).toShort,
        x.getInt(5).toByte,
        x.getDouble(6).toFloat,
        x.getTimestamp(7),
        x.getDouble(8),
        x.getDecimal(9),
        x.getLong(10))
    }

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    def compareRowsWithTimestamp(
        row1: (
            String,
            Int,
            Date,
            Boolean,
            Short,
            Byte,
            Float,
            Timestamp,
            Double,
            BigDecimal,
            Long),
        row2: (
            String,
            Int,
            Timestamp,
            Boolean,
            Short,
            Byte,
            Float,
            Timestamp,
            Double,
            BigDecimal,
            Long)): Boolean = {
      val myValA = row2._10.setScale(2, RoundingMode.HALF_UP)
      val myValB = row1._10.setScale(2, RoundingMode.HALF_UP)

      var res = dateFormat.format(row2._3.getTime) == row1._3.toString &&
        Math.abs(row2._9 - row1._9) < 0.00000000000001 && myValA.compareTo(myValB) == 0

      val it1 = row1.productIterator
      val it2 = row2.productIterator
      while (it1.hasNext) {
        val v1 = it1.next()
        val v2 = it2.next()
        if (v1 != v2 && !v1.isInstanceOf[Date] && !v2.isInstanceOf[BigDecimal]) {
          res = false
        }
      }
      res
    }

    val orig = dfOrig
      .select(
        "String",
        "Int",
        "Date",
        "Boolean",
        "short",
        "Byte",
        "floatie",
        "Timestamp",
        "Double",
        "BigDecimal",
        "Long")
      .rdd
      .map(x => getRowOriginal(x))
      .collect()
      .sortBy(_._2)
    val result = dfResultDist
      .select(
        "String",
        "Int",
        "Date",
        "Boolean",
        "short",
        "Byte",
        "floatie",
        "Timestamp",
        "Double",
        "BigDecimal",
        "Long")
      .rdd
      .map(x => getRowFromKusto(x))
      .collect()
      .sortBy(_._2)
    val resultSingle = dfResultSingle
      .select(
        "String",
        "Int",
        "Date",
        "Boolean",
        "short",
        "Byte",
        "floatie",
        "Timestamp",
        "Double",
        "BigDecimal",
        "Long")
      .rdd
      .map(x => getRowFromKusto(x))
      .collect()
      .sortBy(_._2)

    var isEqual = true
    for (idx <- orig.indices) {
      if (!compareRowsWithTimestamp(orig(idx), result(idx)) || !compareRowsWithTimestamp(
          orig(idx),
          resultSingle(idx))) {
        isEqual = false
      }
    }

    assert(isEqual)
    KDSU.logInfo(
      myName,
      s"KustoBatchSinkDataTypesTest: Ingestion results validated for table '$table'")

    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://$cluster.kusto.windows.net",
      appId,
      appKey,
      authority)
    val engineClient = ClientFactory.createClient(engineKcsb)
    KustoTestUtils.tryDropAllTablesByPrefix(engineClient, database, prefix)
  }

  "KustoBatchSinkSync" should "also ingest simple data to a Kusto cluster" taggedAs KustoE2E in {
    import spark.implicits._
    val df = rows.toDF("name", "value")
    val prefix = "KustoBatchSinkE2E_Ingest"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://$cluster.kusto.windows.net",
      appId,
      appKey,
      authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(
      database,
      generateTempTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoSinkOptions.KUSTO_CLUSTER, cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, appId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, appKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .option(KustoSinkOptions.KUSTO_TIMEOUT_LIMIT, (8 * 60).toString)
      .mode(SaveMode.Append)
      .save()

    val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      database,
      expectedNumberOfRows,
      timeoutMs,
      tableCleanupPrefix = prefix)
  }

  "KustoBatchSinkAsync" should "ingest structured data to a Kusto cluster in async mode" taggedAs KustoE2E in {
    import spark.implicits._
    val df = rows.toDF("name", "value")
    val prefix = "KustoBatchSinkE2EIngestAsync"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://$cluster.kusto.windows.net",
      appId,
      appKey,
      authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(
      database,
      generateTempTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, appId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, appKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .option(KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC, "true")
      .mode(SaveMode.Append)
      .save()

    val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      database,
      expectedNumberOfRows,
      timeoutMs,
      tableCleanupPrefix = prefix)
  }
}

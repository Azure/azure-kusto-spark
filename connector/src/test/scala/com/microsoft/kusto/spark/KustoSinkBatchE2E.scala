// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.KustoTestUtils.getSystemTestOptions
import com.microsoft.kusto.spark.common.KustoDebugOptions
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.{CreateIfNotExist, FailIfNotExist}
import com.microsoft.kusto.spark.datasink.{
  IngestionStorageParameters,
  KustoSinkOptions,
  SchemaAdjustmentMode,
  SinkTableCreationMode,
  WriteMode
}
import com.microsoft.kusto.spark.datasource.{KustoSourceOptions, ReadMode}
import com.microsoft.kusto.spark.sql.extension.SparkExtension.DataFrameReaderExtension
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.{
  generateTableAlterStreamIngestionCommand,
  generateTableCreateCommand,
  generateTempTableCreateCommand
}
import com.microsoft.kusto.spark.utils.{
  KustoConstants,
  KustoQueryUtils,
  KustoDataSourceUtils => KDSU
}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions.lit
import org.awaitility.Awaitility
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.Tables.Table

import java.{lang, util}
import java.math.{BigDecimal, RoundingMode}
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors
import scala.collection.immutable
import scala.concurrent.duration.SECONDS

class KustoSinkBatchE2E extends AnyFlatSpec with BeforeAndAfterAll {
  private val className = this.getClass.getSimpleName
  private val rowId = new AtomicInteger(1)
  private val rowId2 = new AtomicInteger(1)
  private val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes
  private val sleepTimeTillTableCreate: Int = 3 * 60 * 1000 // 2 minutes
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

  private lazy val kustoTestConnectionOptions = getSystemTestOptions
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

  val expectedNumberOfRows: Int = 1 * 1000
  val rows: immutable.IndexedSeq[(String, Int)] =
    (1 to expectedNumberOfRows).map(v => (newRow(), v))

  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  "KustoConnectorLogger" should "log to console according to the level set" taggedAs KustoE2E in {
    KDSU.logInfo(className, "******** info ********")
    KDSU.logError(className, "******** error ********")
    KDSU.logFatal(
      className,
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
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(DateTimeUtils.TIMEZONE_OPTION, "GMT+4")
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    val conf: Map[String, String] = Map(
      KustoSinkOptions.KUSTO_ACCESS_TOKEN -> kustoTestConnectionOptions.accessToken,
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceDistributedMode.toString)

    val conf2: Map[String, String] = Map(
      KustoSinkOptions.KUSTO_ACCESS_TOKEN -> kustoTestConnectionOptions.accessToken,
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceSingleMode.toString)

    val dfResultDist: DataFrame = spark.read.kusto(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.database,
      table,
      conf)
    val dfResultSingle: DataFrame = spark.read.kusto(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.database,
      table,
      conf2)

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
      className,
      s"KustoBatchSinkDataTypesTest: Ingestion results validated for table '$table'")

    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val engineClient = ClientFactory.createClient(engineKcsb)
    KustoTestUtils.tryDropAllTablesByPrefix(
      engineClient,
      kustoTestConnectionOptions.database,
      prefix)
  }

  import spark.implicits._

  private val ingestTests =
    Table("testName", "DMStorage", "CustomStorage")

  TableDrivenPropertyChecks.forAll(ingestTests) { testName =>
    "KustoBatchSinkSync" should s"also ingest simple data to a Kusto cluster for $testName" in {
      import spark.implicits._
      var skipAssertions = false
      val df = rows.toDF("name", "value")
      val prefix = s"KustoBatchSinkE2E_Ingest_$testName"
      val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
      val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
        kustoTestConnectionOptions.cluster,
        kustoTestConnectionOptions.accessToken)
      val kustoAdminClient = ClientFactory.createClient(engineKcsb)
      kustoAdminClient.execute(
        kustoTestConnectionOptions.database,
        generateTempTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))

      if (testName == "DMStorage") {
        df.write
          .format("com.microsoft.kusto.spark.datasource")
          .partitionBy("value")
          .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
          .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
          .option(KustoSinkOptions.KUSTO_TABLE, table)
          .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
          .option(KustoSinkOptions.KUSTO_TIMEOUT_LIMIT, (8 * 60).toString)
          .mode(SaveMode.Append)
          .save()
      } else {
        // Use custom storage
        val storageUrl = KustoTestUtils.getSystemVariable("ingestStorageUrl")
        val containerName = KustoTestUtils.getSystemVariable("ingestStorageContainer")
        if (StringUtils.isEmpty(storageUrl) || StringUtils.isEmpty(containerName)) {
          skipAssertions = true
          KDSU.logWarn(
            className,
            "No ingestion storage URL or container name provided. Skipping ingestion test.")
        } else {
          KDSU.logInfo(className, s"Using ingestion storage container: $containerName")
          val ingestionStorageString = IngestionStorageParameters.toJsonString(
            Array(new IngestionStorageParameters(storageUrl, containerName, "", "")))
//            s"""[{"storageUrl":"$storageUrl" , "
//              |containerName": "$containerName"}]""".stripMargin
          df.write
            .format("com.microsoft.kusto.spark.datasource")
            .partitionBy("value")
            .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
            .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
            .option(KustoSinkOptions.KUSTO_TABLE, table)
            .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
            .option(KustoSinkOptions.KUSTO_INGESTION_STORAGE, ingestionStorageString)
            .option(KustoSinkOptions.KUSTO_TIMEOUT_LIMIT, (8 * 60).toString)
            .mode(SaveMode.Append)
            .save()
        }
      }

      if (!skipAssertions) {
        KustoTestUtils.validateResultsAndCleanup(
          kustoAdminClient,
          table,
          kustoTestConnectionOptions.database,
          expectedNumberOfRows,
          timeoutMs,
          tableCleanupPrefix = prefix)
      }
    }
  }

  "KustoBatchSinkAsync" should "ingest structured data to a Kusto cluster in async mode" taggedAs KustoE2E in {
    val df = rows.toDF("name", "value")
    val prefix = "KustoBatchSinkE2EIngestAsync"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(
      kustoTestConnectionOptions.database,
      generateTempTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC, "true")
      .mode(SaveMode.Append)
      .save()

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      tableCleanupPrefix = prefix)
  }

  "KustoBatchSinkStreaming" should "ingest structured data to a Kusto cluster in stream ingestion mode" taggedAs KustoE2E in {
    val df = rows.toDF("name", "value")
    val prefix = "KustoBatchSinkE2EIngestStreamIngestion"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(
      kustoTestConnectionOptions.database,
      generateTempTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))
    kustoAdminClient.execute(
      kustoTestConnectionOptions.database,
      generateTableAlterStreamIngestionCommand(table))

    Thread.sleep(sleepTimeTillTableCreate)

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
      .option(KustoSinkOptions.KUSTO_WRITE_MODE, "KustoStreaming")
      .mode(SaveMode.Append)
      .save()

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      kustoTestConnectionOptions.database,
      expectedNumberOfRows,
      timeoutMs,
      tableCleanupPrefix = prefix)
  }

  private val schemaAdjustmentModes =
    Table(
      ("targetColumnExists", "schemaAdjustmentMode", "writeMode", "createTable"),
      (
        true,
        SchemaAdjustmentMode.GenerateDynamicCsvMapping,
        WriteMode.Transactional,
        FailIfNotExist),
      (true, SchemaAdjustmentMode.GenerateDynamicCsvMapping, WriteMode.Queued, FailIfNotExist),
      (
        false,
        SchemaAdjustmentMode.GenerateDynamicCsvMapping,
        WriteMode.Transactional,
        FailIfNotExist),
      (false, SchemaAdjustmentMode.GenerateDynamicCsvMapping, WriteMode.Queued, FailIfNotExist),
      (
        true,
        SchemaAdjustmentMode.GenerateDynamicCsvMapping,
        WriteMode.Transactional,
        CreateIfNotExist
      ), // the first param is not relevant here
      (
        true,
        SchemaAdjustmentMode.GenerateDynamicCsvMapping,
        WriteMode.Queued,
        CreateIfNotExist
      ) // the first param is not relevant here
    )

  TableDrivenPropertyChecks.forEvery(schemaAdjustmentModes) {
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    (targetColumnExists, schemaAdjustmentMode, writeMode, tableCreationMode) => {
      "KustoWrite" should s"check for matrix of tests when using SchemaAdjustment: ${schemaAdjustmentMode.toString}, " +
        s"TargetColumnExists: $targetColumnExists , WriteMode: $writeMode and TableCreateMode: $tableCreationMode" in {
          val testName =
            s"${schemaAdjustmentMode.toString.substring(0, 3)}_${writeMode.toString.substring(0, 3)}"
          val df = rows.toDF("name", "value").withColumn("WriteMode", lit(writeMode.toString))
          val prefix = s"KustoBatchSinkE2E_Ingest_$testName"
          val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")

          val columnDefinition = if (targetColumnExists) {
            s"name:string, value:int, ${KustoConstants.SourceLocationColumnName}:string, WriteMode:string"
          } else {
            "name:string, value:int, WriteMode:string"
          }
          if (tableCreationMode != SinkTableCreationMode.CreateIfNotExist) {
            kustoAdminClient.execute(
              kustoTestConnectionOptions.database,
              generateTableCreateCommand(table, columnsTypesAndNames = columnDefinition))
          }

          KDSU.logInfo(
            className,
            s"TableName:: $table. Running test: $testName with schema adjustment mode: $schemaAdjustmentMode")
          if (!targetColumnExists) {
            intercept[Exception] {
              df.write
                .format("com.microsoft.kusto.spark.datasource")
                .partitionBy("value")
                .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
                .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
                .option(KustoSinkOptions.KUSTO_TABLE, table)
                .option(
                  KustoSinkOptions.KUSTO_ACCESS_TOKEN,
                  kustoTestConnectionOptions.accessToken)
                .option(KustoSinkOptions.KUSTO_ADJUST_SCHEMA, schemaAdjustmentMode.toString)
                .option(KustoDebugOptions.KUSTO_ADD_SOURCE_LOCATION_TRANSFORM, "true")
                .option(KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS, tableCreationMode.toString)
                .option(KustoSinkOptions.KUSTO_WRITE_MODE, writeMode.toString)
                .option(KustoSinkOptions.KUSTO_TIMEOUT_LIMIT, (8 * 60).toString)
                .mode(SaveMode.Append)
                .save()
            }
            val query = if (targetColumnExists) {
              s"$table | where isnotempty(${KustoConstants.SourceLocationColumnName}) | summarize Count=count() by ${KustoConstants.SourceLocationColumnName}"
            } else {
              s"$table | summarize Count=count() by WriteMode"
            }
            val queryResults = kustoAdminClient
              .executeQuery(kustoTestConnectionOptions.database, query)
              .getPrimaryResults
              .getData
            assert(queryResults.isEmpty, s"Expected no results for query: $query")
          } else {
            df.write
              .format("com.microsoft.kusto.spark.datasource")
              .partitionBy("value")
              .option(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
              .option(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
              .option(KustoSinkOptions.KUSTO_TABLE, table)
              .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
              .option(
                KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
                SinkTableCreationMode.CreateIfNotExist.toString)
              .option(KustoSinkOptions.KUSTO_ADJUST_SCHEMA, schemaAdjustmentMode.toString)
              .option(KustoDebugOptions.KUSTO_ADD_SOURCE_LOCATION_TRANSFORM, "true")
              .option(KustoSinkOptions.KUSTO_WRITE_MODE, writeMode.toString)
              .option(KustoSinkOptions.KUSTO_TIMEOUT_LIMIT, (8 * 60).toString)
              .mode(SaveMode.Append)
              .save()
            val query = if (targetColumnExists) {
              s"$table | where isnotempty(${KustoConstants.SourceLocationColumnName}) " +
                "and isnotempty(name) and value > 0  and isnotempty(WriteMode)" +
                s"| summarize Count=count() by ${KustoConstants.SourceLocationColumnName}"
            } else {
              s"$table | summarize Count=count() by WriteMode"
            }
            val nonEmptyResult =
              (res: Map[String, Long]) => res.values.sum == expectedNumberOfRows

            val totalRows = Awaitility
              .await()
              .atMost(sleepTimeTillTableCreate, TimeUnit.MILLISECONDS)
              .until(
                () => {
                  kustoAdminClient
                    .executeQuery(kustoTestConnectionOptions.database, query)
                    .getPrimaryResults
                    .getData
                    .toArray()
                    .map {
                      // check if row is a util.List
                      case list: java.util.ArrayList[_] =>
                        val key = list.get(0).toString
                        val value = list.get(1).toString.toLong
                        (key, value)
                    }
                    .toMap
                },
                res => nonEmptyResult(res))
            assert(
              totalRows.values.sum == expectedNumberOfRows,
              s"Expected $expectedNumberOfRows rows, but got $totalRows")
            assert(
              totalRows.keys.count(blobUrl => blobUrl.startsWith("https://")) > 0,
              s"Expected all rows to have a blob URL, but got: ${totalRows.keys}")
          }
          kustoAdminClient.executeMgmt(kustoTestConnectionOptions.database, s".drop table $table")
        }
    }
  }
}

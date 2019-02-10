package com.microsoft.kusto.spark

import java.math.{BigDecimal, RoundingMode}
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.microsoft.azure.kusto.data.{ClientFactory, ConnectionStringBuilder}
import com.microsoft.kusto.spark.datasource.KustoOptions
import com.microsoft.kusto.spark.datasource.KustoOptions.SinkTableCreationMode
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.collection.immutable

@RunWith(classOf[JUnitRunner])
class KustoSinkBatchE2E extends FlatSpec with BeforeAndAfterAll{
  private val myName = this.getClass.getSimpleName
  private val rowId = new AtomicInteger(1)
  private val rowId2 = new AtomicInteger(1)

  private def newRow(): String = s"row-${rowId.getAndIncrement()}"
  private def newAllDataTypesRow(v: Int): (String, Int, java.sql.Date, Boolean, Short, Byte, Float ,java.sql.Timestamp, Double, java.math.BigDecimal, Long) ={
    val longie = 80000000 + v.toLong * 100000000
    (s"row-${rowId2.getAndIncrement()}", v, new java.sql.Date(longie), v % 2 == 0, v.toShort, v.toByte,
                1/v.toFloat, new java.sql.Timestamp(longie), 1/v.toDouble, BigDecimal.valueOf(1/v.toDouble), v)
  }


  private val nofExecutors = 4
  private val spark: SparkSession = SparkSession.builder()
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

  val appId: String = System.getProperty(KustoOptions.KUSTO_AAD_CLIENT_ID)
  val appKey: String = System.getProperty(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD)
  val authority: String = System.getProperty(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com")
  val cluster: String = System.getProperty(KustoOptions.KUSTO_CLUSTER)
  val database: String = System.getProperty(KustoOptions.KUSTO_DATABASE)
  val expectedNumberOfRows: Int =  1* 1000 * 1000
  val rows: immutable.IndexedSeq[(String, Int)] = (1 to expectedNumberOfRows).map(v => (newRow(), v))

  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  "KustoConnectorLogger" should "log to console according to the level set" taggedAs KustoE2E in {
    KDSU.logInfo(myName, "******** info ********")
    KDSU.logError(myName, "******** error ********")
    KDSU.logFatal(myName,"******** fatal  ********. Use a 'logLevel' system variable to change the logging level.")
  }

  "KustoBatchSinkDataTypesTest" should "ingest structured data to a Kusto cluster" taggedAs KustoE2E in {
    import spark.implicits._

    val prefix = "KustoBatchSinkDataTypesTest_Ingest"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")

    val dataTypesRows: immutable.IndexedSeq[(String, Int, java.sql.Date, Boolean, Short, Byte, Float ,java.sql.Timestamp, Double, java.math.BigDecimal, Long)] = (1 to expectedNumberOfRows).map(v => newAllDataTypesRow(v))
    val dfOrig = dataTypesRows.toDF("String", "Int", "Date", "Boolean", "short", "Byte", "floatie", "Timestamp", "Double", "BigDecimal", "Long")

    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoOptions.KUSTO_CLUSTER, cluster)
      .option(KustoOptions.KUSTO_DATABASE, database)
      .option(KustoOptions.KUSTO_TABLE, table)
      .option(KustoOptions.KUSTO_AAD_CLIENT_ID, appId)
      .option(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, appKey)
      .option(KustoOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .option(DateTimeUtils.TIMEZONE_OPTION, "GMT+4")
      .option(KustoOptions.KUSTO_TABLE_CREATE_OPTIONS, SinkTableCreationMode.CreateIfNotExist.toString)
      .save()

    val conf: Map[String, String] = Map(
      KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
      KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey
    )

    val dfResult: DataFrame = spark.read.kusto(cluster, database, table, conf)
    def getRowOriginal(x:Row): (String, Int, Date, Boolean, Short, Byte, Float, Timestamp, Double, BigDecimal, Long) ={
      (x.getString(0), x.getInt(1),x.getDate(2),x.getBoolean(3),x.getShort(4),x.getByte(5),x.getFloat(6),x.getTimestamp(7),x.getDouble(8),x.getDecimal(9),x.getLong(10))
    }

    def getRowFromKusto(x:Row): (String, Int, Timestamp, Boolean, Short, Byte, Float, Timestamp, Double, BigDecimal, Long) ={
      (x.getString(0), x.getInt(1),x.getTimestamp(2),x.getBoolean(3),x.getInt(4).toShort, x.getInt(5).toByte,x.getDouble(6).toFloat,x.getTimestamp(7),x.getDouble(8),x.getDecimal(9),x.getLong(10))
    }

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    def compareRowsWithTimestamp(row1:(String, Int, Date, Boolean, Short, Byte, Float, Timestamp, Double, BigDecimal, Long), row2:(String, Int, Timestamp, Boolean, Short, Byte, Float, Timestamp, Double, BigDecimal, Long)): Boolean ={
      val myValA = row2._10.setScale(2, RoundingMode.HALF_UP)
      val myValB = row1._10.setScale(2, RoundingMode.HALF_UP)

      var res = dateFormat.format(row2._3.getTime)  == row1._3.toString &&
          Math.abs( row2._9 - row1._9) < 0.00000000000001 && myValA.compareTo(myValB) == 0

      val it1 = row1.productIterator
      val it2 = row2.productIterator
      while (it1.hasNext){
        val v1 = it1.next()
        val v2 = it2.next()
        if(v1 != v2 && !v1.isInstanceOf[Date] && !v2.isInstanceOf[BigDecimal]){
          res = false
        }
      }
      res
    }

    val check= dfResult.filter(x=>x.get(1) == 1)

    val orig = dfOrig.select("String", "Int", "Date", "Boolean", "short", "Byte", "floatie", "Timestamp", "Double", "BigDecimal", "Long").rdd.map(x => getRowOriginal(x)).collect().sortBy(_._2)
    val result = dfResult.select("String", "Int", "Date", "Boolean", "short", "Byte", "floatie", "Timestamp", "Double", "BigDecimal", "Long").rdd.map(x => getRowFromKusto(x)).collect().sortBy(_._2)

    var isEqual = true
    for(idx <- orig.indices) {
      if(!compareRowsWithTimestamp(orig(idx),result(idx))){
        isEqual = false
      }
    }
    assert(isEqual)
    KDSU.logInfo(myName, s"KustoBatchSinkDataTypesTest: Ingestion results validated for table '$table'")
  }

    "KustoBatchSinkSync" should "ingest structured data to a Kusto cluster" taggedAs KustoE2E in {
    import spark.implicits._
    val df = rows.toDF("name", "value")
    val prefix = "KustoBatchSinkE2E_Ingest"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(database, generateTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoOptions.KUSTO_CLUSTER, cluster)
      .option(KustoOptions.KUSTO_DATABASE, database)
      .option(KustoOptions.KUSTO_TABLE, table)
      .option(KustoOptions.KUSTO_AAD_CLIENT_ID, appId)
      .option(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, appKey)
      .option(KustoOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .save()

    val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes

    KustoTestUtils.validateResultsAndCleanup(kustoAdminClient, table, database, expectedNumberOfRows, timeoutMs, tableCleanupPrefix = prefix)
  }

  "KustoBatchSinkAsync" should "ingest structured data to a Kusto cluster" taggedAs KustoE2E in {
    import spark.implicits._
    val df = rows.toDF("name", "value")
    val prefix = "KustoBatchSinkE2EIngestAsync"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(database, generateTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))
    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoOptions.KUSTO_CLUSTER, cluster)
      .option(KustoOptions.KUSTO_DATABASE, database)
      .option(KustoOptions.KUSTO_TABLE, table)
      .option(KustoOptions.KUSTO_AAD_CLIENT_ID, appId)
      .option(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, appKey)
      .option(KustoOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .option(KustoOptions.KUSTO_WRITE_ENABLE_ASYNC, "true")
      .save()

    val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes

    KustoTestUtils.validateResultsAndCleanup(kustoAdminClient, table, database, expectedNumberOfRows, timeoutMs, tableCleanupPrefix = prefix)
  }
}
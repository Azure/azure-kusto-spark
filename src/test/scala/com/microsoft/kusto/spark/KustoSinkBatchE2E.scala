package com.microsoft.kusto.spark

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.microsoft.azure.kusto.data.{ClientFactory, ConnectionStringBuilder}
import com.microsoft.kusto.spark.datasource.KustoDataSourceUtils.generateTableCreateCommand
import com.microsoft.kusto.spark.datasource.{KustoOptions, KustoDataSourceUtils => KDSU}
import com.microsoft.kusto.spark.utils.KustoQueryUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DataTypes.IntegerType
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.collection.immutable

@RunWith(classOf[JUnitRunner])
class KustoSinkBatchE2E extends FlatSpec with BeforeAndAfterAll{
  private val myName = this.getClass.getSimpleName
  private val rowId = new AtomicInteger(1)
  private def newRow(): String = s"row-${rowId.getAndIncrement()}"

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
  val customSchema = StructType(StructField("colA", StringType, nullable = true) :: StructField("colB", IntegerType, nullable = true) :: Nil)
  val expectedNumberOfRows: Int =  1* 1000 * 1000
  val rows: immutable.IndexedSeq[(String, Int)] = (1 to expectedNumberOfRows).map(v => (newRow(), v))

  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  "KustoConnectorLogger" should "log to console according to the level set" taggedAs KustoE2E in {
    KDSU.logInfo(myName, "******** info ********")
    KDSU.logError(myName, "******** error ********")
    KDSU.logFatal(myName,"******** fatal  ********. Use a 'logLevel' system variable to change the logging level.")
  }

  "KustoBatchSinkSync" should "ingest structured data to a Kusto cluster" taggedAs KustoE2E in {
    import spark.implicits._
    val df = rows.toDF("name", "value")
    val prefix = "KustoBatchSinkE2E_Ingest"
    val table = KustoQueryUtils.simplifyTableName(s"${prefix}_${UUID.randomUUID()}")
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
    val table = KustoQueryUtils.simplifyTableName(s"${prefix}_${UUID.randomUUID()}")
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
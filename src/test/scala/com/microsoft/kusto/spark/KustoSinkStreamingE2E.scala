package com.microsoft.kusto.spark

import java.util.UUID

import com.microsoft.azure.kusto.data.{ClientFactory, ConnectionStringBuilder}
import com.microsoft.kusto.spark.datasource.KustoDataSourceUtils._
import com.microsoft.kusto.spark.datasource.KustoOptions
import com.microsoft.kusto.spark.datasource.KustoOptions.SinkTableCreationMode
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DataTypes.IntegerType
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

@RunWith(classOf[JUnitRunner])
class KustoSinkStreamingE2E extends FlatSpec with BeforeAndAfterAll{
  val expectedNumberOfRows: Int = 100
  val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes
  val sleepTimeTillTableCreate: Int = 2 * 60 * 1000 // 2 minutes
  val spark: SparkSession = SparkSession.builder()
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

  val appId: String = System.getProperty(KustoOptions.KUSTO_AAD_CLIENT_ID)
  val appKey: String = System.getProperty(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD)
  val authority: String = System.getProperty(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com")
  val cluster: String = System.getProperty(KustoOptions.KUSTO_CLUSTER)
  val database: String = System.getProperty(KustoOptions.KUSTO_DATABASE)

  val csvPath: String = System.getProperty("path", "src/test/resources/TestData")
  val customSchema: StructType = new StructType().add("colA", StringType, nullable = true).add("colB", IntegerType, nullable = true)

  "KustoStreamingSinkSyncWithTableCreate" should "ingest structured data to a Kusto cluster" taggedAs KustoE2E in {

    if(appId == null || appKey == null || authority == null || cluster == null || database == null){
      fail()
    }

    val prefix = "KustoStreamingSparkE2E_Ingest"
    val table = s"${prefix}_${UUID.randomUUID().toString.replace("-","_")}"
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    val csvDf = spark
      .readStream
      .schema(customSchema)
      .csv(csvPath)

    val consoleQ = csvDf
      .writeStream
      .format("console")
      .trigger(Trigger.Once)

    consoleQ.start().awaitTermination()

    spark.conf.set("spark.sql.streaming.checkpointLocation", "target/temp/checkpoint")

    val kustoQ = csvDf
      .writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .options(Map(
        KustoOptions.KUSTO_CLUSTER -> cluster,
        KustoOptions.KUSTO_TABLE -> table,
        KustoOptions.KUSTO_DATABASE -> database,
        KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
        KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
        KustoOptions.KUSTO_AAD_AUTHORITY_ID -> authority,
        KustoOptions.KUSTO_TABLE_CREATE_OPTIONS -> SinkTableCreationMode.CreateIfNotExist.toString))
      .trigger(Trigger.Once)

    kustoQ.start().awaitTermination()

    // Sleep util table is expected to be created
    Thread.sleep(sleepTimeTillTableCreate)
    KustoTestUtils.validateResultsAndCleanup(kustoAdminClient, table, database, expectedNumberOfRows, timeoutMs - sleepTimeTillTableCreate, tableCleanupPrefix = prefix)
  }

  // "KustoStreamingSinkAsync"
  ignore should "also ingest structured data to a Kusto cluster" taggedAs KustoE2E in {

    if(appId == null || appKey == null || authority == null || cluster == null || database == null){
      fail()
    }

    val prefix = "KustoStreamingSparkE2EAsync_Ingest"
    val table = s"${prefix}_${UUID.randomUUID().toString.replace("-","_")}"
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    kustoAdminClient.execute(database, generateTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))

    val csvDf = spark
      .readStream
      .schema(customSchema)
      .csv(csvPath)

    val consoleQ = csvDf
      .writeStream
      .format("console")
      .trigger(Trigger.Once)

    consoleQ.start().awaitTermination()

    spark.conf.set("spark.sql.streaming.checkpointLocation", "target/temp/checkpoint")

    val kustoQ = csvDf
      .writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .options(Map(
        KustoOptions.KUSTO_CLUSTER -> cluster,
        KustoOptions.KUSTO_TABLE -> table,
        KustoOptions.KUSTO_DATABASE -> database,
        KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
        KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
        KustoOptions.KUSTO_AAD_AUTHORITY_ID -> authority,
        KustoOptions.KUSTO_WRITE_ENABLE_ASYNC -> "true"))
      .trigger(Trigger.Once)

    kustoQ.start().awaitTermination()

    KustoTestUtils.validateResultsAndCleanup(kustoAdminClient, table, database, expectedNumberOfRows, timeoutMs, tableCleanupPrefix = prefix)
  }
}
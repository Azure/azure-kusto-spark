package com.microsoft.kusto.spark

import java.util.UUID

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SinkTableCreationMode, SparkIngestionProperties}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DataTypes.IntegerType
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

@RunWith(classOf[JUnitRunner])
class KustoSinkStreamingE2E extends FlatSpec with BeforeAndAfterAll {
  val expectedNumberOfRows: Int = 300
  val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes
  val sleepTimeTillTableCreate: Int = 3 * 60 * 1000 // 2 minutes
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

  val appId: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_ID)
  val appKey: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_SECRET)
  val authority: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com")
  val cluster: String = System.getProperty(KustoSinkOptions.KUSTO_CLUSTER)
  val database: String = System.getProperty(KustoSinkOptions.KUSTO_DATABASE)

  val csvPath: String = System.getProperty("path", "src/test/resources/TestData/csv")
  val customSchema: StructType = new StructType().add("colA", StringType, nullable = true).add("colB", IntegerType, nullable = true)

//  "KustoStreamingSinkSyncWithTableCreateAndIngestIfNotExist" should "ingest structured data to a Kusto cluster" taggedAs KustoE2E in {
//
//    if (appId == null || appKey == null || authority == null || cluster == null || database == null) {
//      fail()
//    }
//
//    val prefix = "KustoStreamingSparkE2E_Ingest"
//    val table = s"${prefix}_${UUID.randomUUID().toString.replace("-", "_")}"
//    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
//    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
//
//    val csvDf = spark
//      .readStream
//      .schema(customSchema)
//      .csv(csvPath)
//
//    val consoleQ = csvDf
//      .writeStream
//      .format("console")
//      .trigger(Trigger.Once)
//    consoleQ.start()
//
//    val sp = new SparkIngestionProperties
//    val tags = new java.util.ArrayList[String]()
//    tags.add("tagytag")
//    sp.ingestByTags = tags
//    sp.ingestIfNotExists = tags
//
//    spark.conf.set("spark.sql.streaming.checkpointLocation", "target/temp/checkpoint")
//
//    val kustoQ = csvDf
//      .writeStream
//      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
//      .options(Map(
//        KustoSinkOptions.KUSTO_CLUSTER -> cluster,
//        KustoSinkOptions.KUSTO_TABLE -> table,
//        KustoSinkOptions.KUSTO_DATABASE -> database,
//        KustoSinkOptions.KUSTO_AAD_APP_ID -> appId,
//        KustoSinkOptions.KUSTO_AAD_APP_SECRET -> appKey,
//        KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID -> authority,
//        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS -> SinkTableCreationMode.CreateIfNotExist.toString,
//        KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON -> sp.toString))
//      .trigger(Trigger.Once)
//
//    kustoQ.start()
//
//    // Sleep util table is expected to be created
//    Thread.sleep(sleepTimeTillTableCreate)
//    KustoTestUtils.validateResultsAndCleanup(kustoAdminClient, table, database, expectedNumberOfRows, timeoutMs - sleepTimeTillTableCreate, tableCleanupPrefix = prefix)
//  }
//
//  "KustoStreamingSinkAsync" should "also ingest structured data to a Kusto cluster" taggedAs KustoE2E in {
//
//    if(appId == null || appKey == null || authority == null || cluster == null || database == null){
//      fail()
//    }
//
//    val prefix = "KustoStreamingSparkE2EAsync_Ingest"
//    val table = s"${prefix}_${UUID.randomUUID().toString.replace("-","_")}"
//    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
//    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
//
//    kustoAdminClient.execute(database, generateTempTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))
//
//    val csvDf = spark
//      .readStream
//      .schema(customSchema)
//      .csv(csvPath)
//
//    val consoleQ = csvDf
//      .writeStream
//      .format("console")
//      .trigger(Trigger.Once)
//
//    consoleQ.start().awaitTermination()
//
//    spark.conf.set("spark.sql.streaming.checkpointLocation", "target/temp/checkpoint")
//
//    val kustoQ = csvDf
//      .writeStream
//      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
//      .options(Map(
//        KustoSinkOptions.KUSTO_CLUSTER -> cluster,
//        KustoSinkOptions.KUSTO_TABLE -> table,
//        KustoSinkOptions.KUSTO_DATABASE -> database,
//        KustoSinkOptions.KUSTO_AAD_APP_ID -> appId,
//        KustoSinkOptions.KUSTO_AAD_APP_SECRET -> appKey,
//        KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID -> authority,
//        KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC -> "true"))
//      .trigger(Trigger.Once)
//
//    kustoQ.start().awaitTermination()
//
//    KustoTestUtils.validateResultsAndCleanup(kustoAdminClient, table, database, expectedNumberOfRows, timeoutMs, tableCleanupPrefix = prefix)
//  }

  val cosmosEndpoint = "https://spark-cosmos-feed.documents.azure.com:443/"
  val cosmosMasterKey = "yBrPB6oNOeQtTkg57F7Y6j6KQyJHb5nokD2l6J9NT8dvlL5KLhWh2m4lJgVJXvQmes397ascOs4mlZAFyTz1DQ=="
  val cosmosDatabaseName = "ToDoList"
  val cosmosContainerName = "Items"

  val changeFeedCfg = Map(
    "spark.cosmos.accountEndpoint"-> cosmosEndpoint,
    "spark.cosmos.accountKey"-> cosmosMasterKey,
    "spark.cosmos.database"-> cosmosDatabaseName,
    "spark.cosmos.container"-> cosmosContainerName,
    "spark.cosmos.read.partitioning.strategy"-> "Default",
    "spark.cosmos.read.inferSchema.enabled" -> "false",
    "spark.cosmos.changeFeed.startFrom" -> "Now",
    "spark.cosmos.changeFeed.mode" ->"Incremental",
  )
  "cosmos" should "also ingest structured data to a Kusto cluster" taggedAs KustoE2E in {

    if(appId == null || appKey == null || authority == null || cluster == null || database == null){
      fail()
    }
    import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SparkIngestionProperties}

    val prefix = "KustoStreamingSparkE2Ecosmos_Ingest"
    val table = s"${prefix}_${UUID.randomUUID().toString.replace("-","_")}"
    val conf = Map(
      KustoSinkOptions.KUSTO_CLUSTER -> cluster,
      KustoSinkOptions.KUSTO_TABLE -> table,
      KustoSinkOptions.KUSTO_DATABASE -> database,
      KustoSinkOptions.KUSTO_AAD_APP_ID -> appId,
      KustoSinkOptions.KUSTO_AAD_APP_SECRET -> appKey,
      KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID -> authority)

    spark.conf.set("spark.sql.streaming.checkpointLocation", "target/temp/checkpoint")
    import java.util.concurrent.TimeUnit
    import org.apache.spark.sql.streaming.Trigger

    val df = spark.readStream.format("cosmos.oltp.changeFeed").options(changeFeedCfg).load()
    val kustoQ = df
      .writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .options(conf)
      .trigger(Trigger.ProcessingTime(TimeUnit.SECONDS.toMillis(10))) // Sync this with the ingestionBatching policy of the database

    kustoQ.start().awaitTermination()

  }
}
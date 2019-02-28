package com.microsoft.kusto.spark

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.microsoft.azure.kusto.data.{ClientFactory, ConnectionStringBuilder}
import com.microsoft.kusto.spark.datasource.KustoOptions
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.collection.immutable

@RunWith(classOf[JUnitRunner])
class KustoPruneAndFilterE2E extends FlatSpec with BeforeAndAfterAll {
  private val myName = this.getClass.getSimpleName

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

  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

//  "KustoSource" should "apply pruning and filtering when reading in lean mode" taggedAs KustoE2E in {
//    val table: String = System.getProperty(KustoOptions.KUSTO_TABLE)
//    val query: String = System.getProperty(KustoOptions.KUSTO_QUERY, s"$table | where (toint(ColB) % 1000 == 0) ")
//
//    val conf: Map[String, String] = Map(
//      KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
//      KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
//      KustoOptions.KUSTO_READ_MODE -> "lean"
//    )
//
//    val df = spark.read.kusto(cluster, database, query, conf).select("ColA")
//    df.show()
//  }
//
//  "KustoSource" should "apply pruning and filtering when reading in scale mode" taggedAs KustoE2E in {
//    val table: String = System.getProperty(KustoOptions.KUSTO_TABLE)
//    val query: String = System.getProperty(KustoOptions.KUSTO_QUERY, s"$table | where (toint(ColB) % 1 == 0)")
//
//    val storageAccount: String = System.getProperty("storageAccount")
//    val container: String = System.getProperty("container")
//    val blobKey: String = System.getProperty("blobKey")
//    val blobSas: String = System.getProperty("blobSas")
//    val blobSasConnectionString: String = System.getProperty("blobSasQuery")
//
//    val conf: Map[String, String] = Map(
//      KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
//      KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
//      KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME -> storageAccount,
//      KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY -> blobKey,
//      KustoOptions.KUSTO_BLOB_CONTAINER -> container
//    )
//
//    spark.read.kusto(cluster, database, query, conf).show(20)
//  }

  "KustoConnector" should "write to a kusto table and read it back in scale mode with pruning and filtering" taggedAs KustoE2E in {
    import spark.implicits._

    val rowId = new AtomicInteger(1)
    def newRow(): String = s"row-${rowId.getAndIncrement()}"
    val expectedNumberOfRows: Int =  100
    val rows: immutable.IndexedSeq[(String, Int)] = (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val dfOrig = rows.toDF("name", "value")
    val query = KustoQueryUtils.simplifyName(s"KustoSparkReadWriteWithFiltersTest_${UUID.randomUUID()}")

    // Storage account parameters
    val storageAccount: String = System.getProperty("storageAccount")
    val container: String = System.getProperty("container")
    val blobKey: String = System.getProperty("blobKey")
    val blobSas: String = System.getProperty("blobSas")
    val blobSasConnectionString: String = System.getProperty("blobSasQuery")

    // Create a new table.
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(database, generateTableCreateCommand(query, columnsTypesAndNames = "ColA:string, ColB:int"))

    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoOptions.KUSTO_CLUSTER, cluster)
      .option(KustoOptions.KUSTO_DATABASE, database)
      .option(KustoOptions.KUSTO_TABLE, query)
      .option(KustoOptions.KUSTO_AAD_CLIENT_ID, appId)
      .option(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, appKey)
      .option(KustoOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .save()

    val conf: Map[String, String] = Map(
      KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
      KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
      KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME -> storageAccount,
      KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY -> blobKey,
      KustoOptions.KUSTO_BLOB_CONTAINER -> container
    )

    val dfResult = spark.read.kusto(cluster, database, query, conf)

    val orig = dfOrig.select("name", "value").rdd.map(x => (x.getString(0), x.getInt(1))).collect().sortBy(_._2)
    val result = dfResult.select("ColA", "ColB").rdd.map(x => (x.getString(0), x.getInt(1))).collect().sortBy(_._2)

    // Verify correctness, without pruning and filtering
    assert(orig.deep == result.deep)

    val dfResultPruned = spark.read.kusto(cluster, database, query, conf)
      .select("ColA")
      .sort("ColA")
      .collect()
      .map(x => x.getString(0))
      .sorted

    val origPruned = orig.map(x => x._1).sorted

    assert(dfResultPruned.length == origPruned.length)
    assert(origPruned.deep == dfResultPruned.deep)

    // Cleanup
    KustoTestUtils.tryDropAllTablesByPrefix(kustoAdminClient, database, "KustoSparkReadWriteWithFiltersTest")
  }

  "KustoConnector" should "write to a kusto table and read it back in scale mode with filtering" taggedAs KustoE2E in {
    import spark.implicits._

    val rowId = new AtomicInteger(1)
    def newRow(): String = s"row-${rowId.getAndIncrement()}"
    val expectedNumberOfRows: Int =  100
    val rows: immutable.IndexedSeq[(String, Int)] = (1 to expectedNumberOfRows).map(v => (newRow(), v))
    val dfOrig = rows.toDF("name", "value")
    val query = KustoQueryUtils.simplifyName(s"KustoSparkReadWriteWithFiltersTest_${UUID.randomUUID()}")

    // Storage account parameters
    val storageAccount: String = System.getProperty("storageAccount")
    val container: String = System.getProperty("container")
    val blobKey: String = System.getProperty("blobKey")
    val blobSas: String = System.getProperty("blobSas")
    val blobSasConnectionString: String = System.getProperty("blobSasQuery")

    // Create a new table.
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(database, generateTableCreateCommand(query, columnsTypesAndNames = "ColA:string, ColB:int"))

    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoOptions.KUSTO_CLUSTER, cluster)
      .option(KustoOptions.KUSTO_DATABASE, database)
      .option(KustoOptions.KUSTO_TABLE, query)
      .option(KustoOptions.KUSTO_AAD_CLIENT_ID, appId)
      .option(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, appKey)
      .option(KustoOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .save()

    val conf: Map[String, String] = Map(
      KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
      KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
      KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME -> storageAccount,
      KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY -> blobKey,
      KustoOptions.KUSTO_BLOB_CONTAINER -> container
    )

    val dfResult = spark.read.kusto(cluster, database, query, conf)
    val dfFiltered = dfResult
      .where(dfResult.col("ColA").startsWith("row-2"))
      .filter("ColB > 12")
      .filter("ColB <= 21")
      .collect().sortBy(x => x.getAs[Int](1))


    val expected = Array(Row("row-20", 20), Row("row-21", 21))
    assert(dfFiltered.deep == expected.deep)

    // Cleanup
    KustoTestUtils.tryDropAllTablesByPrefix(kustoAdminClient, database, "KustoSparkReadWriteWithFiltersTest")
  }
}
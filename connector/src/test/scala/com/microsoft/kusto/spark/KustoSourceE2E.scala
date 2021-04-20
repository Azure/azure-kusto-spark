package com.microsoft.kusto.spark

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SinkTableCreationMode}
import com.microsoft.kusto.spark.datasource.{KustoSourceOptions, ReadMode}
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.collection.immutable

@RunWith(classOf[JUnitRunner])
class KustoSourceE2E extends FlatSpec with BeforeAndAfterAll {
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

  val appId: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_ID)
  val appKey: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_SECRET)
  val authority: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com")
  val cluster: String = System.getProperty(KustoSinkOptions.KUSTO_CLUSTER)
  val database: String = System.getProperty(KustoSinkOptions.KUSTO_DATABASE)

  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  import spark.implicits._

  val rowId = new AtomicInteger(1)

  def newRow(): String = s"row-${rowId.getAndIncrement()}"

  val expectedNumberOfRows: Int = 100
  val rows: immutable.IndexedSeq[(String, Int)] = (1 to expectedNumberOfRows).map(v => (newRow(), v))
  val dfOrig: DataFrame = rows.toDF("name", "value")

  "KustoSource" should "execute a read query on Kusto cluster in single mode" taggedAs KustoE2E in {
    val table: String = System.getProperty(KustoSinkOptions.KUSTO_TABLE)
    val query: String = System.getProperty(KustoSourceOptions.KUSTO_QUERY, s"$table | where (toint(ColB) % 1000 == 0) | distinct ColA ")

    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceSingleMode.toString,
      KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey
    )

    val df = spark.read.kusto(cluster, database, query, conf)
    df.show()
  }

  "KustoSource" should "execute a read query on Kusto cluster in distributed mode" taggedAs KustoE2E in {
    val table: String = System.getProperty(KustoSinkOptions.KUSTO_TABLE)
    val query: String = System.getProperty(KustoSourceOptions.KUSTO_QUERY, s"$table | where (toint(ColB) % 1 == 0)")

    //    val storageAccount: String = System.getProperty("storageAccount")
    //    val container: String = System.getProperty("container")
    //    val blobKey: String = System.getProperty("blobKey")
    val blobSas: String = System.getProperty("blobSas")

    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
      KustoSourceOptions.KUSTO_BLOB_STORAGE_SAS_URL -> blobSas
      //      KustoSourceOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME -> storageAccount,
      //      KustoSourceOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY -> blobKey,
      //      KustoSourceOptions.KUSTO_BLOB_CONTAINER -> container
    )

    spark.read.kusto(cluster, database, query, conf).show(20)
  }

  "KustoConnector" should "write to a kusto table and read it back in default mode" taggedAs KustoE2E in {
    val table = KustoQueryUtils.simplifyName(s"KustoSparkReadWriteTest_${UUID.randomUUID()}")

    // Create a new table.
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(database, generateTempTableCreateCommand(table, columnsTypesAndNames = "ColA:string, ColB:int"))

    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, appId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, appKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .option(KustoSinkOptions.KUSTO_REQUEST_ID, "04ec0408-3cc3_.asd")
      .mode(SaveMode.Append)
      .save()

    val conf: Map[String, String] = Map(
      KustoSinkOptions.KUSTO_AAD_APP_ID -> appId,
      KustoSinkOptions.KUSTO_AAD_APP_SECRET -> appKey
    )

    val dfResult = spark.read.kusto(cluster, database, table, conf)

    val orig = dfOrig.select("name", "value").rdd.map(x => (x.getString(0), x.getInt(1))).collect().sortBy(_._2)
    val result = dfResult.select("ColA", "ColB").rdd.map(x => (x.getString(0), x.getInt(1))).collect().sortBy(_._2)

    assert(orig.deep == result.deep)

    // Cleanup TODO - drop in afterAll
    KustoTestUtils.tryDropAllTablesByPrefix(kustoAdminClient, database, "KustoSparkReadWriteTest")
  }

  "KustoSource" should "read distributed, transient cache change the filter but execute once" taggedAs KustoE2E in {
    import spark.implicits._
    val table = KustoQueryUtils.simplifyName(s"KustoSparkReadWriteTest_${UUID.randomUUID()}")
    val engineKcsb: ConnectionStringBuilder = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster" +
      s".kusto.windows.net", appId, appKey, authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceDistributedMode.toString,
      KustoSourceOptions.KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE -> true.toString,
      KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
      KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID -> authority
    )

    // write
    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, appId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, appKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .option(KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS, SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    val df = spark.read.kusto("ohadprod.westeurope", database, table, conf)

    val time = new DateTime()
    assert(df.count() == expectedNumberOfRows)
    assert(df.count() == expectedNumberOfRows)

    val df2 = df.where(($"value").cast("Int") > 50)
    assert(df2.collect().length == 50)

    // Should take up to another 10 seconds for .show commands to come up
    Thread.sleep(5000 * 60)
    val res3 = kustoAdminClient.execute(
      s""".show commands | where StartedOn > datetime(${time.toString()})  | where
                                        CommandType ==
      "DataExportToFile" | where Text has "$table"""")
    assert(res3.getPrimaryResults.count() == 1)
  }
}

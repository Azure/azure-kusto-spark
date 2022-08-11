package com.microsoft.kusto.spark

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import com.microsoft.azure.kusto.data.{Client, ClientFactory, ClientRequestProperties}
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.KustoTestUtils.KustoConnectionOptions
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SinkTableCreationMode}
import com.microsoft.kusto.spark.datasource.{KustoSourceOptions, ReadMode, TransientStorageCredentials, TransientStorageParameters}
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import java.nio.file.{Files, Paths}
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
  val kustoConnectionOptions: KustoConnectionOptions = KustoTestUtils.getSystemTestOptions
  val table = KustoQueryUtils.simplifyName(s"KustoSparkReadWriteTest_${UUID.randomUUID()}")
//  val getP = System.getProperty("hadoop.home.dir");

  private val loggingLevel = Option(System.getProperty("logLevel"))
  var kustoAdminClient: Option[Client] = None
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)
  override def beforeAll(): Unit = {
    super.beforeAll()

    sc = spark.sparkContext
    sqlContext = spark.sqlContext
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://${kustoConnectionOptions.cluster}.kusto.windows.net",
      kustoConnectionOptions.appId, kustoConnectionOptions.appKey, kustoConnectionOptions.authority)
    kustoAdminClient = Some(ClientFactory.createClient(engineKcsb))
    try {
      kustoAdminClient.get.execute(kustoConnectionOptions.database, generateAlterIngestionBatchingPolicyCommand(
        kustoConnectionOptions.database, "@'{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\": 500, \"MaximumRawDataSizeMB\": 1024}'","database"))
    } catch {
      case _:Exception => // ignore
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()

    sc.stop()
  }

  // Init dataFrame
  import spark.implicits._

  val rowId = new AtomicInteger(1)

  def newRow(): String = s"row-${rowId.getAndIncrement()}"

  val expectedNumberOfRows: Int = 100
  val rows: immutable.IndexedSeq[(String, Int)] = (1 to expectedNumberOfRows).map(v => (newRow(), v))
  val dfOrig: DataFrame = rows.toDF("name", "value")

  "KustoConnector" should "write to a kusto table and read it back in default mode"  in {
    // Create a new table.
    KDSU.logInfo("e2e","running KustoConnector");
    val crp = new ClientRequestProperties
    crp.setTimeoutInMilliSec(2000)
    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, kustoConnectionOptions.appId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, kustoConnectionOptions.appKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, kustoConnectionOptions.authority)
      .option(KustoSinkOptions.KUSTO_REQUEST_ID, "04ec0408-3cc3_.asd")
      .option(KustoSinkOptions.KUSTO_CLIENT_REQUEST_PROPERTIES_JSON, crp.toString)
      .option(KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS, SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    val conf: Map[String, String] = Map(
      KustoSinkOptions.KUSTO_AAD_APP_ID -> kustoConnectionOptions.appId,
      KustoSinkOptions.KUSTO_AAD_APP_SECRET -> kustoConnectionOptions.appKey
    )

    val dfResult = spark.read.kusto(kustoConnectionOptions.cluster, kustoConnectionOptions.database, table, conf)

    val orig = dfOrig.select("name", "value").rdd.map(x => (x.getString(0), x.getInt(1))).collect().sortBy(_._2)
    val result = dfResult.select("name", "value").rdd.map(x => (x.getString(0), x.getInt(1))).collect().sortBy(_._2)

    assert(orig.deep == result.deep)
  }

  "KustoSource" should "execute a read query on Kusto cluster in single mode"  in {
    val query: String = System.getProperty(KustoSourceOptions.KUSTO_QUERY, table)

    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceSingleMode.toString,
      KustoSourceOptions.KUSTO_AAD_APP_ID -> kustoConnectionOptions.appId,
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> kustoConnectionOptions.appKey
    )

    val df = spark.read.kusto(kustoConnectionOptions.cluster, kustoConnectionOptions.database, query, conf)
    df.show()
    val dfres = df.collect()
  }

  "KustoSource" should "execute a read query on Kusto cluster in distributed mode" in {
    val query: String = System.getProperty(KustoSourceOptions.KUSTO_QUERY, table)
    //    val blobSas: String = System.getProperty("blobSas")
    //  TODO - get sas from DM and set it yourself
    //    val storage = new TransientStorageParameters(Array(new TransientStorageCredentials(blobSas)))

    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceDistributedMode.toString,
      //        KustoSourceOptions.KUSTO_TRANSIENT_STORAGE -> storage.toString,
      KustoSourceOptions.KUSTO_AAD_APP_ID -> kustoConnectionOptions.appId,
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> kustoConnectionOptions.appKey
    )

    spark.read.kusto(kustoConnectionOptions.cluster, kustoConnectionOptions.database, query, conf).show(20)
  }

  "KustoSource" should "read distributed, transient cache change the filter but execute once" taggedAs KustoE2E in {
    import spark.implicits._
    val table = KustoQueryUtils.simplifyName(s"KustoSparkReadWriteTest_${UUID.randomUUID()}")

    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceDistributedMode.toString,
      KustoSourceOptions.KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE -> true.toString,
      KustoSourceOptions.KUSTO_AAD_APP_ID -> kustoConnectionOptions.appId,
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> kustoConnectionOptions.appKey,
      KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID -> kustoConnectionOptions.authority
    )

    // write
    dfOrig.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, kustoConnectionOptions.appId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, kustoConnectionOptions.appKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, kustoConnectionOptions.authority)
      .option(KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS, SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()

    val df = spark.read.kusto(kustoConnectionOptions.cluster, kustoConnectionOptions.database, table, conf)

    val time = new DateTime()
    assert(df.count() == expectedNumberOfRows)
    assert(df.count() == expectedNumberOfRows)

    val df2 = df.where(($"value").cast("Int") > 50)
    assert(df2.collect().length == 50)

    // Should take up to another 10 seconds for .show commands to come up
    Thread.sleep(5000 * 60)
    val res3 = kustoAdminClient.get.execute(
      s""".show commands | where StartedOn > datetime(${time.toString()})  | where
                                        CommandType ==
      "DataExportToFile" | where Text has "$table"""")
    assert(res3.getPrimaryResults.count() == 1)
  }
}

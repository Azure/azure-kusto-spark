package com.microsoft.kusto.spark

import com.microsoft.kusto.spark.datasource.KustoSourceOptions
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class KustoSourceTests extends FlatSpec with MockFactory with Matchers with BeforeAndAfterAll {
  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  private val nofExecutors = 4
  private val spark: SparkSession = SparkSession.builder()
    .appName("KustoSource")
    .master(f"local[$nofExecutors]")
    .getOrCreate()

  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _
  private val cluster: String = "KustoCluster"
  private val database: String = "KustoDatabase"
  private val query: String = "KustoTable"
  private val appId: String = "KustoSinkTestApplication"
  private val appKey: String = "KustoSinkTestKey"
  private val appAuthorityId: String = "KustoSinkAuthorityId"

  override def beforeAll(): Unit = {
    super.beforeAll()

    sc = spark.sparkContext
    sqlContext = spark.sqlContext
  }

  override def afterAll(): Unit = {
    super.afterAll()

    sc.stop()
  }

  "KustoDataSource" should "recognize Kusto and get the correct schema" in {
    val spark: SparkSession = SparkSession.builder()
      .appName("KustoSource")
      .master(f"local[$nofExecutors]")
      .getOrCreate()

    val customSchema = "colA STRING, colB INT"

    val df = spark.sqlContext
      .read
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSourceOptions.KUSTO_CLUSTER, cluster)
      .option(KustoSourceOptions.KUSTO_DATABASE, database)
      .option(KustoSourceOptions.KUSTO_QUERY, query)
      .option(KustoSourceOptions.KUSTO_AAD_APP_ID, appId)
      .option(KustoSourceOptions.KUSTO_AAD_APP_SECRET, appKey)
      .option(KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID, appAuthorityId)
      .option(KustoSourceOptions.KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES, customSchema)
      .load("src/test/resources/")

    df.printSchema()
  }
}
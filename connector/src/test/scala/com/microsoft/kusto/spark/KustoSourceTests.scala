package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAccessTokenAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasource.{KustoRelation, KustoSourceOptions, TransientStorageCredentials, TransientStorageParameters}
import com.microsoft.kusto.spark.utils.KustoClientCache.ClusterAndAuth
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

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

    val expected = StructType(Array(StructField("colA", StringType, nullable = true),StructField("colB", IntegerType, nullable = true)))
    assert(df.schema.equals(expected))
  }

  "KustoDataSource" should "fail with credentials in plain text" in {
    val ksr = KustoRelation(
      KustoCoordinates(cluster, "", database, table = Option("tablename"), ingestionUrl = Option("")),
      KustoAccessTokenAuthentication("token1"),
      "",
      KDSU.getReadParameters(Map[String, String](), null),
      Duration(20, TimeUnit.SECONDS),
      Option(""),
      Option(new TransientStorageParameters(Array(new TransientStorageCredentials("https://xxxxxx.blob.core.windows.net/xxxxxx-0?sp=r&st=2023-03-15T17:05:53Z&se=2023-03-16T01:05:53Z&spr=https&sv=2021-12-02&sr=c&sig=xxxxxxxxx")))),
      Option(new ClientRequestProperties),
      "reqid"
    )(sqlContext.sparkSession)
    assert(!ksr.toString.contains("token1"))
  }

  "KustoDataSource" should "parse sas" in {
    val sas = "https://storage.blob.core.customDom/upload/?<secret>"
    val params = new TransientStorageCredentials(sas)
    assert(params.domainSuffix.equals("core.customDom"))
    assert(params.storageAccountName.equals("storage"))
    assert(params.sasKey.equals("?<secret>"))
    assert(params.blobContainer.equals("upload/"))
    assert(params.sasDefined.equals(true))
  }

  "KustoDataSource" should "match cluster default url pattern" in {
    val ingestUrl = "https://ingest-ohbitton.dev.kusto.windows.net"
    val engineUrl = "https://ohbitton.dev.kusto.windows.net"
    val expectedAlias = "ohbitton.dev"
    val alias = KDSU.getClusterNameFromUrlIfNeeded(ingestUrl)
    assert(alias.equals(expectedAlias))
    val engine = KDSU.getEngineUrlFromAliasIfNeeded(expectedAlias)
    assert(engine.equals(engineUrl))
    assert(KDSU.getEngineUrlFromAliasIfNeeded(engineUrl ).equals(engineUrl))

    assert(ingestUrl.equals(ClusterAndAuth(engineUrl,null, None, alias).ingestUri))
    assert(ingestUrl.equals(ClusterAndAuth(engineUrl,null, Some(ingestUrl), alias).ingestUri))

    val engine2 = KDSU.getEngineUrlFromAliasIfNeeded(ingestUrl)
    assert(engine2.equals(engineUrl))
  }

  "KustoDataSource" should "match cluster custom domain url or aria old cluster" in {
    val url = "https://ingest-ohbitton.dev.kusto.customDom"
    val engineUrl = "https://ohbitton.dev.kusto.customDom"
    val expectedAlias = "ohbitton.dev"
    val alias = KDSU.getClusterNameFromUrlIfNeeded(url)
    assert(alias.equals(expectedAlias))
    assert(url.equals(ClusterAndAuth(engineUrl,null, None, alias).ingestUri))

    val ariaEngineUrl = "https://kusto.aria.microsoft.com"
    val expectedAriaAlias = "Aria proxy"
    val ariaAlias = KDSU.getClusterNameFromUrlIfNeeded(ariaEngineUrl)
    assert(ariaAlias.equals(expectedAriaAlias))
  }
}
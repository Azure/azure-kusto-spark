package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.{AadApplicationAuthentication, KustoAuthentication}
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils
import org.apache.parquet.Log
import org.apache.parquet.hadoop.ParquetOutputCommitter
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerTaskEnd}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.time.Instant

class KustoParquetWriterTest extends FunSuite with BeforeAndAfterAll {

  private val classUnderTest = this.getClass.getSimpleName
  private val nofExecutors = 4
  private var spark: SparkSession = _
  private var kustoParquetWriter: KustoParquetWriter = _

  override def beforeAll(): Unit = {
    val LOG = new Log(classOf[KustoParquetWriterTest])
    spark = SparkSession.builder()
      .appName("KustoSink")
      .master(f"local[$nofExecutors]")
      .config("spark.sql.parquet.output.committer.class", "com.microsoft.kusto.spark.datasink.KustoParquetOutputCommitter")
      .config("spark.sql.files.maxPartitionBytes", 100 * 1024 * 1024)
      .config("spark.sql.sources.commitProtocolClass", "com.microsoft.kusto.spark.datasink.KustoFileCommitProtocol")
      .getOrCreate()
      spark.conf.set("spark.sql.parquet.output.committer.class", "com.microsoft.kusto.spark.datasink.KustoParquetOutputCommitter")
      spark.conf.set("spark.sql.files.maxPartitionBytes", 100 * 1024 * 1024)
      spark.conf.set("spark.sql.sources.commitProtocolClass", "com.microsoft.kusto.spark.datasink.KustoFileCommitProtocol")


    /*val appId: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_ID)
    val appKey: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_SECRET)
    val authority: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com")
    val cluster: String = System.getProperty(KustoSinkOptions.KUSTO_CLUSTER)
    val database: String = "sdktestsdb" //System.getProperty(KustoSinkOptions.KUSTO_DATABASE)
    val tableName = "sparkdata"

    val transientStorageCredentials = new TransientStorageCredentials(storageAccountName = "sdke2eteststorage",
      storageAccountKey = "",
      blobContainer = "" )
    transientStorageCredentials.domainSuffix = KustoDataSourceUtils.DefaultDomainPostfix

    /*val ingestionProperties = new IngestionProperties(database, tableName)
    ingestionProperties.getIngestionMapping().setIngestionMappingReference("spark_data_ref2", IngestionMappingKind.PARQUET)
    ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PARQUET)
    ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
    ingestionProperties.setFlushImmediately(true)*/


    kustoParquetWriter = new KustoParquetWriter(spark, storageCredentials = transientStorageCredentials)*/
  }

  test("testWrite") {
    val yellowSourcePath = "wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2018/puMonth=11/part-00000-tid-8898858832658823408-a1de80bd-eed3-4d11-b9d4-fa74bfbd47bc-426339-124.c000.snappy.parquet"
    //val yellowSourcePath = "wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2018/puMonth=11/*.snappy.parquet"
    val inputDF = spark.read.parquet(yellowSourcePath)
    inputDF.show()
    kustoParquetWriter = new KustoParquetWriter()
    kustoParquetWriter.write(Some(100L),
      inputDF,
      KustoCoordinates(
        clusterUrl = "https://xx.southeastasia.dev.kusto.windows.net",
        database = "spark",
        table = Some("yellowtaxi"),clusterAlias = "x",
        ingestionUrl = Some("https://ingest-xx.southeastasia.dev.kusto.windows.net")),
      AadApplicationAuthentication("xx-x-4093-9c2f-6f6c3c85b60c","x",
        "x"),
      WriteOptions(tableCreateOptions = SinkTableCreationMode.CreateIfNotExist),
      new ClientRequestProperties())
    /*val schema = StructType(Array(
      StructField("EventId", StringType, nullable = true),
      StructField("State", StringType, nullable = true),
      StructField("EventType", StringType, nullable = true),
      StructField("Source", StringType, nullable = true)
    ))
    val sourceLines = scala.io.Source.fromFile("storms.csv").getLines()
    val rawSequence =  sourceLines.map(x=>x.split(",")).map(y=>(StringUtils.trim(y(0)),StringUtils.trim(y(1)),y(2),y(3))).toSeq 
    val rdd = spark.sparkContext.parallelize(rawSequence)
    val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2, attributes._3, attributes._4))
    val languageDF = spark.createDataFrame(rowRDD, schema)
    kustoParquetWriter.write(languageDF,"a-test-db","a-test-table")
    /**********/

    val schema = StructType(Array(
      StructField("Language", StringType, nullable = true),
      StructField("Datetime", LongType, nullable = true),
      StructField("Footprint", DoubleType, nullable = true)
    ))
    val inputDataFrame = Seq(("Python", Instant.now().toEpochMilli, 2.123),
      ("Java", Instant.now().toEpochMilli, 1.123),
      ("OCaml", Instant.now().toEpochMilli, 1.123),
      ("Scala", Instant.now().toEpochMilli, 0.123))
    val rdd = spark.sparkContext.parallelize(inputDataFrame)
    val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2, attributes._3))
    val languageDF = spark.createDataFrame(rowRDD, schema)
    //kustoParquetWriter.write(languageDF,"a-test-db","a-test-table")*/
  }
}

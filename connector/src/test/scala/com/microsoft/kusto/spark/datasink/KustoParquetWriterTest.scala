package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestClientFactory, IngestionProperties}
import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import com.microsoft.kusto.spark.utils.{ExtendedKustoClient, KustoClientCache, KustoDataSourceUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.time.Instant

class KustoParquetWriterTest extends FunSuite with BeforeAndAfterAll {

  private val classUnderTest = this.getClass.getSimpleName
  private val nofExecutors = 4
  private var spark: SparkSession = _
  private var kustoParquetWriter: KustoParquetWriter = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("KustoSink")
      .master(f"local[$nofExecutors]")
      .getOrCreate()
    val appId: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_ID)
    val appKey: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_SECRET)
    val authority: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com")
    val cluster: String = System.getProperty(KustoSinkOptions.KUSTO_CLUSTER)
    val database: String = "sdktestsdb" //System.getProperty(KustoSinkOptions.KUSTO_DATABASE)
    val tableName = "sparkdata"

    val transientStorageCredentials = new TransientStorageCredentials(storageAccountName = "",
      storageAccountKey = "",
      blobContainer = "" )
    transientStorageCredentials.domainSuffix = KustoDataSourceUtils.DefaultDomainPostfix

    val ingestionProperties = new IngestionProperties(database, tableName)
    ingestionProperties.getIngestionMapping().setIngestionMappingReference("spark_data_ref2", IngestionMappingKind.PARQUET)
    ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PARQUET)
    ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
    ingestionProperties.setFlushImmediately(true)


    kustoParquetWriter = new KustoParquetWriter(spark, storageCredentials = transientStorageCredentials,ingestionProperties)
  }

  test("testWrite") {
    val schema = StructType(Array(
      StructField("language", StringType, nullable = true),
      StructField("date", LongType, nullable = true),
      StructField("footprint", DoubleType, nullable = true)
    ))
    val inputDataFrame = Seq(("Python", Instant.now().toEpochMilli, 2.123),
      ("Java", Instant.now().toEpochMilli, 1.123),
      ("OCaml", Instant.now().toEpochMilli, 1.123),
      ("Scala", Instant.now().toEpochMilli, 0.123))
    val rdd = spark.sparkContext.parallelize(inputDataFrame)
    val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2, attributes._3))
    val languageDF = spark.createDataFrame(rowRDD, schema)
    kustoParquetWriter.write(languageDF,"a-test-db","a-test-table")
  }
}

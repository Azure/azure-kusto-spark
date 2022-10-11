package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils
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

    val transientStorageCredentials = new TransientStorageCredentials(storageAccountName = "",
      storageAccountKey = "",
      blobContainer = "" )
    transientStorageCredentials.domainSuffix = KustoDataSourceUtils.DefaultDomainPostfix
    kustoParquetWriter = new KustoParquetWriter(sparkContext = spark.sparkContext, storageCredentials = transientStorageCredentials)
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

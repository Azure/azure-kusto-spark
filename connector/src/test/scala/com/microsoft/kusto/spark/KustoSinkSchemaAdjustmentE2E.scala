package com.microsoft.kusto.spark

import com.microsoft.kusto.spark.KustoTestUtils.KustoConnectionOptions
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SparkIngestionProperties}
import com.microsoft.kusto.spark.exceptions.SchemaMatchException
import org.apache.spark.sql._
import org.apache.spark.sql.types.StringType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec}


@RunWith(classOf[JUnitRunner])
class KustoSinkSchemaAdjustmentE2E extends FlatSpec
  with BeforeAndAfterEach with BeforeAndAfterAll {

  private val nofExecutors = 4
  private val testTablePrefix = "KustoBatchSinkE2E_SchemaAdjust"
  private val spark = SparkSession.builder()
    .appName("KustoSink")
    .master(f"local[$nofExecutors]")
    .getOrCreate()

  private val expectedNumberOfRows = 3
  private def newRow(index: Int): String = s"row-$index"

  val appId: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_ID)
  val appKey: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_SECRET)
  val authority: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID)
  val cluster: String = System.getProperty(KustoSinkOptions.KUSTO_CLUSTER)
  val database: String = System.getProperty(KustoSinkOptions.KUSTO_DATABASE)
  val kustoConnectionOptions: KustoConnectionOptions = KustoConnectionOptions(cluster, database, appId, appKey, authority)


  override def afterAll(): Unit = {
    spark.sparkContext.stop()
  }

  override def afterEach(): Unit = {
    KustoTestUtils.cleanup(kustoConnectionOptions, testTablePrefix)
  }



  "Source DataFrame schema adjustment" should "not adjust" taggedAs KustoE2E in {
    import spark.implicits._
    val sourceValues = (1 to expectedNumberOfRows).map(v => (newRow(v), v))
    val df = sourceValues.toDF("WrongColA", "WrongColB")
    val targetSchema = "ColA:int, ColB:string"
    val schemaAdjustmentMode = "NoAdjustment"

    val testTable = KustoTestUtils.createTestTable(kustoConnectionOptions, testTablePrefix, targetSchema)
    KustoTestUtils.ingest(kustoConnectionOptions, df, testTable, schemaAdjustmentMode)

    val expectedData = df
      .withColumn("ColA", functions.lit(null))
      .withColumn("ColB", functions.col("WrongColB").cast(StringType))
      .select("ColA", "ColB")

    assert(KustoTestUtils.validateTargetTable(kustoConnectionOptions, testTable, expectedData, spark))

  }

  "Source DataFrame schema adjustment"  should "produce SchemaMatchException when column names not match" taggedAs KustoE2E in {
    val thrown = intercept[SchemaMatchException] {
      import spark.implicits._
      val sourceValues = (1 to expectedNumberOfRows).map(v => (newRow(v), v))
      val df = sourceValues.toDF("WrongColA", "WrongColB")
      val targetSchema = "ColA:string, ColB:int"
      val schemaAdjustmentMode = "FailIfNotMatch"

      val testTable = KustoTestUtils.createTestTable(kustoConnectionOptions, "", targetSchema)
      KustoTestUtils.ingest(kustoConnectionOptions, df, testTable, schemaAdjustmentMode)

    }
    assert(thrown.getMessage.startsWith("Target table schema does not match to DataFrame schema."))
  }

  "Source DataFrame schema adjustment"  should "produce SchemaMatchException when source has additional columns" taggedAs KustoE2E in {
    val thrown = intercept[SchemaMatchException] {
      import spark.implicits._
      val sourceValues = (1 to expectedNumberOfRows).map(v => (newRow(v), v, "AdditionalData"))
      val df = sourceValues.toDF("ColA", "ColB", "AdditionalColC")
      val targetSchema = "ColA:string, ColB:int"
      val schemaAdjustmentMode = "GenerateDynamicCsvMapping"

      val testTable = KustoTestUtils.createTestTable(kustoConnectionOptions, "", targetSchema)
      KustoTestUtils.ingest(kustoConnectionOptions, df, testTable, schemaAdjustmentMode)

    }
    assert(thrown.getMessage.startsWith("Source schema has columns that are not present in the target"))
  }

  "Source DataFrame schema adjustment"  should "generate dynamic csv mapping according to column names" taggedAs KustoE2E in {
    import spark.implicits._
    val sourceValues = (1 to expectedNumberOfRows).map(v => (newRow(v), v))
    val df = sourceValues.toDF("SourceColA", "SourceColB")
    val targetSchema = "ColA:string, ColB:int, SourceColB:int, SourceColA:string"
    val schemaAdjustmentMode = "GenerateDynamicCsvMapping"

    val testTable = KustoTestUtils.createTestTable(kustoConnectionOptions, "KustoBatchSinkE2E_SchemaAdjust", targetSchema)
    KustoTestUtils.ingest(kustoConnectionOptions, df, testTable, schemaAdjustmentMode)

    val expectedData = df
      .withColumn("ColA", functions.lit(""))
      .withColumn("ColB", functions.lit(null))
      .select("ColA", "ColB", "SourceColB", "SourceColA")

    assert(KustoTestUtils.validateTargetTable(kustoConnectionOptions, testTable, expectedData, spark))

  }

  "Source DataFrame schema adjustment"  should "produce IllegalArgumentException when csvMappingNameReference in sink options" taggedAs KustoE2E in {
    val thrown = intercept[IllegalArgumentException] {
      import spark.implicits._
      val sourceValues = (1 to expectedNumberOfRows).map(v => (newRow(v), v))
      val df = sourceValues.toDF("ColA", "ColB")
      val targetSchema = "ColA:string, ColB:int"
      val schemaAdjustmentMode = "GenerateDynamicCsvMapping"

      val testTable = KustoTestUtils.createTestTable(kustoConnectionOptions, "", targetSchema)
      KustoTestUtils.ingest(kustoConnectionOptions, df, testTable, schemaAdjustmentMode,
        new SparkIngestionProperties(csvMappingNameReference = "testError"))

    }
    assert(thrown.getMessage.contains("are not compatible"))
  }

}

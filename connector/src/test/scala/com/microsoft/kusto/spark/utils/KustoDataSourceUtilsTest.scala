// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.kusto.spark.datasink.KustoSinkOptions.{
  KUSTO_CLUSTER,
  KUSTO_DATABASE,
  KUSTO_INGESTION_STORAGE,
  KUSTO_TABLE,
  KUSTO_TABLE_CREATE_OPTIONS
}
import com.microsoft.kusto.spark.datasink.{
  KustoSinkOptions,
  SchemaAdjustmentMode,
  SparkIngestionProperties
}
import com.microsoft.kusto.spark.datasource.ReadMode.ForceDistributedMode
import com.microsoft.kusto.spark.datasource.{
  KustoReadOptions,
  KustoSourceOptions,
  PartitionOptions,
  ReadMode
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

import java.security.InvalidParameterException
import scala.collection.mutable

class KustoDataSourceUtilsTest extends AnyFlatSpec with MockFactory {
  "ReadParameters" should "KustoReadOptions with passed in options" in {
    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceDistributedMode.toString,
      KustoSourceOptions.KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE -> true.toString,
      KustoSourceOptions.KUSTO_AAD_APP_ID -> "AppId",
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> "AppKey",
      KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID -> "Tenant",
      KustoSourceOptions.KUSTO_EXPORT_OPTIONS_JSON -> "{\"sizeLimit\":250,\"compressionType\":\"gzip\",\"async\":\"none\"}")
    // a no interaction mock only for test
    val actualReadOptions = KustoDataSourceUtils.getReadParameters(conf, null)
    val expectedResult = KustoReadOptions(
      Some(ForceDistributedMode),
      PartitionOptions(1, None, None),
      distributedReadModeTransientCacheEnabled = true,
      None,
      Map("sizeLimit" -> "250", "compressionType" -> "gzip", "async" -> "none"))
    assert(actualReadOptions != null)
    assert(actualReadOptions == expectedResult)
  }

  "ReadParameters" should "throw an exception when an invalid export options is passed" in {
    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceDistributedMode.toString,
      KustoSourceOptions.KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE -> true.toString,
      KustoSourceOptions.KUSTO_AAD_APP_ID -> "AppId",
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> "AppKey",
      KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID -> "Tenant",
      KustoSourceOptions.KUSTO_EXPORT_OPTIONS_JSON -> "\"sizeLimit\":250,\"compressionType\":\"gzip\",\"async\":\"none\"}")
    val illegalArgumentException =
      intercept[IllegalArgumentException](KustoDataSourceUtils.getReadParameters(conf, null))
    assert(
      illegalArgumentException.getMessage == "The configuration for kustoExportOptionsJson has " +
        "a value \"sizeLimit\":250,\"compressionType\":\"gzip\",\"async\":\"none\"} that cannot be parsed as Map")
  }

  "WriteParameters" should "throw an exception streaming writeMode passes in unsupported SparkIngestionProperties" in {

    val testCombinations =
      Table(
        ("k", "v", "isInvalid"),
        (KustoSinkOptions.KUSTO_WRITE_MODE, "KustoStreaming", true),
        (KustoSinkOptions.KUSTO_WRITE_MODE, "Queued", false),
        (KustoSinkOptions.KUSTO_WRITE_MODE, "Transactional", false),
        ("", "", false) // falls back to transactional mode
      )
    forAll(testCombinations) { (k, v, isInvalid) =>
      val conf: mutable.Map[String, String] = mutable.Map(
        KUSTO_DATABASE -> "DB",
        KUSTO_TABLE -> "Table",
        KUSTO_CLUSTER -> "https://test-cluster.southeastasia.kusto.windows.net",
        KustoSourceOptions.KUSTO_AAD_APP_ID -> "AppId",
        KustoSourceOptions.KUSTO_AAD_APP_SECRET -> "AppKey",
        KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID -> "Tenant",
        KUSTO_TABLE_CREATE_OPTIONS -> "CreateIfNotExist",
        KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON ->
          "{\"ingestByTags\":[\"tag\"],\"dropByTags\":[\"tag\"],\"additionalTags\":[\"tag\"],\"creationTime\":\"2021-07-01T00:00:00Z\"}")
      conf.put(k, v)
      if (isInvalid) {
        val illegalArgumentException = {
          intercept[InvalidParameterException](
            KustoDataSourceUtils.parseSinkParameters(conf.toMap))
        }
        assert(
          illegalArgumentException.getMessage == "Ingest by tags / Drop by tags / Additional tags / Creation Time are " +
            "not supported for streaming ingestion through SparkIngestionProperties")
      }
    }
  }

  "WriteParameters" should "throw an exception streaming writeMode passes in unsupported AdjustmentMode" in {
    val testCombinations =
      Table(
        ("map", "isInvalid", "error"),
        (
          mutable.Map(
            KustoSinkOptions.KUSTO_WRITE_MODE -> "KustoStreaming",
            KustoSinkOptions.KUSTO_ADJUST_SCHEMA -> SchemaAdjustmentMode.GenerateDynamicCsvMapping.toString),
          true,
          "GenerateDynamicCsvMapping cannot be used with Spark streaming ingestion"),
        (
          mutable.Map(
            KustoSinkOptions.KUSTO_WRITE_MODE -> "Queued",
            KustoSinkOptions.KUSTO_ADJUST_SCHEMA -> SchemaAdjustmentMode.GenerateDynamicCsvMapping.toString),
          false,
          ""),
        (
          mutable.Map(
            KustoSinkOptions.KUSTO_WRITE_MODE -> "Transactional",
            KustoSinkOptions.KUSTO_ADJUST_SCHEMA -> SchemaAdjustmentMode.GenerateDynamicCsvMapping.toString),
          false,
          ""),
        (
          mutable.Map(
            KustoSinkOptions.KUSTO_WRITE_MODE -> "KustoStreaming",
            KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON ->
              "{\"csvMappingNameReference\":\"a-mapping-ref\"}"),
          false,
          ""),
        (
          mutable.Map(
            KustoSinkOptions.KUSTO_WRITE_MODE -> "KustoStreaming",
            KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON ->
              "{\"csvMapping\":\"(..a real mapping.)\"}"),
          true,
          "CSVMapping cannot be used with Spark streaming ingestion"),
        (mutable.Map("" -> ""), false, "") // falls back to transactional mode
      )
    forAll(testCombinations) { (map, isInvalid, errorMessage) =>
      val conf: mutable.Map[String, String] = mutable.Map(
        KUSTO_DATABASE -> "DB",
        KUSTO_TABLE -> "Table",
        KUSTO_CLUSTER -> "https://test-cluster.southeastasia.kusto.windows.net",
        KustoSourceOptions.KUSTO_AAD_APP_ID -> "AppId",
        KustoSourceOptions.KUSTO_AAD_APP_SECRET -> "AppKey",
        KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID -> "Tenant",
        KUSTO_TABLE_CREATE_OPTIONS -> "CreateIfNotExist")
      map.foreach { case (k, v) =>
        conf.put(k, v)
      }
      if (isInvalid) {
        val illegalArgumentException = {
          intercept[IllegalArgumentException](
            KustoDataSourceUtils.parseSinkParameters(conf.toMap))
        }
        assert(illegalArgumentException.getMessage == errorMessage)
      }
    }
  }
  "Parsing" should "fail for invalid ingestion storage strings" in {
    val ingestionStorage =
      s"""[{"storageUrl":"https://ateststorage.blob.core.windows.net/container1","containerName":"container1","userMsi":"msi1"},
         |{"storageUrl":"https://ateststorage.blob.core.windows.net/container2","containerName":"","userMsi":"msi2"}]""".stripMargin

    val conf: Map[String, String] = Map(
      KUSTO_DATABASE -> "DB",
      KUSTO_TABLE -> "Table",
      KUSTO_CLUSTER -> "https://test-cluster.southeastasia.kusto.windows.net",
      KustoSourceOptions.KUSTO_AAD_APP_ID -> "AppId",
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> "AppKey",
      KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID -> "Tenant",
      KUSTO_TABLE_CREATE_OPTIONS -> "CreateIfNotExist",
      KUSTO_INGESTION_STORAGE -> ingestionStorage)
    val illegalArgumentException = {
      intercept[IllegalArgumentException](KustoDataSourceUtils.parseSinkParameters(conf))
    }
    assert(
      illegalArgumentException.getMessage == "storageUrl and containerName must be set when supplying ingestion storage")
  }

  private val testName = "Create debug options"
  private val minimalExtentsCountForSplitMergePerNode = 5
  private val maxRetriesOnMoveExtents = 3
  private val allSchemaTestCombinations = Table(
    "schemaAdjustmentMode",
    SchemaAdjustmentMode.GenerateDynamicCsvMapping,
    SchemaAdjustmentMode.FailIfNotMatch,
    SchemaAdjustmentMode.NoAdjustment)
  forAll(allSchemaTestCombinations) { schemaAdjustmentMode =>
    testName should s"get created with default values when no special options specified with $schemaAdjustmentMode" in {
      val result = KustoDataSourceUtils.validateAndCreateWriteDebugOptions(
        schemaAdjustmentMode,
        minimalExtentsCountForSplitMergePerNode,
        maxRetriesOnMoveExtents,
        disableFlushImmediately = false,
        ensureNoDupBlobs = false,
        addSourceLocationTransform = false,
        maybeSparkIngestionProperties = None)

      assert(
        result.minimalExtentsCountForSplitMergePerNode == minimalExtentsCountForSplitMergePerNode)
      assert(result.maxRetriesOnMoveExtents == 3)
      assert(!result.disableFlushImmediately)
      assert(!result.ensureNoDuplicatedBlobs)
      assert(!result.addSourceLocationTransform)
    }
  }

  s"$testName" should "throw IllegalArgumentException when addSourceLocationTransform is true and ingestion properties contain mapping" in {
    val sparkIngestionProperties = new SparkIngestionProperties()
    sparkIngestionProperties.csvMapping = "csv_mapping"

    val exception = intercept[IllegalArgumentException] {
      KustoDataSourceUtils.validateAndCreateWriteDebugOptions(
        SchemaAdjustmentMode.GenerateDynamicCsvMapping,
        minimalExtentsCountForSplitMergePerNode,
        maxRetriesOnMoveExtents,
        disableFlushImmediately = false,
        ensureNoDupBlobs = false,
        addSourceLocationTransform = true,
        maybeSparkIngestionProperties = Some(sparkIngestionProperties))
    }
    assert(
      exception.getMessage == "addSourceLocationTransform cannot be used with Spark ingestion properties that already contain a CSV mapping.")
  }

  s"$testName" should "get created only when GenerateDynamicCsvMapping is specified" in {
    val sparkIngestionProperties = new SparkIngestionProperties()
    val result = KustoDataSourceUtils.validateAndCreateWriteDebugOptions(
      SchemaAdjustmentMode.GenerateDynamicCsvMapping,
      minimalExtentsCountForSplitMergePerNode,
      maxRetriesOnMoveExtents,
      disableFlushImmediately = true,
      ensureNoDupBlobs = true,
      addSourceLocationTransform = true,
      maybeSparkIngestionProperties = Some(sparkIngestionProperties))
    assert(result.addSourceLocationTransform)
    assert(
      result.minimalExtentsCountForSplitMergePerNode == minimalExtentsCountForSplitMergePerNode)
    assert(result.maxRetriesOnMoveExtents == maxRetriesOnMoveExtents)
    assert(result.disableFlushImmediately)
    assert(result.ensureNoDuplicatedBlobs)
  }

  private val invalidSchemaTestCombinations = Table(
    "schemaAdjustmentMode",
    SchemaAdjustmentMode.FailIfNotMatch,
    SchemaAdjustmentMode.NoAdjustment)
  forAll(invalidSchemaTestCombinations) { schemaAdjustmentMode =>
    testName should s"fail when $schemaAdjustmentMode is used with addSourceLocationTransform" in {
      val exception = intercept[IllegalArgumentException] {
        KustoDataSourceUtils.validateAndCreateWriteDebugOptions(
          schemaAdjustmentMode,
          minimalExtentsCountForSplitMergePerNode,
          maxRetriesOnMoveExtents,
          disableFlushImmediately = false,
          ensureNoDupBlobs = false,
          addSourceLocationTransform = true,
          maybeSparkIngestionProperties = None)
      }
      assert(
        exception.getMessage == "addSourceLocationTransform can only be used with GenerateDynamicCsvMapping schema adjustment mode.")
    }
  }
}

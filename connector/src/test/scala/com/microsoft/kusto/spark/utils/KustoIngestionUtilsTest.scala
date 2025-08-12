// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.microsoft.azure.kusto.ingest.TransformationMethod
import com.microsoft.kusto.spark.datasink.{
  SinkTableCreationMode,
  SparkIngestionProperties,
  WriteMode
}
import com.microsoft.kusto.spark.exceptions.SchemaMatchException
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.Tables.Table

class KustoIngestionUtilsTest extends AnyFlatSpec with Matchers {

  private val col1 = "col1"
  private val col2 = "col2"
  private val cslString = "string"
  private val cslInt = "int"
  private val ordinal = "Ordinal"

  private def createJsonNode(name: String, cslType: String): JsonNode = {
    val node = JsonNodeFactory.instance.objectNode()
    node.put("Name", name)
    node.put("CslType", cslType)
    node
  }

  private val fnUnderTest = "setCsvMapping"

  private val allWriteModes =
    Table("writeMode", WriteMode.Queued, WriteMode.Transactional, WriteMode.KustoStreaming)

  allWriteModes.forEvery(writeMode => {
    fnUnderTest should s"generate correct CSV mapping for write mode: $writeMode" in {
      val sourceSchema =
        StructType(Seq(StructField(col1, StringType), StructField(col2, IntegerType)))

      val targetSchema = Array(createJsonNode(col1, cslString), createJsonNode(col2, cslInt))

      val ingestionProperties = new SparkIngestionProperties()

      val mappings = KustoIngestionUtils
        .setCsvMapping(
          writeMode,
          sourceSchema,
          targetSchema,
          ingestionProperties,
          includeSourceLocationTransform = false,
          SinkTableCreationMode.FailIfNotExist)
        .toArray

      mappings.length shouldBe 2
      mappings(0).getColumnName shouldBe col1
      mappings(0).getColumnType shouldBe cslString
      mappings(0).getProperties.get(ordinal) shouldBe "0"
      mappings(1).getColumnName shouldBe col2
      mappings(1).getColumnType shouldBe cslInt
      mappings(1).getProperties.get(ordinal) shouldBe "1"
    }
  })

  allWriteModes.forEvery(writeMode => {
    fnUnderTest should s"generate CSV mapping with source location transform when requested in WriteMode $writeMode" in {
      val sourceSchema = StructType(Seq(StructField(col1, StringType)))
      val targetSchema = Array(
        createJsonNode(col1, cslString),
        createJsonNode(KustoConstants.SourceLocationColumnName, cslString))
      val ingestionProperties = new SparkIngestionProperties()
      val mappings = KustoIngestionUtils
        .setCsvMapping(
          writeMode,
          sourceSchema,
          targetSchema,
          ingestionProperties,
          includeSourceLocationTransform = true,
          SinkTableCreationMode.FailIfNotExist)
        .toArray

      mappings.length shouldBe 2
      mappings.exists(_.getColumnName == "col1") shouldBe true
      mappings.exists(_.getColumnName == KustoConstants.SourceLocationColumnName) shouldBe true
      mappings
        .find(_.getColumnName == KustoConstants.SourceLocationColumnName)
        .get
        .getTransform shouldBe TransformationMethod.SourceLocation
    }
  })

  allWriteModes.forEvery(writeMode => {
    fnUnderTest should s"throw SchemaMatchException when source schema has columns missing in target schema without source transforms in $writeMode mode" in {
      val sourceSchema =
        StructType(Seq(StructField(col1, StringType), StructField(col2, IntegerType)))

      val targetSchema = Array(createJsonNode(col1, cslString))

      val ingestionProperties = new SparkIngestionProperties()

      an[SchemaMatchException] should be thrownBy {
        KustoIngestionUtils.setCsvMapping(
          writeMode,
          sourceSchema,
          targetSchema,
          ingestionProperties,
          includeSourceLocationTransform = false,
          SinkTableCreationMode.FailIfNotExist)
      }
    }
  })

  fnUnderTest should "throw SchemaMatchException if addSourceLocationTransform is true but target schema does not contain SourceLocation column" in {
    val sourceSchema = StructType(Seq(StructField(col1, StringType)))
    val targetSchema = Array(createJsonNode(col1, cslString)) // No SourceLocation column
    val ingestionProperties = new SparkIngestionProperties()

    an[SchemaMatchException] should be thrownBy {
      KustoIngestionUtils.setCsvMapping(
        WriteMode.Queued,
        sourceSchema,
        targetSchema,
        ingestionProperties,
        includeSourceLocationTransform = true,
        SinkTableCreationMode.FailIfNotExist)
    }
  }

  fnUnderTest should "not throw exception when source schema has columns missing in target schema but source transforms are enabled" in {
    val sourceSchema =
      StructType(Seq(StructField(col1, StringType), StructField(col2, IntegerType)))

    val targetSchema = Array(
      createJsonNode(col1, cslString),
      createJsonNode(KustoConstants.SourceLocationColumnName, cslString))

    val ingestionProperties = new SparkIngestionProperties()
    noException should be thrownBy {
      KustoIngestionUtils.setCsvMapping(
        WriteMode.Queued,
        sourceSchema,
        targetSchema,
        ingestionProperties,
        includeSourceLocationTransform = true,
        SinkTableCreationMode.FailIfNotExist)
    }
  }

  fnUnderTest should "generate identity mapping when there is a mapping column in target when add source location is true" in {
    val sourceSchema =
      StructType(Seq(StructField(col1, StringType), StructField(col2, IntegerType)))

    val targetSchema = Array(
      createJsonNode(col1, cslString),
      createJsonNode(KustoConstants.SourceLocationColumnName, cslString))

    val ingestionProperties = new SparkIngestionProperties()
    val mappings = KustoIngestionUtils
      .setCsvMapping(
        WriteMode.Queued,
        sourceSchema,
        targetSchema,
        ingestionProperties,
        includeSourceLocationTransform = true,
        SinkTableCreationMode.CreateIfNotExist)
      .toArray

    mappings.length shouldBe 2
    mappings(0).getColumnName shouldBe col1
    mappings(0).getColumnType shouldBe cslString

    mappings(1).getColumnName shouldBe KustoConstants.SourceLocationColumnName
    mappings(1).getColumnType shouldBe cslString
  }
}

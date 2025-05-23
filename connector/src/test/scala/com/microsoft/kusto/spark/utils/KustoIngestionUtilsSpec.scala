// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.microsoft.kusto.spark.datasink.{SinkTableCreationMode, SparkIngestionProperties}
import com.microsoft.azure.kusto.ingest.{ColumnMapping, TransformationMethod}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import com.microsoft.kusto.spark.exceptions.SchemaMatchException

class KustoIngestionUtilsSpec extends AnyFlatSpec with Matchers {

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

  "setCsvMapping" should "generate correct CSV mapping when source and target schemas match exactly" in {
    val sourceSchema = StructType(Seq(
      StructField(col1, StringType),
      StructField(col2, IntegerType)
    ))

    val targetSchema = Array(
      createJsonNode(col1, cslString),
      createJsonNode(col2, cslInt)
    )

    val ingestionProperties = new SparkIngestionProperties()
    val mappings = KustoIngestionUtils.setCsvMapping(
      sourceSchema,
      targetSchema,
      ingestionProperties,
      includeSourceTransforms = false,
      SinkTableCreationMode.FailIfNotExist
    ).toArray

    mappings.length shouldBe 2
    mappings(0).getColumnName shouldBe col1
    mappings(0).getColumnType shouldBe cslString
    mappings(0).getProperties.get(ordinal) shouldBe "0"
    mappings(1).getColumnName shouldBe col2
    mappings(1).getColumnType shouldBe cslInt
    mappings(1).getProperties.get(ordinal) shouldBe "1"
  }

  it should "generate CSV mapping with source location transform when requested" in {
    val sourceSchema = StructType(Seq(
      StructField(col1, StringType)
    ))

    val targetSchema = Array(
      createJsonNode(col1, cslString)
    )

    val ingestionProperties = new SparkIngestionProperties()
    val mappings = KustoIngestionUtils.setCsvMapping(
      sourceSchema,
      targetSchema,
      ingestionProperties,
      includeSourceTransforms = true,
      SinkTableCreationMode.FailIfNotExist
    ).toArray

    mappings.length shouldBe 2
    mappings.exists(_.getColumnName == "col1") shouldBe true
    mappings.exists(_.getColumnName == KustoConstants.SourceLocationColumnName) shouldBe true
    mappings.find(_.getColumnName == KustoConstants.SourceLocationColumnName).get.getTransform shouldBe TransformationMethod.SourceLocation
  }

  it should "throw SchemaMatchException when source schema has columns missing in target schema without source transforms" in {
    val sourceSchema = StructType(Seq(
      StructField(col1, StringType),
      StructField(col2, IntegerType)
    ))

    val targetSchema = Array(
      createJsonNode(col1, cslString)
    )

    val ingestionProperties = new SparkIngestionProperties()

    an[SchemaMatchException] should be thrownBy {
      KustoIngestionUtils.setCsvMapping(
        sourceSchema,
        targetSchema,
        ingestionProperties,
        includeSourceTransforms = false,
        SinkTableCreationMode.FailIfNotExist
      )
    }
  }

  it should "not throw exception when source schema has columns missing in target schema but source transforms are enabled" in {
    val sourceSchema = StructType(Seq(
      StructField(col1, StringType),
      StructField(col2, IntegerType)
    ))

    val targetSchema = Array(
      createJsonNode(col1, cslString)
    )

    val ingestionProperties = new SparkIngestionProperties()

    noException should be thrownBy {
      KustoIngestionUtils.setCsvMapping(
        sourceSchema,
        targetSchema,
        ingestionProperties,
        includeSourceTransforms = true,
        SinkTableCreationMode.FailIfNotExist
      )
    }
  }

  it should "generate identity mapping when table creation mode is CreateIfNotExist and target schema is empty" in {
    val sourceSchema = StructType(Seq(
      StructField(col1, StringType),
      StructField(col2, IntegerType)
    ))

    val targetSchema = Array(createJsonNode(col1, cslString))

    val ingestionProperties = new SparkIngestionProperties()
    val mappings = KustoIngestionUtils.setCsvMapping(
      sourceSchema,
      targetSchema,
      ingestionProperties,
      includeSourceTransforms = true,
      SinkTableCreationMode.CreateIfNotExist
    ).toArray

    mappings.length shouldBe 2
    mappings(0).getColumnName shouldBe col1
    mappings(0).getColumnType shouldBe cslString

    mappings(1).getColumnName shouldBe KustoConstants.SourceLocationColumnName
    mappings(1).getColumnType shouldBe cslString
  }
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.microsoft.azure.kusto.ingest.{ColumnMapping, TransformationMethod}
import com.microsoft.kusto.spark.datasink.{
  SchemaAdjustmentMode,
  SinkTableCreationMode,
  SparkIngestionProperties
}
import com.microsoft.kusto.spark.datasink.SchemaAdjustmentMode.SchemaAdjustmentMode
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.exceptions.SchemaMatchException
import com.microsoft.kusto.spark.utils.DataTypeMapping.{
  SparkTypeToKustoTypeMap,
  getSparkTypeToKustoTypeMap
}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.StructType

object KustoIngestionUtils {
  lazy private val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
    .setVisibility(PropertyAccessor.ALL, Visibility.NONE)
    .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)

  private[kusto] def adjustSchema(
      mode: SchemaAdjustmentMode,
      sourceSchema: StructType,
      targetSchema: Array[JsonNode],
      ingestionProperties: SparkIngestionProperties,
      tableCreationMode: SinkTableCreationMode,
      kustoCustomDebugWriteOptions: KustoCustomDebugWriteOptions): Unit = {

    val hasCustomTransforms =
      StringUtils.isNotEmpty(kustoCustomDebugWriteOptions.customTransforms)

    val effectiveMode = if (hasCustomTransforms) {
      SchemaAdjustmentMode.GenerateDynamicCsvMapping
    } else {
      mode
    }

    effectiveMode match {
      case SchemaAdjustmentMode.NoAdjustment =>
      case SchemaAdjustmentMode.FailIfNotMatch => forceAdjustSchema(sourceSchema, targetSchema)
      case SchemaAdjustmentMode.GenerateDynamicCsvMapping =>
        val columnMappings = setCsvMapping(
          sourceSchema,
          targetSchema,
          ingestionProperties,
          hasCustomTransforms,
          customTransforms = kustoCustomDebugWriteOptions.customTransforms,
          tableCreationMode)
        val mapping = csvMappingToString(columnMappings.toArray)
        KustoDataSourceUtils.logDebug(
          this.getClass.getSimpleName,
          s"Using CSV mapping : $mapping")
        ingestionProperties.csvMapping = mapping
    }
  }

  private[kusto] def forceAdjustSchema(
      sourceSchema: StructType,
      targetSchema: Array[JsonNode]): Unit = {

    val targetSchemaColumns = targetSchema.map(c => c.get("Name").asText()).toSeq
    val sourceSchemaColumns = sourceSchema.map(c => c.name)

    if (targetSchemaColumns != sourceSchemaColumns) {
      throw SchemaMatchException(
        "Target table schema does not match to DataFrame schema." +
          s"Target columns: ${targetSchemaColumns.mkString(
              ", ")}.Source columns: ${sourceSchemaColumns.mkString(", ")}")
    }
  }

  private[kusto] def setCsvMapping(
      sourceSchema: StructType,
      targetSchema: Array[JsonNode],
      ingestionProperties: SparkIngestionProperties,
      hasCustomTransforms: Boolean = false,
      customTransforms: String = "",
      tableCreationMode: SinkTableCreationMode): Iterable[ColumnMapping] = {
    require(
      ingestionProperties.csvMappingNameReference == null
        || ingestionProperties.csvMappingNameReference.isEmpty,
      "Sink options SparkIngestionProperties.csvMappingNameReference and adjustSchema." +
        "GenerateDynamicCsvMapping are not compatible. Use only one.")

    val targetSchemaColumns = targetSchema
      .map(c =>
        (
          c.get(KustoConstants.Schema.NAME).asText(),
          c.get(KustoConstants.Schema.CSLTYPE).asText()))
      .toMap
    val sourceSchemaColumns = sourceSchema.fields.zipWithIndex.map(c => (c._1.name, c._2)).toMap
    /* This was created for the case where CreateTable is used along with Create CSV mapping.
    There are 2 options:
    either to not have a mapping or create an explicit identity mapping. Since GenerateCSVMapping is requested explicitly
    creating an identity mapping made the most appropriate fit
     */
    val sourceSchemaColumnTypes =
      if (tableCreationMode == SinkTableCreationMode.CreateIfNotExist) {
        sourceSchema.fields
          .map(field => (field.name, getSparkTypeToKustoTypeMap(field.dataType)))
          .toMap
      } else {
        Map.empty[String, String]
      }
    val notFoundSourceColumns =
      sourceSchemaColumns.filter(c => !targetSchemaColumns.contains(c._1)).keys
    if (notFoundSourceColumns.nonEmpty && targetSchema != null && targetSchema.nonEmpty) {
      throw SchemaMatchException(
        s"Source schema has columns that are not present in the target: ${notFoundSourceColumns.mkString(", ")}.")
    }

    val columnMappingsBase = sourceSchemaColumns
      .map(sourceColumn => {
        val targetDataType = targetSchemaColumns.get(sourceColumn._1)
        val columnMapping = targetDataType match {
          case Some(targetMapping) => new ColumnMapping(sourceColumn._1, targetMapping)
          // Get the datatype by column or fallback to string
          case None =>
            new ColumnMapping(
              sourceColumn._1,
              sourceSchemaColumnTypes.getOrElse(sourceColumn._1, "string"))
        }
        columnMapping.setOrdinal(sourceColumn._2)
        columnMapping
      })
    if (hasCustomTransforms) {
      val customTransformsArr =
        objectMapper.readValue(customTransforms, classOf[Array[CustomTransform]])
      val additionalMappings = customTransformsArr.map(customTransform => {
        val columnMapping =
          new ColumnMapping(customTransform.targetColumnName, customTransform.cslDataType)
        // columnMapping.setOrdinal(customTransform.ordinal)
        columnMapping.setTransform(
          TransformationMethod.valueOf(customTransform.transformationMethod))
        columnMapping
      })
      columnMappingsBase ++ additionalMappings
    } else {
      columnMappingsBase
    }
  }

  // Returns the CSV mapping as string
  private[kusto] def csvMappingToString(columnMappings: Array[ColumnMapping]): String = {
    if (columnMappings == null) {
      ""
    } else {
      objectMapper.writeValueAsString(columnMappings)
    }
  }
}

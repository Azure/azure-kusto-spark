//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.microsoft.azure.kusto.ingest.ColumnMapping
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
import org.apache.spark.sql.types.StructType

object KustoIngestionUtils {
  private[kusto] def adjustSchema(
      mode: SchemaAdjustmentMode,
      sourceSchema: StructType,
      targetSchema: Array[JsonNode],
      ingestionProperties: SparkIngestionProperties,
      tableCreationMode: SinkTableCreationMode): Unit = {

    mode match {
      case SchemaAdjustmentMode.NoAdjustment =>
      case SchemaAdjustmentMode.FailIfNotMatch => forceAdjustSchema(sourceSchema, targetSchema)
      case SchemaAdjustmentMode.GenerateDynamicCsvMapping =>
        setCsvMapping(sourceSchema, targetSchema, ingestionProperties, tableCreationMode)
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
      tableCreationMode: SinkTableCreationMode): Unit = {
    require(
      ingestionProperties.csvMappingNameReference == null || ingestionProperties.csvMappingNameReference.isEmpty,
      "Sink options SparkIngestionProperties.csvMappingNameReference and adjustSchema.GenerateDynamicCsvMapping are not compatible. Use only one.")

    val targetSchemaColumns = targetSchema
      .map(c =>
        (
          c.get(KustoConstants.Schema.NAME).asText(),
          c.get(KustoConstants.Schema.CSLTYPE).asText()))
      .toMap
    val sourceSchemaColumns = sourceSchema.fields.zipWithIndex.map(c => (c._1.name, c._2)).toMap
    /* This was created for the case where CreateTable is used along with Create CSV mapping. There are 2 options
    either to not have a mapping or create an explicit identity mapping. Since GenerateCSVMapping is requested explicitly
    creating an identity mapping made the most appropriate fit */
    val sourceSchemaColumnTypes =
      if (tableCreationMode == SinkTableCreationMode.CreateIfNotExist)
        sourceSchema.fields
          .map(field => (field.name, getSparkTypeToKustoTypeMap(field.dataType)))
          .toMap
      else Map.empty[String, String]
    val notFoundSourceColumns =
      sourceSchemaColumns.filter(c => !targetSchemaColumns.contains(c._1)).keys
    if (notFoundSourceColumns.nonEmpty && targetSchema != null && targetSchema.nonEmpty) {
      throw SchemaMatchException(
        s"Source schema has columns that are not present in the target: ${notFoundSourceColumns.mkString(", ")}.")
    }

    val columnMappingReset = sourceSchemaColumns
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
    val mapping = csvMappingToString(columnMappingReset.toArray)
    KustoDataSourceUtils.logDebug(this.getClass.getSimpleName, s"Using CSV mapping : $mapping")
    ingestionProperties.csvMapping = mapping
  }

  // Returns the CSV mapping as string
  private[kusto] def csvMappingToString(columnMappings: Array[ColumnMapping]): String = {
    if (columnMappings == null) {
      ""
    } else {
      val objectMapper = new ObjectMapper
      objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE)
      objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      objectMapper.writeValueAsString(columnMappings)
    }
  }
}

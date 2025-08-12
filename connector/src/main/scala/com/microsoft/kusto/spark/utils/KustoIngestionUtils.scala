// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.microsoft.azure.kusto.ingest.{ColumnMapping, TransformationMethod}
import com.microsoft.kusto.spark.datasink.{
  SchemaAdjustmentMode,
  SinkTableCreationMode,
  SparkIngestionProperties,
  WriteMode
}
import com.microsoft.kusto.spark.datasink.SchemaAdjustmentMode.SchemaAdjustmentMode
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.datasink.WriteMode.WriteMode
import com.microsoft.kusto.spark.exceptions.SchemaMatchException
import com.microsoft.kusto.spark.utils.DataTypeMapping.getSparkTypeToKustoTypeMap
import org.apache.spark.sql.types.StructType

object KustoIngestionUtils {
  private[kusto] def adjustSchema(
      writeMode: WriteMode,
      schemaAdjustmentMode: SchemaAdjustmentMode,
      sourceSchema: StructType,
      targetSchema: Array[JsonNode],
      ingestionProperties: SparkIngestionProperties,
      tableCreationMode: SinkTableCreationMode,
      kustoCustomDebugWriteOptions: KustoCustomDebugWriteOptions): Unit = {

    val effectiveMode = if (kustoCustomDebugWriteOptions.addSourceLocationTransform) {
      SchemaAdjustmentMode.GenerateDynamicCsvMapping
    } else {
      schemaAdjustmentMode
    }

    effectiveMode match {
      case SchemaAdjustmentMode.NoAdjustment =>
      case SchemaAdjustmentMode.FailIfNotMatch => forceAdjustSchema(sourceSchema, targetSchema)
      case SchemaAdjustmentMode.GenerateDynamicCsvMapping =>
        val columnMappings = setCsvMapping(
          writeMode,
          sourceSchema,
          targetSchema,
          ingestionProperties,
          includeSourceLocationTransform =
            kustoCustomDebugWriteOptions.addSourceLocationTransform,
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
      throw new SchemaMatchException(
        "Target table schema does not match to DataFrame schema." +
          s"Target columns: ${targetSchemaColumns.mkString(
              ", ")}.Source columns: ${sourceSchemaColumns.mkString(", ")}")
    }
  }

  private[kusto] def setCsvMapping(
      writeMode: WriteMode,
      sourceSchema: StructType,
      targetSchema: Array[JsonNode],
      ingestionProperties: SparkIngestionProperties,
      includeSourceLocationTransform: Boolean = false,
      tableCreationMode: SinkTableCreationMode): Iterable[ColumnMapping] = {

    validateMappingParameters(ingestionProperties)
    val targetSchemaColumns = extractTargetSchemaColumns(targetSchema)
    val sourceSchemaColumns = extractSourceSchemaColumns(sourceSchema)
    val sourceSchemaColumnTypes = extractSourceSchemaTypes(sourceSchema, tableCreationMode)

    val notFoundSourceColumns = findMissingSourceColumns(sourceSchemaColumns, targetSchemaColumns)
    // Why should validation be done only if the target schema is not empty?
    // If the target schema is empty, it means that the table is not created yet, so we don't need to validate
    // the schema compatibility. The table will be created with the source schema, and the validation will be done
    if (targetSchemaColumns.nonEmpty && tableCreationMode != SinkTableCreationMode.CreateIfNotExist) {
      validateSchemaCompatibility(
        writeMode,
        notFoundSourceColumns,
        targetSchemaColumns,
        includeSourceLocationTransform)
    }
    val columnMappingsBase = createBaseMappings(
      tableCreationMode,
      sourceSchemaColumns,
      notFoundSourceColumns,
      targetSchemaColumns,
      sourceSchemaColumnTypes)

    if (includeSourceLocationTransform) {
      addSourceLocationMapping(columnMappingsBase)
    } else {
      columnMappingsBase
    }
  }

  private def validateMappingParameters(ingestionProperties: SparkIngestionProperties): Unit = {
    require(
      ingestionProperties.csvMappingNameReference == null
        || ingestionProperties.csvMappingNameReference.isEmpty,
      "Sink options SparkIngestionProperties.csvMappingNameReference and adjustSchema." +
        "GenerateDynamicCsvMapping are not compatible. Use only one.")
  }

  private def extractTargetSchemaColumns(targetSchema: Array[JsonNode]): Map[String, String] = {
    targetSchema
      .map(c =>
        (
          c.get(KustoConstants.Schema.NAME).asText(),
          c.get(KustoConstants.Schema.CSLTYPE).asText()))
      .toMap
  }

  private def extractSourceSchemaColumns(sourceSchema: StructType): Map[String, Int] = {
    sourceSchema.fields.zipWithIndex.map(c => (c._1.name, c._2)).toMap
  }

  private def extractSourceSchemaTypes(
      sourceSchema: StructType,
      tableCreationMode: SinkTableCreationMode): Map[String, String] = {
    if (tableCreationMode == SinkTableCreationMode.CreateIfNotExist) {
      sourceSchema.fields
        .map(field => (field.name, getSparkTypeToKustoTypeMap(field.dataType)))
        .toMap
    } else {
      Map.empty[String, String]
    }
  }

  private def findMissingSourceColumns(
      sourceSchemaColumns: Map[String, Int],
      targetSchemaColumns: Map[String, String]): Set[String] = {
    sourceSchemaColumns.filter(c => !targetSchemaColumns.contains(c._1)).keys.toSet
  }

  private val cslStringType = "string"

  private def validateSchemaCompatibility(
      writeMode: WriteMode,
      notFoundSourceColumns: Set[String],
      targetSchemaColumns: Map[String, String],
      includeSourceLocationTransform: Boolean): Unit = {
    // If the transform is included, we need to ensure that the target schema contains the source location column and
    // that it is of type string.
    if (includeSourceLocationTransform && (!targetSchemaColumns.contains(
        KustoConstants.SourceLocationColumnName) ||
        !cslStringType.equalsIgnoreCase(
          targetSchemaColumns.getOrElse(KustoConstants.SourceLocationColumnName, "")))) {
      throw SchemaMatchException(
        "addSourceLocationTransform is set to true, but the target schema does not contain the " +
          s"column '${KustoConstants.SourceLocationColumnName}' of type '${targetSchemaColumns
              .get(KustoConstants.SourceLocationColumnName)} " +
          s"Please ensure that the target table has this column defined.")
    }

    if (notFoundSourceColumns.nonEmpty && targetSchemaColumns != null && targetSchemaColumns.nonEmpty) {
      // The drift between source and target schema is allowed for queued ingestion only if includeSourceLocationTransform is true
      if (writeMode == WriteMode.Queued && includeSourceLocationTransform) {
        KustoDataSourceUtils.logWarn(
          this.getClass.getSimpleName,
          s"Source schema has columns that are not present in the target: ${notFoundSourceColumns.mkString(",")}. " +
            s"Since the option 'addSourceLocationTransform' is set to true, the ingestion will continue." +
            "In order to preserve behavior of 'no-mapping like behavior for the client where we do not fail " +
            "on schema drift as no validation is made")
      } else {
        throw SchemaMatchException(
          s"Source schema has columns that are not present in the target: ${notFoundSourceColumns
              .mkString(", ")}.")
      }
    }
  }

  private def createBaseMappings(
      tableCreationMode: SinkTableCreationMode,
      sourceSchemaColumns: Map[String, Int],
      notFoundSourceColumns: Set[String],
      targetSchemaColumns: Map[String, String],
      sourceSchemaColumnTypes: Map[String, String]): Iterable[ColumnMapping] = {

    // If the target schema is empty, we assume that the table is not created yet. This combined with CreateIfNotExist
    // implies the table will be created this run. So there is no filter to be applied on source columns.

    val predicate: String => Boolean = srcCol =>
      (targetSchemaColumns.isEmpty && tableCreationMode == SinkTableCreationMode.CreateIfNotExist) || !notFoundSourceColumns
        .contains(srcCol)

    sourceSchemaColumns
      .filter(sourceColumn => predicate(sourceColumn._1))
      .map(sourceColumn => {
        val targetDataType = targetSchemaColumns.get(sourceColumn._1)
        val columnMapping = targetDataType match {
          case Some(targetMapping) => new ColumnMapping(sourceColumn._1, targetMapping)
          // Get the datatype by column or fallback to string
          case None =>
            new ColumnMapping(
              sourceColumn._1,
              sourceSchemaColumnTypes.getOrElse(sourceColumn._1, cslStringType))
        }
        columnMapping.setOrdinal(sourceColumn._2)
        columnMapping
      })
  }

  private def addSourceLocationMapping(
      columnMappingsBase: Iterable[ColumnMapping]): Iterable[ColumnMapping] = {
    val sourceLocationTransform =
      new ColumnMapping(KustoConstants.SourceLocationColumnName, cslStringType)
    sourceLocationTransform.setTransform(TransformationMethod.SourceLocation)
    columnMappingsBase ++ Seq(sourceLocationTransform)
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

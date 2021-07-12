package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind
import com.microsoft.azure.kusto.ingest.{ColumnMapping, IngestionProperties}
import com.microsoft.azure.kusto.ingest.IngestionProperties.DATA_FORMAT
import com.microsoft.kusto.spark.datasink.SchemaAdjustmentMode
import com.microsoft.kusto.spark.datasink.SchemaAdjustmentMode.SchemaAdjustmentMode
import org.apache.spark.sql.types.StructType
import org.json.JSONObject

object KustoIngestionUtils {

  private[kusto] def adjustSchema(mode: SchemaAdjustmentMode ,sourceSchema: StructType, targetSchema: Array[JSONObject], ingestionProperties: IngestionProperties) : Unit = {

    mode match {
      case SchemaAdjustmentMode.NoAdjustment =>
      case SchemaAdjustmentMode.FailIfNotMatch => forceAdjustSchema(sourceSchema, targetSchema)
      case SchemaAdjustmentMode.GenerateDynamicCsvMapping => setCsvMapping(sourceSchema, targetSchema, ingestionProperties)
    }

  }

  private[kusto] def forceAdjustSchema(sourceSchema: StructType, targetSchema: Array[JSONObject]) : Unit ={

    val targetSchemaColumns = targetSchema.map(c=> c.getString("Name")).toSeq
    val sourceSchemaColumns = sourceSchema.map(c=> c.name)

    if (targetSchemaColumns != sourceSchemaColumns) {
      throw new RuntimeException(s"Target table schema does not match to DataFrame schema. " +
        s"Target columns: ${targetSchemaColumns.mkString(", ")}. " +
        s"Source columns: ${sourceSchemaColumns.mkString(", ")}. ")
    }
  }

  private[kusto] def setCsvMapping(sourceSchema: StructType, targetSchema: Array[JSONObject], ingestionProperties: IngestionProperties): Unit = {

    val targetSchemaColumns = targetSchema.map(c => (c.getString("Name"),c.getString("CslType"))).toMap
    val sourceSchemaColumns = sourceSchema.fields.zipWithIndex.map(c => (c._1.name, c._2 )).toMap
    val notFoundSourceColumns = sourceSchemaColumns.filter(c => !targetSchemaColumns.contains(c._1)).keys
    if(notFoundSourceColumns.nonEmpty)
      throw new RuntimeException(s"Source schema has columns that are not present in the target: ${notFoundSourceColumns.mkString(", ")}.")

    val columnMappingReset = sourceSchemaColumns
      .map(sourceColumn => {
        val targetDataType = targetSchemaColumns.get(sourceColumn._1)
        val columnMapping = new ColumnMapping(sourceColumn._1, targetDataType.get)
        columnMapping.setOrdinal(sourceColumn._2)
        columnMapping
      })

    ingestionProperties.setIngestionMapping(columnMappingReset.toArray, IngestionMappingKind.Csv)
    ingestionProperties.setDataFormat(DATA_FORMAT.csv)

  }

  private[kusto] def csvMappingToString(columnMappings: Array[ColumnMapping]) : String = {

    if(columnMappings == null)
      return ""
    val objectMapper = new ObjectMapper
    objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE)
    objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
    objectMapper.writeValueAsString(columnMappings)

  }
}

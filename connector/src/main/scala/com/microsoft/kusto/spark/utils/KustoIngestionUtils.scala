package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.kusto.ingest.ColumnMapping
import com.microsoft.kusto.spark.datasink.SchemaAdjustmentMode.{FailIfNotMatch, GenerateDynamicParquetMapping, NoAdjustment, SchemaAdjustmentMode}
import com.microsoft.kusto.spark.datasink.SparkIngestionProperties
import com.microsoft.kusto.spark.exceptions.SchemaMatchException
import org.apache.spark.sql.types.StructType
import org.json.JSONObject

object KustoIngestionUtils {

  private[kusto] def adjustSchema(mode: SchemaAdjustmentMode,
                                  sourceSchema: StructType,
                                  targetSchema: Array[JSONObject],
                                  ingestionProperties: SparkIngestionProperties): SparkIngestionProperties = {

    mode match {
      case NoAdjustment => ingestionProperties
      case FailIfNotMatch => forceAdjustSchema(sourceSchema, targetSchema,ingestionProperties)
      case GenerateDynamicParquetMapping =>
        ingestionProperties.copy(maybeParquetMapping = Some(setParquetMapping(sourceSchema, targetSchema)))
    }
  }

  private[kusto] def forceAdjustSchema(sourceSchema: StructType, targetSchema: Array[JSONObject],
                                       ingestionProperties: SparkIngestionProperties) = {
    val targetSchemaColumns = targetSchema.map(c => c.getString("Name")).toSeq
    val sourceSchemaColumns = sourceSchema.map(c => c.name)
    if (targetSchemaColumns != sourceSchemaColumns) {
      throw SchemaMatchException("Target table schema does not match to DataFrame schema." +
        s"Target columns: ${targetSchemaColumns.mkString(", ")}. " +
        s"Source columns: ${sourceSchemaColumns.mkString(", ")}. ")
    }
    else {
      ingestionProperties
    }
  }

  private[kusto] def setParquetMapping(sourceSchema: StructType,
                                       targetSchema: Array[JSONObject]): String = {
    /*
    TODO
    require(ingestionProperties.maybeParquetMapping == null || ingestionProperties.parquetMappingNameReference.isEmpty,
      "Sink options SparkIngestionProperties.parquetMappingNameReference and adjustSchema.GenerateDynamicParquetMapping are not compatible. Use only one.")
     */
    val targetSchemaColumns = targetSchema.map(c => (c.getString("Name"), c.getString("CslType"))).toMap
    val sourceSchemaColumns = sourceSchema.fields.zipWithIndex.map(c => (c._1.name, c._2)).toMap
    val notFoundSourceColumns = sourceSchemaColumns.filter(c => !targetSchemaColumns.contains(c._1)).keys
    if (notFoundSourceColumns.nonEmpty) {
      throw SchemaMatchException(s"Source schema has columns that are not present in the target: ${notFoundSourceColumns.mkString(", ")}.")
    }
    val columnMappingReset = sourceSchemaColumns
      .map(sourceColumn => {
        val targetDataType = targetSchemaColumns.get(sourceColumn._1)
        val columnMapping = new ColumnMapping(sourceColumn._1, targetDataType.get)
        columnMapping.setPath(s"$$.${sourceColumn._1}")
        columnMapping
      })
    mappingToString(columnMappingReset.toArray)
  }

  private[kusto] def mappingToString(columnMappings: Array[ColumnMapping]): String = {
    if (Option(columnMappings).isDefined || columnMappings.nonEmpty) {
      val objectMapper = new ObjectMapper
      objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE)
      objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      objectMapper.writeValueAsString(columnMappings)
    }
    else {
      ""
    }
  }
}

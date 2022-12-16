package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, BaseJsonNode}
import com.microsoft.azure.kusto.ingest.ColumnMapping
import com.microsoft.kusto.spark.datasink.{SchemaAdjustmentMode, SinkTableCreationMode, SparkIngestionProperties}
import com.microsoft.kusto.spark.datasink.SchemaAdjustmentMode.SchemaAdjustmentMode
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.exceptions.SchemaMatchException
import com.microsoft.kusto.spark.utils.DataTypeMapping.getSparkTypeToKustoTypeMap
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.StructType
import org.json.JSONObject

import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object KustoIngestionUtils {
  private val objectMapper = new ObjectMapper().setVisibility(PropertyAccessor.ALL, Visibility.NONE)
    .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)

  private[kusto] def adjustSchema(mode: SchemaAdjustmentMode,
                                  sourceSchema: StructType,
                                  targetSchema: Array[JSONObject],
                                  ingestionProperties: SparkIngestionProperties , tableCreationMode: SinkTableCreationMode) : SparkIngestionProperties = {

    mode match {
      case SchemaAdjustmentMode.NoAdjustment => ingestionProperties
      case SchemaAdjustmentMode.FailIfNotMatch => forceAdjustSchema(sourceSchema, targetSchema,ingestionProperties)
      case SchemaAdjustmentMode.GenerateDynamicParquetMapping =>
        setParquetMapping(sourceSchema, targetSchema, ingestionProperties, tableCreationMode)
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
                                       targetSchema: Array[JSONObject],
                                       ingestionProperties: SparkIngestionProperties,tableCreationMode: SinkTableCreationMode): SparkIngestionProperties = {
    /*
    TODO
    require(ingestionProperties.maybeParquetMapping == null || ingestionProperties.parquetMappingNameReference.isEmpty,
      "Sink options SparkIngestionProperties.parquetMappingNameReference and adjustSchema.GenerateDynamicParquetMapping are not compatible. Use only one.")
     */
    val targetSchemaColumns = targetSchema.map(c => (c.getString("Name"), c.getString("CslType"))).toMap
    val sourceSchemaColumns = sourceSchema.fields.zipWithIndex.map(c => (c._1.name, c._2)).toMap
    /* This was created for the case where CreateTable is used along with Create Parquet mapping. There are 2 options
    either to not have a mapping or create an explicit identity mapping. Since GenerateParquetMapping is requested explicitly
    creating an identity mapping made the most appropriate fit */
    val sourceSchemaColumnTypes = if(tableCreationMode == SinkTableCreationMode.CreateIfNotExist) sourceSchema.fields.map(field=>(field.name,getSparkTypeToKustoTypeMap(field.dataType))).toMap else  Map.empty[String,String]
    val notFoundSourceColumns = sourceSchemaColumns.filter(c => !targetSchemaColumns.contains(c._1)).keys
    if(notFoundSourceColumns.nonEmpty && targetSchema!=null && targetSchema.nonEmpty) {
      throw SchemaMatchException(s"Source schema has columns that are not present in the target: ${notFoundSourceColumns.mkString(", ")}.")
    }

    val columnMappingReset = sourceSchemaColumns
      .map(sourceColumn => {
        val targetDataType = targetSchemaColumns.get(sourceColumn._1)
        val columnMapping = targetDataType match {
          case Some(targetMapping) => new ColumnMapping(sourceColumn._1, targetMapping)
          // Get the datatype by column or fallback to string
          case None => new ColumnMapping(sourceColumn._1, sourceSchemaColumnTypes.getOrElse(sourceColumn._1, "string"))
        }
        columnMapping.setPath(s"$$.${sourceColumn._1}")
        columnMapping
      }).toList
    ingestionProperties.copy(maybeParquetMapping = Some(mappingToString(columnMappingReset.toArray)))
        // replaceAll("columnName", "Column")))
  }

  private[kusto] def mappingToString(columnMappings: Array[ColumnMapping]): String = {
    if (Option(columnMappings).isDefined || columnMappings.nonEmpty) {
      objectMapper.writeValueAsString(columnMappings)
    }
    else {
      ""
    }
  }

  private[kusto] def stringToMapping(columnMapping: String): Array[ColumnMapping] = {
    if (StringUtils.isNotEmpty(columnMapping)) {
      val arrayBuffer = new ArrayBuffer[ColumnMapping]()
      val jsonObject = objectMapper.readTree(columnMapping)
      jsonObject match {
        case mappings: ArrayNode  =>
          val elements = mappings.elements()
          while(elements.hasNext){
            val jsonNode = elements.next()
            val columnMapping: ColumnMapping = columnMappingFromJson(jsonNode)
            arrayBuffer.append(columnMapping)
        }
          arrayBuffer.toArray
        case _ => Array(columnMappingFromJson(jsonObject))
      }
    }
    else {
      Array.empty
    }
  }

  private def columnMappingFromJson(element: JsonNode) = {
    val columnName = element.get("columnName").toString.replace("\"","")
    val columnType = element.get("columnType").toString.replace("\"","")
    val propertyString = element.get("properties")
    val propertyMap: util.Map[String, String] = objectMapper.convertValue(propertyString, new TypeReference[util.Map[String, String]]() {})
    val columnMapping = new ColumnMapping(columnName, columnType, propertyMap)
    columnMapping
  }
}

package com.microsoft.kusto.spark.datasink

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind
import com.microsoft.azure.kusto.ingest.{ColumnMapping, IngestionProperties}
import com.microsoft.kusto.spark.utils.KustoIngestionUtils.stringToMapping

import java.time.Instant
import scala.collection.JavaConverters._
import scala.collection.mutable

@JsonIgnoreProperties(Array("maybeParquetMappingCols"))
final case class SparkIngestionProperties(flushImmediately: Boolean = false,
                                          dropByTags: List[String] = List.empty,
                                          ingestByTags: List[String] = List.empty,
                                          additionalTags: List[String] = List.empty,
                                          ingestIfNotExists: List[String] = List.empty,
                                          maybeCreationTime: Option[Instant] = None,
                                          maybeParquetMapping: Option[String] = None,
                                          maybeParquetMappingCols: List[ColumnMapping] = List.empty
                                         ) extends Serializable

object SparkIngestionProperties {
  private final val mapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  def ingestionPropertiesToString(sparkIngestionProperties: SparkIngestionProperties): String = {
    mapper.writeValueAsString(sparkIngestionProperties)
  }

  def toIngestionProperties(database: String, table: String,
                            sparkIngestionProperties: SparkIngestionProperties): IngestionProperties = {
    val ingestionProperties = new IngestionProperties(database, table)
    val additionalProperties = new mutable.HashMap[String, String]()
    ingestionProperties.setFlushImmediately(true)
    ingestionProperties.setDropByTags(sparkIngestionProperties.dropByTags.asJava)
    ingestionProperties.setIngestByTags(sparkIngestionProperties.ingestByTags.asJava)
    ingestionProperties.setAdditionalTags(sparkIngestionProperties.additionalTags.asJava)
    ingestionProperties.setIngestIfNotExists(sparkIngestionProperties.ingestIfNotExists.asJava)
    sparkIngestionProperties.maybeCreationTime.map(creationTime => creationTime.toString).
      foreach(creationTime => additionalProperties.put("creationTime", creationTime))
    sparkIngestionProperties.maybeParquetMapping.foreach(parquetMapping => {
      additionalProperties.put("ingestionMapping", parquetMapping)
      additionalProperties.put("ingestionMappingType", IngestionMappingKind.PARQUET.getKustoValue)
      ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PARQUET)
      val mapping = stringToMapping(parquetMapping)
      ingestionProperties.setIngestionMapping(mapping, IngestionMappingKind.PARQUET)
    })
    /*
    sparkIngestionProperties.maybeParquetMappingNameReference.foreach(parquetMappingReference => {
      ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PARQUET)
      ingestionProperties.getIngestionMapping.setIngestionMappingReference(parquetMappingReference,
        IngestionMappingKind.PARQUET)
    })
     */
    ingestionProperties.setAdditionalProperties(additionalProperties.asJava)
    ingestionProperties
  }

  private[kusto] def ingestionPropertiesFromString(json: String): SparkIngestionProperties = {
    mapper.readValue(json, classOf[SparkIngestionProperties])
  }
}

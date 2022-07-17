package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind

import java.util
import com.microsoft.azure.kusto.ingest.{IngestionMapping, IngestionProperties}
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility
import org.codehaus.jackson.annotate.JsonMethod
import org.codehaus.jackson.map.ObjectMapper
import org.joda.time.DateTime

class SparkIngestionProperties(var flushImmediately: Boolean = false,
                               var dropByTags: util.ArrayList[String] = null,
                               var ingestByTags: util.ArrayList[String] = null,
                               var additionalTags: util.ArrayList[String] = null,
                               var ingestIfNotExists: util.ArrayList[String] = null,
                               var creationTime: DateTime = null,
                               var csvMapping: String = null,
                               var csvMappingNameReference: String = null){
  // C'tor for serialization
  def this(){
    this(false)
  }

  override def toString: String = {
    new ObjectMapper().setVisibility(JsonMethod.FIELD, Visibility.ANY)
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(this)
  }

  def toIngestionProperties(database: String, table: String): IngestionProperties ={
    val ingestionProperties = new IngestionProperties(database, table)
    val additionalProperties = new util.HashMap[String, String]()

    if (this.flushImmediately){
      ingestionProperties.setFlushImmediately(true)
    }

    if (this.dropByTags != null) {
      ingestionProperties.setDropByTags(this.dropByTags)
    }

    if (this.ingestByTags != null) {
      ingestionProperties.setIngestByTags(this.ingestByTags)
    }

    if (this.additionalTags != null) {
      ingestionProperties.setAdditionalTags(this.additionalTags)
    }

    if (this.ingestIfNotExists != null) {
      ingestionProperties.setIngestIfNotExists(this.ingestIfNotExists)
    }

    if (this.creationTime != null) {
      additionalProperties.put("creationTime", this.creationTime.toString())
    }

    if (this.csvMapping != null) {
      additionalProperties.put("ingestionMapping", this.csvMapping)
      additionalProperties.put("ingestionMappingType", IngestionMappingKind.CSV.getKustoValue)
    }

    if (this.csvMappingNameReference != null) {
      ingestionProperties.setIngestionMapping(new IngestionMapping(this.csvMappingNameReference, IngestionMapping.IngestionMappingKind.CSV))
    }

    ingestionProperties.setAdditionalProperties(additionalProperties)
    ingestionProperties
  }
}

object SparkIngestionProperties {
  def cloneIngestionProperties(ingestionProperties: IngestionProperties, destinationTable: Option[String] = None): IngestionProperties = {
    val cloned = new IngestionProperties(ingestionProperties.getDatabaseName,
      if(destinationTable.isDefined) destinationTable.get else ingestionProperties.getTableName)
    cloned.setReportLevel(ingestionProperties.getReportLevel)
    cloned.setReportMethod(ingestionProperties.getReportMethod)
    cloned.setAdditionalTags(ingestionProperties.getAdditionalTags)
    cloned.setDropByTags(ingestionProperties.getDropByTags)
    cloned.setIngestByTags(ingestionProperties.getIngestByTags)
    cloned.setIngestIfNotExists(ingestionProperties.getIngestIfNotExists)
    cloned.setDataFormat(ingestionProperties.getDataFormat)
    cloned.setIngestionMapping(ingestionProperties.getIngestionMapping)
    cloned
  }

  private[kusto] def fromString(json: String): SparkIngestionProperties = {
    new ObjectMapper().setVisibility(JsonMethod.FIELD, Visibility.ANY).readValue(json, classOf[SparkIngestionProperties])
  }
}

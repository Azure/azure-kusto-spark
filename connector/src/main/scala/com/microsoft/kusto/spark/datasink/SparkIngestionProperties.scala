package com.microsoft.kusto.spark.datasink

import java.util

import com.microsoft.azure.kusto.ingest.IngestionProperties
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility
import org.codehaus.jackson.annotate.JsonMethod
import org.codehaus.jackson.map.ObjectMapper
import org.joda.time.DateTime

class SparkIngestionProperties {
  var flushImmediately: Boolean = false
  var dropByTags: util.ArrayList[String] = _
  var ingestByTags: util.ArrayList[String] = _
  var additionalTags: util.ArrayList[String] = _
  var ingestIfNotExists: util.ArrayList[String] = _
  var creationTime: DateTime = _
  var csvMapping: String = _
  var csvMappingNameReference: String = _

  override def toString: String = {
    new ObjectMapper().setVisibility(JsonMethod.FIELD, Visibility.ANY)
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(this)
  }

  def toIngestionProperties(database: String, table: String): IngestionProperties ={
    val ingestionProperties = new IngestionProperties(database, table)
    val additionalProperties = new util.HashMap[String, String]()
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
      additionalProperties.put("csvMapping", this.csvMapping)
    }

    if (this.csvMappingNameReference != null) {
      ingestionProperties.setCsvMappingName(this.csvMappingNameReference)
    }

    ingestionProperties.setAdditionalProperties(additionalProperties)
    ingestionProperties
  }
}

object SparkIngestionProperties {
  private[kusto] def fromString(json: String): SparkIngestionProperties = {
    new ObjectMapper().setVisibility(JsonMethod.FIELD, Visibility.ANY).readValue(json, classOf[SparkIngestionProperties])
  }
}

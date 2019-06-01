package com.microsoft.kusto.spark.datasink

import java.util

import com.microsoft.azure.kusto.ingest.IngestionProperties
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility
import org.codehaus.jackson.annotate.JsonMethod
import org.codehaus.jackson.map.ObjectMapper

class SparkIngestionProperties {
  var flushImmediately: Boolean = false
  var dropByTags: util.ArrayList[String] = _
  var ingestByTags: util.ArrayList[String] = _
  var additionalTags: util.ArrayList[String] = _
  var ingestIfNotExists: util.ArrayList[String] = _
  var creationTime: String = _ //TODO: should be date? what are the ok formats?
  var csvMapping: String = _
  var csvMappingNameReference: String = _

  override def toString: String = {
    new ObjectMapper().setVisibility(JsonMethod.FIELD, Visibility.ANY)
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(this)
  }
}

object SparkIngestionProperties {
  private[kusto] def fromStringToIngestionProperties(json: String, database: String, table: String) = {
    new ObjectMapper().setVisibility(JsonMethod.FIELD, Visibility.ANY).readValue(json, classOf[SparkIngestionProperties])

    val sparkIngestionProperties = new ObjectMapper().setVisibility(JsonMethod.FIELD, Visibility.ANY).readValue(json, classOf[SparkIngestionProperties])
    val ingestionProperties = new IngestionProperties(database, table)
    val additionalProperties = new util.HashMap[String, String]()
    if (sparkIngestionProperties.dropByTags != null) {
      ingestionProperties.setDropByTags(sparkIngestionProperties.dropByTags)
    }

    if (sparkIngestionProperties.ingestByTags != null) {
      ingestionProperties.setIngestByTags(sparkIngestionProperties.ingestByTags)
    }

    if (sparkIngestionProperties.additionalTags != null) {
      ingestionProperties.setAdditionalTags(sparkIngestionProperties.additionalTags)
    }

    if (sparkIngestionProperties.ingestIfNotExists != null) {
      ingestionProperties.setIngestIfNotExists(sparkIngestionProperties.ingestIfNotExists)
    }

    if (sparkIngestionProperties.creationTime != null) {
      additionalProperties.put("creationTime", sparkIngestionProperties.creationTime)
    }

    if (sparkIngestionProperties.csvMapping != null) {
      additionalProperties.put("csvMapping", sparkIngestionProperties.csvMapping)
    }

    if (sparkIngestionProperties.csvMappingNameReference != null) {
      ingestionProperties.setCsvMappingName(sparkIngestionProperties.csvMappingNameReference)
    }

    ingestionProperties.setAdditionalProperties(additionalProperties)
    ingestionProperties
  }
}

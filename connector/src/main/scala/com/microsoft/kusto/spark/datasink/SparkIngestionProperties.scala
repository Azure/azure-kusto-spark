// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind

import java.util
import com.microsoft.azure.kusto.ingest.{IngestionMapping, IngestionProperties}
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils

import java.time.Instant
import java.util.Objects

class SparkIngestionProperties(
    var flushImmediately: Boolean = false,
    var dropByTags: util.List[String] = null,
    var ingestByTags: util.List[String] = null,
    var additionalTags: util.List[String] = null,
    var ingestIfNotExists: util.List[String] = null,
    var creationTime: Instant = null,
    var csvMapping: String = null,
    var csvMappingNameReference: String = null)
    extends Serializable {
  // C'tor for serialization
  def this() {
    this(false)
  }

  override def toString: String = {
    new ObjectMapper()
      .registerModule(new JavaTimeModule())
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(this)
  }

  // In case of Streaming, these options are not supported. The idea is to validate before sending the request to Kusto
  def validateStreamingProperties(): Unit = {
    if ((this.ingestByTags != null && !this.ingestByTags.isEmpty)
      || (this.dropByTags != null && !this.dropByTags.isEmpty)
      || (this.additionalTags != null && !this.additionalTags.isEmpty)
      || Objects.nonNull(creationTime)) {
      throw new IllegalArgumentException(
        "Ingest by tags / Drop by tags / Additional tags / Creation Time are not supported for streaming ingestion " +
          "through SparkIngestionProperties")
    }

    if (StringUtils.isNotEmpty(this.csvMapping)) {
      throw new IllegalArgumentException(
        "CSVMapping cannot be used with Spark streaming ingestion")
    }
  }

  def toIngestionProperties(database: String, table: String): IngestionProperties = {
    val ingestionProperties = new IngestionProperties(database, table)
    val additionalProperties = new util.HashMap[String, String]()

    if (this.flushImmediately) {
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
      additionalProperties.put("creationTime", this.creationTime.toString)
    }

    if (this.csvMapping != null) {
      additionalProperties.put("ingestionMapping", this.csvMapping)
      additionalProperties.put("ingestionMappingType", IngestionMappingKind.CSV.getKustoValue)
    }

    if (this.csvMappingNameReference != null) {
      ingestionProperties.setIngestionMapping(
        new IngestionMapping(
          this.csvMappingNameReference,
          IngestionMapping.IngestionMappingKind.CSV))
    }

    ingestionProperties.setAdditionalProperties(additionalProperties)
    ingestionProperties
  }
}

object SparkIngestionProperties {
  def cloneIngestionProperties(
      ingestionProperties: IngestionProperties,
      destinationTable: Option[String] = None): IngestionProperties = {
    val cloned = new IngestionProperties(
      ingestionProperties.getDatabaseName,
      if (destinationTable.isDefined) destinationTable.get else ingestionProperties.getTableName)
    cloned.setReportLevel(ingestionProperties.getReportLevel)
    cloned.setReportMethod(ingestionProperties.getReportMethod)
    cloned.setAdditionalTags(ingestionProperties.getAdditionalTags)
    cloned.setDropByTags(ingestionProperties.getDropByTags)
    cloned.setIngestByTags(ingestionProperties.getIngestByTags)
    cloned.setIngestIfNotExists(ingestionProperties.getIngestIfNotExists)
    cloned.setDataFormat(ingestionProperties.getDataFormat)
    cloned.setIngestionMapping(ingestionProperties.getIngestionMapping)
    cloned.setAdditionalProperties(ingestionProperties.getAdditionalProperties)
    cloned.setFlushImmediately(ingestionProperties.getFlushImmediately)
    cloned
  }

  private[kusto] def fromString(json: String): SparkIngestionProperties = {
    new ObjectMapper()
      .registerModule(new JavaTimeModule())
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .readValue(json, classOf[SparkIngestionProperties])
  }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.StringUtils

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind

import java.util
import com.microsoft.azure.kusto.ingest.{IngestionMapping, IngestionProperties}

import java.security.InvalidParameterException
import java.time.Instant

class SparkIngestionProperties(
    var flushImmediately: Boolean = false,
    var dropByTags: Option[util.List[String]] = None,
    var ingestByTags: Option[util.List[String]] = None,
    var additionalTags: Option[util.List[String]] = None,
    var ingestIfNotExists: Option[util.List[String]] = None,
    var creationTime: Option[Instant] = None,
    var csvMapping: Option[String] = None,
    var csvMappingNameReference: Option[String] = None)
    extends Serializable {
  // C'tor for serialization
  def this() = {
    this(false)
  }

  override def toString: String = {
    new ObjectMapper()
      .registerModule(new JavaTimeModule())
      .registerModule(DefaultScalaModule)
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(this)
  }

  // In case of Streaming, these options are not supported. The idea is to validate before sending the request to Kusto
  def validateStreamingProperties(): Unit = {
    if (this.ingestByTags.exists(!_.isEmpty)
      || this.dropByTags.exists(!_.isEmpty)
      || this.additionalTags.exists(!_.isEmpty)
      || this.creationTime.isDefined) {
      throw new InvalidParameterException(
        "Ingest by tags / Drop by tags / Additional tags / Creation Time are not supported for streaming ingestion " +
          "through SparkIngestionProperties")
    }
    if (this.csvMapping.exists(s => !StringUtils.isEmpty(s))) {
      throw new InvalidParameterException(
        "CSVMapping cannot be used with Spark streaming ingestion")
    }
  }

  def toIngestionProperties(database: String, table: String): IngestionProperties = {
    val ingestionProperties = new IngestionProperties(database, table)
    val additionalProperties = new util.HashMap[String, String]()

    if (this.flushImmediately) {
      ingestionProperties.setFlushImmediately(true)
    }

    this.dropByTags.foreach(ingestionProperties.setDropByTags)
    this.ingestByTags.foreach(ingestionProperties.setIngestByTags)
    this.additionalTags.foreach(ingestionProperties.setAdditionalTags)
    this.ingestIfNotExists.foreach(ingestionProperties.setIngestIfNotExists)
    this.creationTime.foreach(ct => additionalProperties.put("creationTime", ct.toString))

    this.csvMapping.foreach { mapping =>
      additionalProperties.put("ingestionMapping", mapping)
      additionalProperties.put("ingestionMappingType", IngestionMappingKind.CSV.getKustoValue)
    }

    this.csvMappingNameReference.foreach { ref =>
      ingestionProperties.setIngestionMapping(
        new IngestionMapping(ref, IngestionMapping.IngestionMappingKind.CSV))
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
      .registerModule(DefaultScalaModule)
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .readValue(json, classOf[SparkIngestionProperties])
  }
}

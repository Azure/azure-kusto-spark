//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark.datasink

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind

import java.util
import com.microsoft.azure.kusto.ingest.{IngestionMapping, IngestionProperties}

import java.time.Instant

class SparkIngestionProperties(
    var flushImmediately: Boolean = false,
    var dropByTags: util.ArrayList[String] = null,
    var ingestByTags: util.ArrayList[String] = null,
    var additionalTags: util.ArrayList[String] = null,
    var ingestIfNotExists: util.List[String] = null,
    var creationTime: Instant = null,
    var csvMapping: String = null,
    var csvMappingNameReference: String = null) {
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

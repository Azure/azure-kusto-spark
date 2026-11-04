// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.{ColumnMapping, IngestionMapping}
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping.IngestionMappingType
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.kusto.spark.datasink.IngestionFormat.IngestionFormat
import com.microsoft.kusto.spark.datasink.{IngestionFormat, SparkIngestionProperties}

import java.time.{OffsetDateTime, ZoneOffset}
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Maps SparkIngestionProperties to ingest-v2 request properties, with the same meaning as
 * `SparkIngestionProperties.toIngestionProperties` on the existing path.
 */
private[v2] object IngestV2RequestProperties {
  private val mapper = new ObjectMapper()
  private val MappingFields = Set("columnName", "columnType", "properties")

  private def nonEmpty(list: java.util.List[String]): Option[java.util.List[String]] =
    Option(list).filter(!_.isEmpty)

  private def nonBlank(value: String): Option[String] = Option(value).filter(_.trim.nonEmpty)

  /** Why these properties cannot be sent through ingest-v2, if they cannot. */
  def unsupportedReason(
      properties: Option[SparkIngestionProperties],
      format: IngestionFormat): Option[String] =
    properties.flatMap { p =>
      val hasInlineMapping = nonBlank(p.csvMapping).isDefined
      val hasMappingReference = nonBlank(p.csvMappingNameReference).isDefined
      if (format == IngestionFormat.Parquet && (hasInlineMapping || hasMappingReference)) {
        Some("a CSV mapping cannot be used with the parquet ingestion format")
      } else if (hasInlineMapping && hasMappingReference) {
        Some("both an inline CSV mapping and a CSV mapping reference are set")
      } else if (hasInlineMapping && parseCsvMapping(p.csvMapping).isEmpty) {
        Some("the inline CSV mapping is not in the form produced by the connector")
      } else {
        None
      }
    }

  /**
   * Converts an inline CSV mapping in the connector's JSON form (an array of {columnName,
   * columnType, properties}, see KustoIngestionUtils.csvMappingToString) to ingest-v2 column
   * mappings. Any other shape returns None, so the write uses the existing path, which sends the
   * JSON unchanged.
   */
  private[v2] def parseCsvMapping(json: String): Option[java.util.List[ColumnMapping]] =
    Try(mapper.readTree(json)).toOption
      .filter(n => n != null && n.isArray && n.size() > 0)
      .flatMap { array =>
        val parsed = array.elements().asScala.toList.map { node =>
          val fieldsOk = node.isObject && node.fieldNames().asScala.forall(MappingFields.contains)
          val name = Option(node.get("columnName")).filter(_.isTextual).map(_.asText())
          val columnType = Option(node.get("columnType")).filter(_.isTextual).map(_.asText())
          val properties = Option(node.get("properties")).filterNot(_.isNull)
          val propertiesOk =
            properties.forall(p => p.isObject && p.fields().asScala.forall(_.getValue.isTextual))
          if (fieldsOk && propertiesOk && name.exists(_.trim.nonEmpty) && columnType.isDefined) {
            val props = new java.util.LinkedHashMap[String, String]()
            properties.foreach(
              _.fields().asScala.foreach(e => props.put(e.getKey, e.getValue.asText())))
            Some(new ColumnMapping(name.get, columnType.get, props))
          } else {
            None
          }
        }
        if (parsed.forall(_.isDefined)) Some(parsed.flatten.asJava) else None
      }

  /**
   * @param forceSkipBatching
   *   skip batching even if the user did not ask for it; the existing path does this for blobs
   *   sealed because they reached the size limit
   * @param enableTracking
   *   ask the service to keep per-blob status; only writes that wait for the result need it,
   *   like the existing path which tracks transactional writes only
   */
  def build(
      properties: Option[SparkIngestionProperties],
      format: IngestionFormat,
      forceSkipBatching: Boolean = false,
      enableTracking: Boolean = true): IngestRequestProperties = {
    unsupportedReason(properties, format).foreach(r => throw new IllegalArgumentException(r))

    val builder = IngestRequestPropertiesBuilder.create().withEnableTracking(enableTracking)
    if (forceSkipBatching || properties.exists(_.flushImmediately)) builder.withSkipBatching(true)
    properties.foreach { p =>
      nonEmpty(p.dropByTags).foreach(builder.withDropByTags)
      nonEmpty(p.ingestByTags).foreach(builder.withIngestByTags)
      nonEmpty(p.additionalTags).foreach(builder.withAdditionalTags)
      nonEmpty(p.ingestIfNotExists).foreach(builder.withIngestIfNotExists)
      Option(p.creationTime).foreach(t =>
        builder.withCreationTime(OffsetDateTime.ofInstant(t, ZoneOffset.UTC)))
      nonBlank(p.csvMappingNameReference).foreach(ref =>
        builder.withIngestionMapping(new IngestionMapping(ref, IngestionMappingType.CSV)))
      nonBlank(p.csvMapping)
        .flatMap(parseCsvMapping)
        .foreach(columns =>
          builder.withIngestionMapping(new IngestionMapping(columns, IngestionMappingType.CSV)))
    }
    builder.build()
  }

}

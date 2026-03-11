// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.sink

import com.microsoft.kusto.spark.datasource.KustoSourceOptions
import com.microsoft.kusto.spark.utils.{
  KustoClientCache,
  KustoQueryUtils,
  KustoDataSourceUtils => KDSU
}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._
import scala.util.Try

class KustoTableProvider extends TableProvider with DataSourceRegister {

  private val className = this.getClass.getSimpleName

  override def shortName(): String = "kustoV2"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    KDSU.logInfo(
      className,
      s"inferSchema called with ${options.size()} options, " +
        s"keys: ${options.keySet().asScala.mkString(", ")}")
    Try(KustoTableProvider.inferSchemaFromOptions(options)).recover { case e: Exception =>
      KDSU.logWarn(className, s"Schema inference failed (KustoTable will retry): ${e.getMessage}")
      new StructType()
    }.get
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    KustoTable(schema, partitioning, properties)
  }

  override def supportsExternalMetadata(): Boolean = true
}

object KustoTableProvider {

  private val className = "KustoTableProvider"

  /**
   * Build a params Map from a [[CaseInsensitiveStringMap]] that is safe for use with
   * [[KDSU.parseSourceParameters]]. Uses the asCaseSensitiveMap (original-case keys) and filters
   * out any null values that may leak through Spark's option conversion pipeline.
   */
  private def safeParams(options: CaseInsensitiveStringMap): Map[String, String] = {
    val jMap = options.asCaseSensitiveMap()
    jMap
      .entrySet()
      .asScala
      .iterator
      .filter(e => Option(e.getValue).isDefined)
      .map(e => e.getKey -> e.getValue)
      .toMap
  }

  /**
   * Infer schema from a [[CaseInsensitiveStringMap]]. Uses the case-insensitive `get()` to read
   * the query option reliably, then builds a safe params map for `parseSourceParameters`.
   */
  def inferSchemaFromOptions(options: CaseInsensitiveStringMap): StructType = {
    // Use case-insensitive get() directly — avoids issues with asCaseSensitiveMap key casing
    val query = Option(options.get(KustoSourceOptions.KUSTO_QUERY)).getOrElse("")
    if (query.isEmpty) {
      KDSU.logInfo(className, "No kustoQuery option found; returning empty schema (write path)")
      new StructType()
    } else {
      KDSU.logInfo(className, s"Inferring schema for query: ${query.take(80)}")
      val normalizedQuery = KustoQueryUtils.normalizeQuery(query)
      val schemaQuery =
        if (KustoQueryUtils.isQuery(query)) KustoQueryUtils.getQuerySchemaQuery(normalizedQuery)
        else ""
      if (schemaQuery.isEmpty) {
        KDSU.logWarn(
          className,
          "Cannot infer schema from a Kusto command; returning empty schema")
        new StructType()
      } else {
        val params = safeParams(options)
        val sourceParams = KDSU.parseSourceParameters(params, allowProxy = true)
        val authentication =
          if (sourceParams.keyVaultAuth.isDefined) {
            KDSU.mergeKeyVaultAndOptionsAuthentication(
              com.microsoft.kusto.spark.utils.KeyVaultUtils
                .getAadAppParametersFromKeyVault(sourceParams.keyVaultAuth.get),
              Some(sourceParams.authenticationParameters))
          } else {
            sourceParams.authenticationParameters
          }
        val client = KustoClientCache.getClient(
          sourceParams.kustoCoordinates.clusterUrl,
          authentication,
          sourceParams.kustoCoordinates.ingestionUrl,
          sourceParams.kustoCoordinates.clusterAlias)
        val result = KDSU
          .getSchema(
            sourceParams.kustoCoordinates.database,
            schemaQuery,
            client,
            Some(sourceParams.clientRequestProperties))
          .sparkSchema
        KDSU.logInfo(
          className,
          s"Inferred schema with ${result.fields.length} fields: " +
            s"${result.fieldNames.mkString(", ")}")
        result
      }
    }
  }
}

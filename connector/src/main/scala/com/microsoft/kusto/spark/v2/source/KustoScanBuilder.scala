// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.source

import com.microsoft.kusto.spark.datasource.{KustoFiltering, KustoSourceOptions}
import com.microsoft.kusto.spark.v2.sink.KustoTableProvider
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.connector.read.{
  Scan,
  ScanBuilder,
  SupportsPushDownFilters,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * V2 ScanBuilder that parses read options and supports column pruning and filter push-down.
 */
final class KustoScanBuilder(fullSchema: StructType, options: CaseInsensitiveStringMap)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters {

  private val className = this.getClass.getSimpleName

  // Fallback: if the schema passed from getTable is empty but we have a query option,
  // infer the schema directly. This handles cases where inferSchema() returned empty.
  private val resolvedSchema: StructType = if (fullSchema.nonEmpty) {
    fullSchema
  } else {
    KDSU.logInfo(
      className,
      "Full schema is empty — attempting fallback schema inference from options")
    KustoTableProvider.inferSchemaFromOptions(options)
  }

  private var readSchema: StructType = resolvedSchema
  private var acceptedFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val pushDown = Option(options.get(KustoSourceOptions.KUSTO_QUERY_FILTER_PUSH_DOWN))
      .forall(_.trim.toBoolean)
    if (pushDown) {
      acceptedFilters = filters
      // Return empty — we accept all filters for push-down to Kusto
      Array.empty
    } else {
      // No push-down; Spark will apply all filters post-scan
      filters
    }
  }

  override def pushedFilters(): Array[Filter] = acceptedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    readSchema = requiredSchema
  }

  override def build(): Scan = {
    KDSU.logInfo(
      className,
      s"Building KustoScan with ${readSchema.fields.length} columns, " +
        s"${acceptedFilters.length} pushed filters")
    new KustoScan(readSchema, options, KustoFiltering(readSchema.fieldNames, acceptedFilters))
  }
}

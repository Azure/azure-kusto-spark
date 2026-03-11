// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.sink

import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.datasource.KustoSourceOptions
import com.microsoft.kusto.spark.v2.source.KustoScanBuilder
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  SupportsWrite,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._
import scala.util.Try

final case class KustoTable(
    tableSchema: StructType,
    tablePartitioning: Array[Transform],
    override val properties: util.Map[String, String])
    extends Table
    with SupportsRead
    with SupportsWrite {

  private val className = "KustoTable"

  // Resolve schema eagerly: use the provided tableSchema if non-empty, otherwise try to
  // infer it from the properties (which contain the user's read options like kustoQuery).
  // This handles cases where inferSchema() in KustoTableProvider returned empty.
  lazy val resolvedSchema: StructType = {
    if (tableSchema.nonEmpty) {
      tableSchema
    } else {
      // Wrap properties in CaseInsensitiveStringMap for reliable case-insensitive access
      val opts = new CaseInsensitiveStringMap(properties)
      val hasQuery = Option(opts.get(KustoSourceOptions.KUSTO_QUERY)).exists(_.nonEmpty)
      if (hasQuery) {
        KDSU.logInfo(
          className,
          "Table created with empty schema but has kustoQuery — inferring schema from properties")
        Try(KustoTableProvider.inferSchemaFromOptions(opts)).getOrElse {
          KDSU.logWarn(className, "Fallback schema inference failed; using empty schema")
          tableSchema
        }
      } else {
        tableSchema
      }
    }
  }

  override def name(): String = {
    val cluster = Option(properties.get(KustoSinkOptions.KUSTO_CLUSTER)).getOrElse("unknown")
    val database = Option(properties.get(KustoSinkOptions.KUSTO_DATABASE)).getOrElse("unknown")
    val table = Option(properties.get(KustoSinkOptions.KUSTO_TABLE))
      .orElse(Option(properties.get(KustoSourceOptions.KUSTO_QUERY))
        .map(q => if (q.length > 40) q.take(40) + "..." else q))
      .getOrElse("unknown")
    s"$cluster.$database.$table"
  }

  override def schema(): StructType = resolvedSchema

  override def partitioning(): Array[Transform] = tablePartitioning

  override def columns(): Array[org.apache.spark.sql.connector.catalog.Column] = {
    resolvedSchema.fields.map(f =>
      org.apache.spark.sql.connector.catalog.Column.create(f.name, f.dataType, f.nullable))
  }

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new KustoScanBuilder(resolvedSchema, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    KustoWriteBuilder(info)
  }
}

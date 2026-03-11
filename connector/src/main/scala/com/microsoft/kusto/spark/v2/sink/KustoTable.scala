// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.sink

import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

import java.util

final case class KustoTable(
    tableSchema: StructType,
    tablePartitioning: Array[Transform],
    override val properties: util.Map[String, String])
    extends Table
    with SupportsWrite {

  override def name(): String = {
    val cluster = properties.getOrDefault(KustoSinkOptions.KUSTO_CLUSTER, "unknown")
    val database = properties.getOrDefault(KustoSinkOptions.KUSTO_DATABASE, "unknown")
    val table = properties.getOrDefault(KustoSinkOptions.KUSTO_TABLE, "unknown")
    s"$cluster.$database.$table"
  }

  override def schema(): StructType = tableSchema

  override def partitioning(): Array[Transform] = tablePartitioning

  override def columns(): Array[org.apache.spark.sql.connector.catalog.Column] = {
    tableSchema.fields.map(f =>
      org.apache.spark.sql.connector.catalog.Column.create(f.name, f.dataType, f.nullable))
  }

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.BATCH_WRITE, TableCapability.ACCEPT_ANY_SCHEMA)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    KustoWriteBuilder(info)
  }
}

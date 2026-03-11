// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.sink

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class KustoTableProvider extends TableProvider with DataSourceRegister {

  override def shortName(): String = "kustoV2"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // Write-only path: schema comes from the DataFrame being written,
    // not from inference. Return empty schema.
    new StructType()
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    KustoTable(schema, partitioning, properties)
  }

  override def supportsExternalMetadata(): Boolean = true
}

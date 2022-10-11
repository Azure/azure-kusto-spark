package com.microsoft.kusto.spark.datasink.parquet

import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

class KustoParquetFormat extends ParquetFileFormat {
  override def shortName(): String = "kustoparquet"

  override def toString: String = "KustoParquet"

}

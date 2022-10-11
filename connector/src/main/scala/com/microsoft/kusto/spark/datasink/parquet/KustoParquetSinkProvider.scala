package com.microsoft.kusto.spark.datasink.parquet

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode

class KustoParquetSinkProvider extends StreamSinkProvider {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    new KustoParquetSink(sqlContext, parameters, partitionColumns, outputMode)
  }

  override def toString: String = {
    this.getClass.getCanonicalName
  }
}

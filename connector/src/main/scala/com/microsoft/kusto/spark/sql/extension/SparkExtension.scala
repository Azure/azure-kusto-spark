package com.microsoft.kusto.spark.sql.extension

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SparkIngestionProperties}
import com.microsoft.kusto.spark.datasource.KustoSourceOptions
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql._

object SparkExtension {
  implicit class DataFrameReaderExtension(df: DataFrameReader) {
    def kusto(kustoCluster: String, database: String, query: String,
              conf: Map[String, String] = Map.empty[String, String],
              maybeCpr: Option[ClientRequestProperties] = None): DataFrame = {
      maybeCpr.foreach(crp=>df.option(KustoSourceOptions.KUSTO_CLIENT_REQUEST_PROPERTIES_JSON, crp.toString))
      df.format("com.microsoft.kusto.spark.datasource")
        .option(KustoSourceOptions.KUSTO_CLUSTER, kustoCluster)
        .option(KustoSourceOptions.KUSTO_DATABASE, database)
        .option(KustoSourceOptions.KUSTO_QUERY, query)
        .options(conf)
        .load()
    }
  }

  implicit class DataFrameWriterExtension(df: DataFrameWriter[Row]) {
    def kusto(kustoCluster: String, database: String, table: String,
              conf: Map[String, String] = Map.empty[String, String],
              mayBeSparkIngestionProperties: Option[SparkIngestionProperties] = None): Unit = {
      mayBeSparkIngestionProperties.foreach(
        sparkIngestionProperties=>df.option(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON, sparkIngestionProperties.toString)
      )
      df.format("com.microsoft.kusto.spark.datasource")
        .option(KustoSinkOptions.KUSTO_CLUSTER, kustoCluster)
        .option(KustoSinkOptions.KUSTO_DATABASE, database)
        .option(KustoSinkOptions.KUSTO_TABLE, table)
        .options(conf)
        .mode(SaveMode.Append)
        .save()
    }
  }

  implicit class DataStreamWriterExtension(df: DataStreamWriter[Row]) {
    def kusto(kustoCluster: String, database: String, table: String,
              conf: Map[String, String] = Map.empty[String, String],
              maybeSparkIngestionProperties: Option[SparkIngestionProperties] = None): Unit = {
      maybeSparkIngestionProperties.foreach(sparkIngestionProperties =>
        df.option(KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON, sparkIngestionProperties.toString))
      df.format("com.microsoft.kusto.spark.datasource")
        .option(KustoSinkOptions.KUSTO_CLUSTER, kustoCluster)
        .option(KustoSinkOptions.KUSTO_DATABASE, database)
        .option(KustoSinkOptions.KUSTO_TABLE, table)
        .options(conf)
    }
  }
}

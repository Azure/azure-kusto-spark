package com.microsoft.kusto.spark.sql.extension

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.datasink.SparkIngestionProperties
import com.microsoft.kusto.spark.datasource.KustoOptions
import org.apache.spark.sql.{DataFrameWriter, _}

object SparkExtension {

  implicit class DataFrameReaderExtension(df: DataFrameReader) {

    def kusto(kustoCluster: String, database: String, query: String, conf: Map[String, String] = Map.empty[String, String], cpr: Option[ClientRequestProperties] = None): DataFrame = {
      (if (cpr.isDefined) {
        df.option(KustoOptions.KUSTO_CLIENT_REQUEST_PROPERTIES_JSON, cpr.get.toString)
      } else {
        df
      }).format("com.microsoft.kusto.spark.datasource")
        .option(KustoOptions.KUSTO_CLUSTER, kustoCluster)
        .option(KustoOptions.KUSTO_DATABASE, database)
        .option(KustoOptions.KUSTO_QUERY, query)
        .options(conf)
        .load()
    }
  }

  implicit class DataFrameWriterExtension(df: DataFrameWriter[Row]) {
    def kusto(kustoCluster: String, database: String, table: String, sparkIngestionProperties: Option[SparkIngestionProperties] = None): Unit = {
      (if (sparkIngestionProperties.isDefined) {
        df.option(KustoOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON, sparkIngestionProperties.get.toString)
      } else {
        df
      }).format("com.microsoft.kusto.spark.datasource")
        .option(KustoOptions.KUSTO_CLUSTER, kustoCluster)
        .option(KustoOptions.KUSTO_DATABASE, database)
        .option(KustoOptions.KUSTO_TABLE, table)
        .save()
    }
  }

}
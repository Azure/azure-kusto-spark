package com.microsoft.kusto.spark.sql.extension

import java.util.Properties

import com.microsoft.kusto.spark.datasource.KustoOptions
import org.apache.spark.sql._

import scala.collection.JavaConverters._

object SparkExtension {
   implicit class DataFrameReaderExtension(dataframeReader: DataFrameReader) {

    def kusto(kustoCluster: String, database: String, query: String, properties: Map[String, String]): DataFrame = {
      dataframeReader.format("com.microsoft.kusto.spark.datasource")
        .option(KustoOptions.KUSTO_CLUSTER, kustoCluster)
        .option(KustoOptions.KUSTO_DATABASE, database)
        .option(KustoOptions.KUSTO_QUERY, query)
        .options(properties)
        .load()
    }
  }
}
//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark.sql.extension

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SparkIngestionProperties}
import com.microsoft.kusto.spark.datasource.KustoSourceOptions
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrameWriter, _}

object SparkExtension {

  implicit class DataFrameReaderExtension(df: DataFrameReader) {

    def kusto(
        kustoCluster: String,
        database: String,
        query: String,
        conf: Map[String, String] = Map.empty[String, String],
        cpr: Option[ClientRequestProperties] = None): DataFrame = {
      if (cpr.isDefined) {
        df.option(KustoSourceOptions.KUSTO_CLIENT_REQUEST_PROPERTIES_JSON, cpr.get.toString)
      }

      df.format("com.microsoft.kusto.spark.datasource")
        .option(KustoSourceOptions.KUSTO_CLUSTER, kustoCluster)
        .option(KustoSourceOptions.KUSTO_DATABASE, database)
        .option(KustoSourceOptions.KUSTO_QUERY, query)
        .options(conf)
        .load()
    }
  }

  implicit class DataFrameWriterExtension(df: DataFrameWriter[Row]) {
    def kusto(
        kustoCluster: String,
        database: String,
        table: String,
        conf: Map[String, String] = Map.empty[String, String],
        sparkIngestionProperties: Option[SparkIngestionProperties] = None): Unit = {
      if (sparkIngestionProperties.isDefined) {
        df.option(
          KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON,
          sparkIngestionProperties.get.toString)
      }

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
    def kusto(
        kustoCluster: String,
        database: String,
        table: String,
        conf: Map[String, String] = Map.empty[String, String],
        sparkIngestionProperties: Option[SparkIngestionProperties] = None) = {
      if (sparkIngestionProperties.isDefined) {
        df.option(
          KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON,
          sparkIngestionProperties.get.toString)
      }

      df.format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
        .option(KustoSinkOptions.KUSTO_CLUSTER, kustoCluster)
        .option(KustoSinkOptions.KUSTO_DATABASE, database)
        .option(KustoSinkOptions.KUSTO_TABLE, table)
        .options(conf)
    }
  }

}

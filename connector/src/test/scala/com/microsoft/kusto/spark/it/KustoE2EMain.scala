// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.it

import com.microsoft.kusto.spark.KustoTestUtils.getSystemTestOptions
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SinkTableCreationMode}
import org.apache.spark.sql.{SaveMode, SparkSession}


object KustoE2EMain {
  private val nofExecutors = 4
  private lazy val kustoTestConnectionOptions = getSystemTestOptions

  def main(args: Array[String]): Unit = {
    val wasbs_path =
      "wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2018/puMonth=11"
    val spark: SparkSession = SparkSession
      .builder()
      .appName("KustoSink")
      .config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      .master(f"local[$nofExecutors]")
      .getOrCreate()
    val nyctlc = spark.read.parquet(wasbs_path)

    nyctlc.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, args(0))
      .option(KustoSinkOptions.KUSTO_DATABASE, args(1))
      .option(KustoSinkOptions.KUSTO_TABLE, "NYCYellowTaxi")
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, args(2))
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        SinkTableCreationMode.CreateIfNotExist.toString)
      .mode(SaveMode.Append)
      .save()
  }
}

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

import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.datasource.KustoSourceOptions
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SimpleKustoDataSink {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "Your HADOOP_HOME")
    val sparkConf = new SparkConf()
      .set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSink")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_AAD_APP_ID -> "Your Client ID",
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> "Your secret",
      KustoSourceOptions.KUSTO_QUERY -> "Your Kusto query")

    // Create a DF - read from a Kusto cluster
    val df = sparkSession.read.kusto(
      "Your Kusto Cluster",
      "Your Kusto Database",
      "Your Kusto Query in KustoOptions.Kusto_Query",
      conf)
    df.show

    // Now write to a Kusto table
    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSinkOptions.KUSTO_CLUSTER, "Your Kusto Cluster")
      .option(KustoSinkOptions.KUSTO_DATABASE, "Your Kusto Database")
      .option(KustoSinkOptions.KUSTO_TABLE, "Your Kusto Destination Table")
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, "Your Client ID")
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, "Your secret")
      .mode(SaveMode.Append)
      .save()

    sparkSession.stop
  }
}

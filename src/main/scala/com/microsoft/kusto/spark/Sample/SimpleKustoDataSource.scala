package com.microsoft.kusto.spark.Sample

import com.microsoft.kusto.spark.datasource.KustoOptions
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SimpleKustoDataSource {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "Your HADOOP_HOME")
    val sparkConf = new SparkConf().set("spark.testing", "true")
      .set("spark.ui.enabled", "false")
      .setAppName("SimpleKustoDataSource")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val conf: Map[String, String] = Map(
      KustoOptions.KUSTO_AAD_CLIENT_ID -> "Your Client ID",
      KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> "Your secret",
      KustoOptions.KUSTO_QUERY -> "Your Kusto query",
      KustoOptions.KUSTO_READ_MODE -> "Your reading mode. When set as 'lean', storage parameters are not required",
      KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME -> "Your blob storage account",
      KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY -> "Your storage account key, Alternatively, SAS key can be used",
      KustoOptions.KUSTO_BLOB_CONTAINER -> "Your blob storage container name"
    )
    val df = sparkSession.read.kusto("Your Kusto Cluster", "Your Kusto Database", "Your Kusto Query in KustoOptions.Kusto_Query", conf)
    df.show
    sparkSession.stop
  }

}

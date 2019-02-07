package com.microsoft.kusto.spark.sql.extension

import java.util.Properties

import com.microsoft.kusto.spark.datasource.KustoOptions
import org.apache.spark.sql._

import scala.collection.JavaConverters._

object SparkExtension {
  /*
  implicit class SparkSessionExtension(spark: SparkSession) {
    def kusto(kustoQuery: String): DataFrame = {

    }
  }
  */

  implicit class DataFrameReaderExtension(dataframeReader: DataFrameReader) {

    def kusto(kustoCluster: String, database: String, query: String, properties: Map[String, String]): DataFrame = {
      dataframeReader.format("com.microsoft.kusto.spark.datasource")
        .option(KustoOptions.KUSTO_CLUSTER, kustoCluster)
        .option(KustoOptions.KUSTO_DATABASE, database)
        .option(KustoOptions.KUSTO_QUERY, query)
        .option(KustoOptions.KUSTO_NUM_PARTITIONS, "1")
        .options(properties)
        .load()
    }

    def kusto(kustoCluster: String,
              database: String,
              query: String,
              columnName: String,
              lowerBound: Long,
              upperBound: Long,
              numPartitions: Int,
              connectionProperties: Properties): DataFrame = {
      dataframeReader.option(KustoOptions.KUSTO_PARTITION_COLUMN, columnName)
        .option(KustoOptions.KUSTO_LOWER_BOUND, lowerBound)
        .option(KustoOptions.KUSTO_UPPER_BOUND, upperBound)
        .option(KustoOptions.KUSTO_NUM_PARTITIONS, numPartitions.toString)

      val hashMap = new scala.collection.mutable.HashMap[String, String]
      hashMap ++= connectionProperties.asScala
      kusto(kustoCluster, database, query, hashMap.toMap)
    }
  }
}
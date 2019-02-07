package com.microsoft.kusto.spark.Sample

/***************************************************************************************/
/*   This code is intended for reference only.                                         */
/*   It was tested on an Azure DataBricks cluster using Databricks Runtime Version 5.0 */
/*   (Apache Spark 2.4.0, Scala 2.11)                                                  */
/***************************************************************************************/

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.microsoft.kusto.spark.datasource.KustoOptions
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DataTypes.IntegerType
import org.apache.spark.sql.types.{StringType, StructType}

object KustoConnectorDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "Your HADOOP_HOME")

    // COMMAND ----------
    // Note! This command is not required if you run in a Databricks notebook
    val spark: SparkSession = SparkSession.builder()
      .appName("KustoSink")
      .master(f"local[4]")
      .getOrCreate()

    // COMMAND ----------
    // Set logging level. Logs are available via databricks visualization    
    KDSU.setLoggingLevel("all")

    // COMMAND ----------
    // Get the application Id and Key from the secret store. Scope must be pre-configured in Databricks. For details, refer to https://docs.azuredatabricks.net/user-guide/secrets/index.html#secrets-user-guide
    // The application must be registered in Azure Active Directory (AAD)
    // It must also have proper privileges on the target database and table
    val KustoSparkTestAppId = "Your AAD Application Id" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "KustoSparkTestAppId")
    val KustoSparkTestAppKey = "Your AAD Application Key" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "KustoSparkTestAppKey")

    // COMMAND ----------
    // Set up sink connection parameters
    val appId = s"$KustoSparkTestAppId"
    val appKey = s"$KustoSparkTestAppKey"
    val authorityId = "Your AAD authority id"
    val cluster = "Your Cluster Name"
    val database = "Your Database Name"
    val table = "Your Kusto Table Name"

    // COMMAND ----------
    /** ************************************************/
    /*            BATCH SINK EXAMPLES                 */
    /** ************************************************/
    // generate a Data Frame for batch sink
    val rowId = new AtomicInteger(1)
    def newRow(): String = s"row-${rowId.getAndIncrement()}"

    // Create 8KB rows, each containing a String and an Integer (change this size per intended test scenario)
    val rows = (1 to 8 * 1024).map(v => (newRow(), v))

    import spark.implicits._
    val df = rows.toDF("name", "value")

    // COMMAND ----------
    // BATCH SINK EXAMPLES
    // Write the data to a Kusto cluster, synchronously
    // To see how the number of artitions effect the command performance, use 'repartition': e.g. 'df.repartition(16).write...'
    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoOptions.KUSTO_CLUSTER, cluster)
      .option(KustoOptions.KUSTO_DATABASE, database)
      .option(KustoOptions.KUSTO_TABLE, table)
      .option(KustoOptions.KUSTO_AAD_CLIENT_ID, appId)
      .option(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, appKey)
      .option(KustoOptions.KUSTO_AAD_AUTHORITY_ID, authorityId)
      .save()

    // COMMAND ----------
    // BATCH SINK EXAMPLES
    // Write the data to a Kusto cluster, ASYNChronously
    // The driver will return quickly, and will complete the operation asynchronously once the workers end ingestion to Kusto.
    // However exceptions are not captured in the driver, and tracking command success/failure status is not straightforward as in synchronous mode
    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoOptions.KUSTO_CLUSTER, cluster)
      .option(KustoOptions.KUSTO_DATABASE, database)
      .option(KustoOptions.KUSTO_TABLE, table)
      .option(KustoOptions.KUSTO_AAD_CLIENT_ID, appId)
      .option(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, appKey)
      .option(KustoOptions.KUSTO_AAD_AUTHORITY_ID, authorityId)
      .option(KustoOptions.KUSTO_WRITE_ENABLE_ASYNC, value = true)
      .save()

    // COMMAND ----------
    /** ************************************************/
    /*          STREAMING SINK EXAMPLE                */
    /** ************************************************/
    var customSchema = new StructType().add("colA", StringType, nullable = true).add("colB", IntegerType, nullable = true)

    // Read data from a file to a stream
    val csvDf = spark
      .readStream
      .schema(customSchema)
      .csv("/FileStore/tables")

    // COMMAND ----------
    // STREAMING SINK EXAMPLE
    // Set up a checkpoint and disable codeGen
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/FileStore/temp/checkpoint")
    spark.conf.set("spark.sql.codegen.wholeStage", "false")

    // Write to a Kusto table from a streaming source
    val kustoQ = csvDf
      .writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .options(Map(
        KustoOptions.KUSTO_CLUSTER -> cluster,
        KustoOptions.KUSTO_TABLE -> table,
        KustoOptions.KUSTO_DATABASE -> database,
        KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
        KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
        KustoOptions.KUSTO_AAD_AUTHORITY_ID -> authorityId))
      .trigger(Trigger.Once)

    kustoQ.start().awaitTermination(TimeUnit.MINUTES.toMillis(8))

    // COMMAND ----------
    /** ************************************************/
    /*               SOURCE EXAMPLES                  */
    /** ************************************************/
    /* USING KUSTO QUERY */
    /** *******************/
    val conf: Map[String, String] = Map(
      KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
      KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
      KustoOptions.KUSTO_QUERY -> s"$table | where (ColB % 1000 == 0) | distinct ColA "
    )

    // SOURCE EXAMPLES: query, simplified syntax flavor
    val df2 = spark.read.kusto(cluster, database, "", conf)
    // Databricks: display(df2)

    // COMMAND ----------
    // SOURCE EXAMPLES: query, elaborate (without extension) format.
    // In this case we run a query. Table parameter is empty.
    val conf2: Map[String, String] = Map(
      KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
      KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
      KustoOptions.KUSTO_QUERY -> s"$table | where (ColB % 1000 == 0) | distinct ColA "
    )

    val df3 = spark.sqlContext.read
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoOptions.KUSTO_CLUSTER, cluster)
      .option(KustoOptions.KUSTO_DATABASE, database)
      .options(conf2)
      .load()
    // Databricks: display(df3)
  }
}
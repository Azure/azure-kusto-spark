// NOTE: This sample requires the azure-eventhubs-spark connector which is currently
// not available for Scala 2.13 / Spark 4.0. To use this sample, you would need to
// either use Spark 3.x with Scala 2.12, or wait for the EventHubs connector to be
// updated for Spark 4.0, or use an alternative EventHubs client library.
//
// See: https://github.com/Azure/azure-event-hubs-spark

import java.util.concurrent.TimeUnit

import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import org.apache.spark.sql._
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

// COMMAND ----------
/**
 * ***********************************************
 */
/*          STREAMING SINK EXAMPLE                */
/**
 * ***********************************************
 */

// To enable faster ingestion into kusto, set a minimal value for the batching ingestion policy:
// .alter table <table name> policy ingestionbatching @'{"MaximumBatchingTimeSpan": "00:00:10",}'

object SparkStreamingKustoSink {
  def main(args: Array[String]): Unit = {
    // COMMAND ----------
    // Note! This command is not required if you run in a Databricks notebook
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SparkStreamingKustoSink")
      .master(f"local[4]")
      .getOrCreate()

    // read messages from Azure Event Hub
    val connectionString = ConnectionStringBuilder("Event Hub Connection String")
      .setEventHubName("Event Hub Name")
      .build

    val eventHubsConf = EventHubsConf(connectionString)
      .setStartingPosition(EventPosition.fromEndOfStream)

    val eventhubs = spark.readStream
      .format("eventhubs")
      .options(eventHubsConf.toMap)
      .option("checkpointLocation", "/checkpoint")
      .load()

    val toString = udf((payload: Array[Byte]) => new String(payload))
    val df = eventhubs.withColumn("body", toString(eventhubs("body")))

    spark.conf.set("spark.sql.streaming.checkpointLocation", "target/temp/checkpoint/")

    // Write to a Kusto table from a streaming source
    val df1 = df.writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .option(KustoSinkOptions.KUSTO_CLUSTER, "Your Kusto Cluster")
      .option(KustoSinkOptions.KUSTO_DATABASE, "Your Kusto Database")
      .option(KustoSinkOptions.KUSTO_TABLE, "Your Kusto Destination Table")
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, "Your Client ID")
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, "Your secret")
      .trigger(Trigger.ProcessingTime(10000))
      .start()

    df1.awaitTermination(TimeUnit.MINUTES.toMillis(8))
  }
}

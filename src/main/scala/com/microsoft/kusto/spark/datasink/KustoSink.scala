package com.microsoft.kusto.spark.datasink

import java.io._

import com.microsoft.kusto.spark.datasource.KustoOptions.SinkTableCreationMode
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class KustoSink(
   sqlContext: SQLContext,
   cluster: String,
   database: String,
   table: String,
   appId: String,
   appKey: String,
   authorityId: String,
   enableAsync: Boolean = false,
   tableCreation: SinkTableCreationMode.Value = SinkTableCreationMode.FailIfNotExist,
   timeZone: String = "UTC",
   saveMode: SaveMode = SaveMode.Append) extends Sink with Serializable {

  private val myName = this.getClass.getSimpleName
  val MessageSource = "KustoSink"
  @volatile private var latestBatchId = -1L

  override def toString = "KustoSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      KDSU.logInfo(myName, s"Skipping already committed batch $batchId")
    } else {
      KustoWriter.write(Option(batchId), data, cluster, database, table, appId, appKey, authorityId, enableAsync, tableCreation, saveMode, timeZone)
      latestBatchId = batchId
    }
  }
}
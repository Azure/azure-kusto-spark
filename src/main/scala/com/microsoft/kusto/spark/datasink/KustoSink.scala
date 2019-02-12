package com.microsoft.kusto.spark.datasink

import java.io._

import com.microsoft.kusto.spark.datasource.{AadApplicationAuthentication, KustoSparkWriteOptions, KustoTableCoordinates}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}

class KustoSink(
                 sqlContext: SQLContext,
                 tableCoordinates: KustoTableCoordinates,
                 appAuthentication: AadApplicationAuthentication,
                 kustoSparkWriteOptions:KustoSparkWriteOptions) extends Sink with Serializable {

  private val myName = this.getClass.getSimpleName
  val MessageSource = "KustoSink"
  @volatile private var latestBatchId = -1L

  override def toString = "KustoSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      KDSU.logInfo(myName, s"Skipping already committed batch $batchId")
    } else {
      KustoWriter.write(Option(batchId), data, tableCoordinates, appAuthentication, kustoSparkWriteOptions)
      latestBatchId = batchId
    }
  }
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import java.io._

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import com.microsoft.kusto.spark.common.KustoCoordinates
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.DataFrame

class KustoSink(
    tableCoordinates: KustoCoordinates,
    authentication: KustoAuthentication,
    writeOptions: WriteOptions,
    clientRequestProperties: ClientRequestProperties)
    extends Sink
    with Serializable {

  private val myName = this.getClass.getSimpleName
  val MessageSource = "KustoSink"
  @volatile private var latestBatchId = -1L

  override def toString = "KustoSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      KDSU.logInfo(myName, s"Skipping already committed batch $batchId")
    } else {
      KustoWriter.write(
        Option(batchId),
        data,
        tableCoordinates,
        authentication,
        writeOptions,
        clientRequestProperties)
      latestBatchId = batchId
    }
  }
}

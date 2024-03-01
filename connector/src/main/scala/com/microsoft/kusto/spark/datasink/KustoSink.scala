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

package com.microsoft.kusto.spark.datasink

import java.io._

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import com.microsoft.kusto.spark.common.KustoCoordinates
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}

class KustoSink(
    sqlContext: SQLContext,
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

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.v2.{IngestV2WriterOrchestrator, IngestionConfig}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.DataFrame

/**
 * Wrapper for kusto-ingest-v2 SDK that implements the unified IngestClient interface.
 *
 * Delegates write operations to IngestV2WriterOrchestrator with config-driven behavior.
 */
class IngestV2ClientWrapper(
    tableCoordinates: KustoCoordinates,
    authentication: KustoAuthentication,
    writeOptions: WriteOptions,
    configOpt: Option[IngestionConfig])
    extends IngestClient {

  private val myName = this.getClass.getSimpleName

  override def write(
      batchId: Option[Long],
      data: DataFrame,
      tableCoordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions,
      crp: ClientRequestProperties): Unit = {

    val table = tableCoordinates.table.get
    val batchIdIfExists = batchId.map(b => s"${b.toString}").getOrElse("")

    // Generate temp table name (same logic as V1)
    val tmpTableName: String = KDSU.generateTempTableName(
      data.sparkSession.sparkContext.appName,
      table,
      writeOptions.requestId,
      batchIdIfExists,
      writeOptions.userTempTableName)

    KDSU.logInfo(myName, s"Delegating write to IngestV2WriterOrchestrator")

    // Delegate to V2 orchestrator
    IngestV2WriterOrchestrator.write(
      data,
      tableCoordinates,
      authentication,
      writeOptions,
      tmpTableName,
      crp)
  }

  override def close(): Unit = {
    // V2 clients are managed by IngestV2ClientCache
    // No explicit cleanup needed here
    KDSU.logDebug(myName, "IngestV2ClientWrapper closed (clients cached)")
  }
}

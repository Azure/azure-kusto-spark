// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.DataFrame

/**
 * Wrapper for kusto-ingest (v1) SDK that implements the unified IngestClient interface.
 *
 * Delegates write operations to KustoWriter's V1 logic (queue-based ingestion).
 */
class IngestV1ClientWrapper(
    tableCoordinates: KustoCoordinates,
    authentication: KustoAuthentication,
    writeOptions: WriteOptions)
    extends IngestClient {

  private val myName = this.getClass.getSimpleName

  override def write(
      batchId: Option[Long],
      data: DataFrame,
      tableCoordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions,
      crp: ClientRequestProperties): Unit = {

    KDSU.logInfo(myName, s"Delegating write to V1 ingestion path (queue-based)")

    // Delegate to V1 logic in KustoWriter
    KustoWriter.writeV1(
      batchId,
      data,
      tableCoordinates,
      authentication,
      writeOptions,
      crp)
  }

  override def close(): Unit = {
    // V1 clients are managed by KustoClientCache
    // No explicit cleanup needed here
    KDSU.logDebug(myName, "IngestV1ClientWrapper closed (clients cached)")
  }
}

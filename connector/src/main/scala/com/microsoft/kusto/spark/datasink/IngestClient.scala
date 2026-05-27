// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import org.apache.spark.sql.DataFrame

/**
 * Unified interface for both V1 and V2 ingestion clients.
 *
 * This abstraction allows the connector to auto-detect and route to the appropriate SDK
 * based on config API response, without manual useIngestV2 flag.
 */
trait IngestClient {

  /**
   * Write a DataFrame to Kusto.
   *
   * This method handles the full write operation including:
   * - Schema validation and table creation (if needed)
   * - Data serialization and upload
   * - Ingestion triggering (queue-based or REST-based)
   * - Transactional finalization (if enabled)
   *
   * @param batchId
   *   Optional batch ID for structured streaming
   * @param data
   *   DataFrame to write
   * @param tableCoordinates
   *   Kusto cluster, database, and table coordinates
   * @param authentication
   *   Authentication configuration
   * @param writeOptions
   *   Write configuration (mode, format, options)
   * @param crp
   *   Client request properties (timeout, tags, etc.)
   */
  def write(
      batchId: Option[Long],
      data: DataFrame,
      tableCoordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions,
      crp: ClientRequestProperties): Unit

  /**
   * Close any resources held by this client.
   */
  def close(): Unit
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.ingest.v2.client.{IngestionOperation, QueuedIngestClient}
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.WriteOptions
import com.microsoft.kusto.spark.utils.{ExtendedKustoClient, KustoDataSourceUtils => KDSU}

import java.time.{Duration, Instant}

/**
 * Self-contained finalization helper for the kusto-ingest-v2 SDK transactional write path.
 * Handles:
 *   - Polling for ingestion completion via kusto-ingest-v2 SDK REST status
 *   - Moving extents from temp table to destination table
 *   - Cleaning up temp table on success
 *
 * This duplicates the transactional finalization logic from v1's FinalizeHelper but uses
 * kusto-ingest-v2 SDK's REST-based status tracking instead of queue polling.
 *
 * Note: We still use ExtendedKustoClient for management commands (move extents, drop table) since
 * those are engine operations not related to ingestion.
 */
object IngestV2FinalizeHelper {
  private val myName = this.getClass.getSimpleName

  /**
   * Finalizes transactional ingestion: waits for all operations to complete, then moves extents
   * from temp table to destination table.
   *
   * @param operations
   *   All ingestion operations from all partitions
   * @param queuedClient
   *   kusto-ingest-v2 SDK client for status tracking
   * @param kustoClient
   *   Engine client for management commands (move extents)
   * @param coordinates
   *   Destination table coordinates
   * @param tmpTableName
   *   Temporary staging table name
   * @param writeOptions
   *   Write configuration
   * @param crp
   *   Client request properties
   * @param sinkStartTime
   *   Time when the sink started
   */
  def finalizeTransactionalIngestion(
      operations: List[IngestionOperation],
      queuedClient: QueuedIngestClient,
      kustoClient: ExtendedKustoClient,
      coordinates: KustoCoordinates,
      tmpTableName: String,
      writeOptions: WriteOptions,
      crp: ClientRequestProperties,
      sinkStartTime: Instant): Unit = {

    val timeout = Duration.ofMillis(writeOptions.timeout.toMillis)
    val database = coordinates.database
    val destinationTable = coordinates.table.get

    KDSU.logInfo(
      myName,
      s"Finalizing transactional ingestion: polling ${operations.size} operations (timeout: $timeout)")

    // Step 1: Wait for all ingestion operations to complete
    val allSucceeded = IngestV2StatusTracker.waitForCompletion(operations, queuedClient, timeout)

    if (!allSucceeded) {
      // Cleanup temp table and throw
      try {
        kustoClient.cleanupIngestionByProducts(database, tmpTableName, crp)
      } catch {
        case e: Exception =>
          KDSU.logWarn(myName, s"Failed to cleanup temp table after failure: ${e.getMessage}")
      }
      throw new RuntimeException(
        s"Transactional ingestion failed: some operations did not complete successfully. " +
          s"Temp table '$tmpTableName' has been cleaned up.")
    }

    // Step 2: Move extents from temp table to destination table
    KDSU.logInfo(myName, s"Moving extents from '$tmpTableName' to '$destinationTable'")

    kustoClient.moveExtents(
      database,
      tmpTableName,
      destinationTable,
      crp,
      writeOptions,
      sinkStartTime)

    // Step 3: Cleanup temp table
    kustoClient.cleanupIngestionByProducts(database, tmpTableName, crp)

    KDSU.logInfo(
      myName,
      s"Transactional ingestion finalized successfully for $database.$destinationTable")
  }
}

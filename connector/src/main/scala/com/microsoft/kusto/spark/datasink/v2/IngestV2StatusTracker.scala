// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.client.{IngestionOperation, QueuedIngestClient}
import com.microsoft.azure.kusto.ingest.v2.models.{Status, StatusResponse}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

import java.time.Duration

/**
 * Self-contained status tracker for kusto-ingest-v2 SDK ingestion operations. Uses the SDK's
 * built-in REST-based status tracking (no Azure Storage Queue polling).
 *
 * This replaces v1's queue-based polling approach.
 */
object IngestV2StatusTracker {
  private val myName = this.getClass.getSimpleName
  private val DefaultPollingInterval: Duration = Duration.ofSeconds(10)

  /**
   * Polls all operations until completion or timeout. Uses the kusto-ingest-v2 SDK's built-in
   * `pollForCompletion` method which uses REST API.
   *
   * @return
   *   true if all operations completed successfully, false if any failed
   */
  def waitForCompletion(
      operations: List[IngestionOperation],
      queuedClient: QueuedIngestClient,
      timeout: Duration): Boolean = {

    if (operations.isEmpty) return true

    KDSU.logInfo(
      myName,
      s"Waiting for ${operations.size} ingestion operations to complete (timeout: $timeout)")

    val results = operations.map { op =>
      try {
        val statusResponse: StatusResponse = queuedClient
          .pollForCompletion(op, DefaultPollingInterval, timeout)
          .join()

        val status = statusResponse.getStatus
        val succeeded = Option(status.getSucceeded).map(_.longValue()).getOrElse(0L)
        val failed = Option(status.getFailed).map(_.longValue()).getOrElse(0L)
        val inProgress = Option(status.getInProgress).map(_.longValue()).getOrElse(0L)

        KDSU.logDebug(
          myName,
          s"Operation ${op.getOperationId}: succeeded=$succeeded, failed=$failed, inProgress=$inProgress")

        failed == 0 && inProgress == 0
      } catch {
        case e: Exception =>
          KDSU.logError(myName, s"Failed to poll operation ${op.getOperationId}: ${e.getMessage}")
          false
      }
    }

    val allSucceeded = results.forall(identity)
    if (allSucceeded) {
      KDSU.logInfo(myName, s"All ${operations.size} operations completed successfully")
    } else {
      KDSU.logError(
        myName,
        s"Some operations failed: ${results.count(identity)}/${results.size} succeeded")
    }
    allSucceeded
  }

  /**
   * Gets the current status summary for an operation without blocking.
   */
  def getOperationStatus(
      operation: IngestionOperation,
      queuedClient: QueuedIngestClient): Status = {
    queuedClient.getOperationSummaryAsyncJava(operation).join()
  }
}

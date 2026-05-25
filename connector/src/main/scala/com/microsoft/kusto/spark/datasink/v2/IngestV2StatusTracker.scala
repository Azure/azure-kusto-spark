// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.client.{IngestionOperation, QueuedIngestClient}
import com.microsoft.azure.kusto.ingest.v2.models.{Status, StatusResponse}
import org.slf4j.LoggerFactory

import java.time.Duration

/**
 * Self-contained status tracker for kusto-ingest-v2 SDK ingestion operations. Uses the SDK's
 * built-in REST-based status tracking (no Azure Storage Queue polling).
 *
 * This replaces v1's queue-based polling approach.
 */
object IngestV2StatusTracker {
  private val logger = LoggerFactory.getLogger(getClass)
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

    logger.info(
      "Waiting for {} ingestion operations to complete (timeout: {})",
      operations.size.toString,
      timeout.toString)

    val results = operations.map { op =>
      try {
        val statusResponse: StatusResponse = queuedClient
          .pollForCompletion(op, DefaultPollingInterval, timeout)
          .join()

        val status = statusResponse.getStatus
        val succeeded = Option(status.getSucceeded).map(_.longValue()).getOrElse(0L)
        val failed = Option(status.getFailed).map(_.longValue()).getOrElse(0L)
        val inProgress = Option(status.getInProgress).map(_.longValue()).getOrElse(0L)

        logger.info(
          "Operation {} completed: succeeded={}, failed={}, inProgress={}",
          op.getOperationId,
          succeeded.toString,
          failed.toString,
          inProgress.toString)

        failed == 0 && inProgress == 0
      } catch {
        case e: Exception =>
          logger.error("Failed to poll operation {}: {}", op.getOperationId, e.getMessage)
          false
      }
    }

    val allSucceeded = results.forall(identity)
    if (allSucceeded) {
      logger.info("All {} operations completed successfully", operations.size.toString)
    } else {
      logger.error(
        "Some operations failed: {}/{} succeeded",
        results.count(identity).toString,
        results.size.toString)
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

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.client.{IngestionOperation, QueuedIngestClient}
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind
import com.microsoft.azure.kusto.ingest.v2.models.Status
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

import java.time.Duration
import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}
import scala.util.control.NonFatal

/** Waits for ingest-v2 operations through the service's REST status API. */
object IngestV2StatusTracker {
  private val myName = this.getClass.getSimpleName
  private[v2] val PollingInterval: Duration = Duration.ofSeconds(10)
  private val MaxStatusCallDuration: Duration = Duration.ofMinutes(2)

  /** Counts reported for one operation. */
  private[v2] final case class Counts(
      succeeded: Long,
      failed: Long,
      canceled: Long,
      inProgress: Long)

  private[v2] object Counts {
    def of(status: Status): Counts = {
      def value(v: java.lang.Long): Long = Option(v).map(_.longValue()).getOrElse(0L)
      if (status == null) Counts(0, 0, 0, 0)
      else
        Counts(
          value(status.getSucceeded),
          value(status.getFailed),
          value(status.getCanceled),
          value(status.getInProgress))
    }
  }

  /** Done when nothing is in progress and every submitted blob has a final state. */
  private[v2] def isDone(counts: Counts, blobCount: Int): Boolean =
    counts.inProgress == 0 && counts.succeeded + counts.failed + counts.canceled >= blobCount

  /** Successful only when every submitted blob succeeded. */
  private[v2] def isSuccess(counts: Counts, blobCount: Int): Boolean =
    counts.failed == 0 && counts.canceled == 0 && counts.inProgress == 0 &&
      counts.succeeded >= blobCount

  /**
   * Waits until every operation finished, sharing one deadline. Throws if any blob failed or was
   * canceled, or if the deadline passed first.
   */
  def waitForCompletion(
      operations: Seq[IngestV2SubmittedOperation],
      queuedClient: QueuedIngestClient,
      timeout: Duration,
      requestId: String): Unit = {
    if (operations.isEmpty) return

    val deadline = System.nanoTime() + timeout.toNanos
    KDSU.logInfo(
      myName,
      s"Waiting for ${operations.size} ingestion operation(s) (timeout: $timeout), requestId: $requestId")

    operations.foreach { op =>
      val operation =
        new IngestionOperation(
          op.operationId,
          op.database,
          op.table,
          IngestKind.valueOf(op.ingestKind))
      var counts = Counts(0, 0, 0, 0)
      var lastError: Option[Throwable] = None
      var done = false
      while (!done) {
        try {
          counts = Counts.of(getSummary(queuedClient, operation, remaining(deadline)))
          lastError = None
          done = isDone(counts, op.blobCount)
        } catch {
          case NonFatal(e) =>
            // Status reads are retried until the deadline; the operation itself is unaffected.
            KDSU.logWarn(
              myName,
              s"Reading the status of ingestion operation ${op.operationId} failed, will retry: $e")
            lastError = Some(e)
        }
        if (!done) {
          val left = remaining(deadline)
          if (left.isZero) {
            lastError.foreach(e => throw e)
            throw new TimeoutException(
              s"Ingestion operation ${op.operationId} did not finish within $timeout " +
                s"(succeeded=${counts.succeeded}, failed=${counts.failed}, " +
                s"canceled=${counts.canceled}, inProgress=${counts.inProgress}), requestId: $requestId")
          }
          Thread.sleep(math.min(PollingInterval.toMillis, left.toMillis))
        }
      }
      if (!isSuccess(counts, op.blobCount)) {
        throw new IllegalStateException(
          s"Ingestion operation ${op.operationId} into '${op.table}' did not fully succeed " +
            s"(blobs=${op.blobCount}, succeeded=${counts.succeeded}, failed=${counts.failed}, " +
            s"canceled=${counts.canceled}), requestId: $requestId. Run '.show ingestion failures' " +
            "on the database for details.")
      }
      KDSU.logDebug(myName, s"Ingestion operation ${op.operationId} succeeded: $counts")
    }
    KDSU.logInfo(
      myName,
      s"All ${operations.size} ingestion operation(s) succeeded, requestId: $requestId")
  }

  private def remaining(deadline: Long): Duration =
    Duration.ofNanos(math.max(0L, deadline - System.nanoTime()))

  private def getSummary(
      queuedClient: QueuedIngestClient,
      operation: IngestionOperation,
      left: Duration): Status = {
    val bound = math.max(1000L, math.min(left.toMillis, MaxStatusCallDuration.toMillis))
    val future = queuedClient.getOperationSummaryAsyncJava(operation)
    try future.get(bound, TimeUnit.MILLISECONDS)
    catch {
      case _: TimeoutException =>
        future.cancel(true)
        throw new TimeoutException(
          s"Reading the status of ingestion operation ${operation.getOperationId} timed out")
      case e: ExecutionException if e.getCause != null => throw e.getCause
    }
  }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.client.{IngestionOperation, QueuedIngestClient}
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind
import com.microsoft.azure.kusto.ingest.v2.models.Status
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

import java.time.Duration
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit, TimeoutException}
import scala.util.{Failure, Success, Try}

/** Waits for ingest-v2 operations through the service's REST status API. */
object IngestV2StatusTracker {
  private val myName = this.getClass.getSimpleName
  private[v2] val PollingInterval: Duration = Duration.ofSeconds(10)
  private val MaxStatusCallDuration: Duration = Duration.ofMinutes(2)
  private[v2] val MaxParallelStatusCalls = 32
  private[v2] val MaxUnknownDuration: Duration = Duration.ofMinutes(15)

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
   * Counts that can never turn into a full success: a blob failed or was canceled, or nothing is
   * in progress any more while some blobs have no state (the service writes an "in progress" row
   * for every blob before it answers the submission).
   */
  private[v2] def hasFailed(counts: Counts, blobCount: Int): Boolean = {
    val known = counts.succeeded + counts.failed + counts.canceled
    counts.failed > 0 || counts.canceled > 0 ||
      (counts.inProgress == 0 && known > 0 && known < blobCount)
  }

  /** The service reports nothing about the operation (all counts zero, or not found). */
  private[v2] def isUnknown(counts: Counts): Boolean =
    counts.succeeded + counts.failed + counts.canceled + counts.inProgress == 0

  /** Status errors that will not go away by asking again. */
  private[v2] def isPermanentStatusError(e: Throwable): Boolean = e match {
    case ie: IngestException =>
      Option(ie.getFailureCode).map(_.intValue()).exists(c => c == 400 || c == 401 || c == 403)
    case _ => false
  }

  private[v2] def isNotFound(e: Throwable): Boolean = e match {
    case ie: IngestException => Option(ie.getFailureCode).exists(_.intValue() == 404)
    case _ => false
  }

  private final class Tracked(val op: IngestV2SubmittedOperation) {
    val operation =
      new IngestionOperation(op.operationId, op.database, op.table, IngestKind.valueOf(op.ingestKind))
    var counts: Counts = Counts(0, 0, 0, 0)
    var lastError: Option[Throwable] = None
    var unknownSinceNanos: Option[Long] = None
  }

  /**
   * Waits until every operation finished, sharing one deadline. Up to [[MaxParallelStatusCalls]]
   * operations are read at the same time. Throws as soon as any blob failed, when an operation
   * stays unknown to the service for [[MaxUnknownDuration]], or when the deadline passes.
   */
  def waitForCompletion(
      operations: Seq[IngestV2SubmittedOperation],
      queuedClient: QueuedIngestClient,
      timeout: Duration,
      requestId: String): Unit =
    waitForCompletion(
      operations,
      timeout,
      requestId,
      (operation: IngestionOperation) => queuedClient.getOperationSummaryAsyncJava(operation),
      (millis: Long) => Thread.sleep(millis))

  private[v2] def waitForCompletion(
      operations: Seq[IngestV2SubmittedOperation],
      timeout: Duration,
      requestId: String,
      readStatus: IngestionOperation => CompletableFuture[Status],
      sleep: Long => Unit): Unit = {
    if (operations.isEmpty) return

    val deadline = System.nanoTime() + timeout.toNanos
    KDSU.logInfo(
      myName,
      s"Waiting for ${operations.size} ingestion operation(s) (timeout: $timeout), requestId: $requestId")

    var pending = operations.map(new Tracked(_)).toList
    while (pending.nonEmpty) {
      // Each group is checked right away so a failure is reported without waiting for the rest;
      // once the deadline has passed, no further group is read.
      pending.grouped(MaxParallelStatusCalls).zipWithIndex.foreach { case (group, i) =>
        if (i == 0 || !remaining(deadline).isZero) {
          poll(group, readStatus, deadline)
          group.foreach(t => check(t, requestId))
        }
      }
      pending = pending.filterNot(t => t.lastError.isEmpty && isDone(t.counts, t.op.blobCount))
      if (pending.nonEmpty) {
        val left = remaining(deadline)
        if (left.isZero) {
          val t = pending.head
          t.lastError.foreach(e => throw e)
          throw new TimeoutException(
            s"${pending.size} ingestion operation(s) did not finish within $timeout, e.g. " +
              s"${t.op.operationId} (succeeded=${t.counts.succeeded}, failed=${t.counts.failed}, " +
              s"canceled=${t.counts.canceled}, inProgress=${t.counts.inProgress}), requestId: $requestId")
        }
        sleep(math.min(PollingInterval.toMillis, left.toMillis))
      }
    }
    KDSU.logInfo(
      myName,
      s"All ${operations.size} ingestion operation(s) succeeded, requestId: $requestId")
  }

  /** Reads the status of a group of operations at the same time. */
  private def poll(
      group: Seq[Tracked],
      readStatus: IngestionOperation => CompletableFuture[Status],
      deadline: Long): Unit = {
    val bound = math.max(1000L, math.min(remaining(deadline).toMillis, MaxStatusCallDuration.toMillis))
    val callDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(bound)
    val futures = group.map(t => t -> Try(readStatus(t.operation)))
    try {
      futures.foreach { case (t, started) =>
        val result = started.flatMap { future =>
          Try(future.get(math.max(1L, TimeUnit.NANOSECONDS.toMillis(callDeadline - System.nanoTime())), TimeUnit.MILLISECONDS))
            .recoverWith {
              case _: TimeoutException =>
                future.cancel(true)
                Failure(new TimeoutException(
                  s"Reading the status of ingestion operation ${t.op.operationId} timed out"))
              case e: ExecutionException if e.getCause != null => Failure(e.getCause)
            }
        }
        result match {
          case Success(status) =>
            t.counts = Counts.of(status)
            t.lastError = None
          case Failure(e: InterruptedException) => throw e
          case Failure(e) => t.lastError = Some(e)
        }
      }
    } finally {
      futures.foreach { case (_, f) => f.foreach(x => if (!x.isDone) x.cancel(true)) }
    }
  }

  /** Throws when the operation can no longer succeed; otherwise records how long it is unknown. */
  private def check(t: Tracked, requestId: String): Unit = {
    val op = t.op
    t.lastError match {
      case Some(e) if isPermanentStatusError(e) => throw e
      case Some(e) =>
        KDSU.logWarn(
          myName,
          s"Reading the status of ingestion operation ${op.operationId} failed, will retry: $e")
      case None if hasFailed(t.counts, op.blobCount) =>
        throw new IllegalStateException(
          s"Ingestion operation ${op.operationId} into '${op.table}' did not fully succeed " +
            s"(blobs=${op.blobCount}, succeeded=${t.counts.succeeded}, failed=${t.counts.failed}, " +
            s"canceled=${t.counts.canceled}, inProgress=${t.counts.inProgress}), requestId: " +
            s"$requestId. Run '.show ingestion failures' on the database for details.")
      case None if isDone(t.counts, op.blobCount) =>
        KDSU.logDebug(myName, s"Ingestion operation ${op.operationId} succeeded: ${t.counts}")
      case None =>
    }
    val unknown = t.lastError.exists(isNotFound) || (t.lastError.isEmpty && isUnknown(t.counts))
    if (!unknown) {
      t.unknownSinceNanos = None
    } else {
      val since = t.unknownSinceNanos.getOrElse(System.nanoTime())
      t.unknownSinceNanos = Some(since)
      if (System.nanoTime() - since > MaxUnknownDuration.toNanos) {
        throw new IllegalStateException(
          s"The ingestion service has reported no status for operation ${op.operationId} into " +
            s"'${op.table}' for $MaxUnknownDuration, requestId: $requestId")
      }
    }
  }

  private def remaining(deadline: Long): Duration =
    Duration.ofNanos(math.max(0L, deadline - System.nanoTime()))
}

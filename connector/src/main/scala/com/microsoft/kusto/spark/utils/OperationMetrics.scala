// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

/**
 * Lightweight operation-level metrics that are emitted as structured log lines. Each metric line
 * uses the format:
 *
 * {{{
 * [KustoConnectorMetrics] operation=<name> durationMs=<ms> [key=value ...]
 * }}}
 *
 * This makes it easy to grep / parse logs and compare durations across different ingestion modes,
 * read paths, and retry attempts.
 */
object OperationMetrics {

  private val className = "OperationMetrics"

  /**
   * Executes `body` while measuring its wall-clock duration. Logs a structured metric line and
   * returns the result of `body`. Extra key-value pairs can be supplied via `attributes` to
   * annotate the metric (e.g. requestId, table name, partition).
   */
  def timed[T](reporter: String, operation: String, attributes: Map[String, String])(
      body: => T): T = {
    val startNanos = System.nanoTime()
    val result = body
    val durationMs = (System.nanoTime() - startNanos) / 1000000L
    logMetric(reporter, operation, durationMs, attributes)
    result
  }

  /**
   * Logs a pre-computed metric (for cases where the caller already tracks start/end times or
   * needs to log after the fact).
   */
  def logMetric(
      reporter: String,
      operation: String,
      durationMs: Long,
      attributes: Map[String, String]): Unit = {
    val attrs =
      if (attributes.nonEmpty)
        attributes.map { case (k, v) => s"$k=$v" }.mkString(" ", " ", "")
      else ""
    KDSU.logInfo(
      reporter,
      s"[KustoConnectorMetrics] operation=$operation durationMs=$durationMs$attrs")
  }
}

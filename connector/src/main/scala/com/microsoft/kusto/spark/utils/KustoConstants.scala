// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

object KustoConstants {
  // Setting high value to have no timeout on Await commands
  val DefaultWaitingIntervalLongRunning: String = (2 days).toSeconds.toString
  val DefaultCleaningInterval: String = (7 days).toSeconds.toString
  val DefaultPeriodicSamplePeriod: FiniteDuration = 1 seconds
  val NoTimeout: String = (-1 seconds).toSeconds.toString
  val ClientName: String = KustoDataSourceUtils.clientName
  val DefaultBufferSize: Int = 16 * 1024
  val StorageExpirySeconds: Int =
    2 * 60 * 60 // 2 hours of delay in seconds. Refactored for seconds
  val SparkSettingsRefreshMinutes: Int = 120
  val OneKiloByte: Int = 1024
  val OneMegaByte: Int = OneKiloByte * OneKiloByte
  val OneGigaByte: Int = OneMegaByte * OneKiloByte
  // The restriction from kusto is 50000 rows but 5000 can still be really big
  val DirectQueryUpperBoundRows = 5000
  val TimeoutForCountCheck: FiniteDuration = 3 seconds
  val IngestByPrefix = "ingest-by:"
  val IngestSkippedTrace =
    s"Ingestion skipped: Provided ingest-by tags are present in the destination table: "
  val MaxSleepOnMoveExtentsMillis: Int = 3 * 60 * 1000
  val DefaultBatchingLimit: Int = 300
  val DefaultExtentsCountForSplitMergePerNode: Int = 400
  val DefaultMaxRetriesOnMoveExtents: Int = 10
  val DefaultExecutionQueueing: Int = TimeUnit.SECONDS.toMillis(60).toInt
  val DefaultTimeoutQueueing: Int = TimeUnit.SECONDS.toMillis(40).toInt
  val MaxIngestRetryAttempts = 2
  val MaxCommandsRetryAttempts = 4
  val MaxStreamingBytes: Int = 4 * OneMegaByte
  val EmptyString = ""
  val DefaultMaximumIngestionTime: FiniteDuration = FiniteDuration.apply(
    MaxIngestRetryAttempts * (DefaultExecutionQueueing + DefaultTimeoutQueueing) + 2000,
    "millis")
  val QueueRetryAttempts = 1

  object Schema {
    val NAME: String = "Name"
    val CSLTYPE: String = "CslType"
    val TYPE: String = "Type"
  }
}

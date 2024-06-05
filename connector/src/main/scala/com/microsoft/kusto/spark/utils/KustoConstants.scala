// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import org.apache.commons.io.FileUtils

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

object KustoConstants {
  // Setting high value to have no timeout on Await commands
  val DefaultWaitingIntervalLongRunning: String =
    Duration.create(2, TimeUnit.DAYS).toSeconds.toString
  val DefaultCleaningInterval: String = Duration.create(7, TimeUnit.DAYS).toSeconds.toString
  val DefaultPeriodicSamplePeriod: FiniteDuration = Duration.create(1, TimeUnit.SECONDS)
  val ClientName: String = KustoDataSourceUtils.clientName
  val DefaultBufferSize: Int = 16 * 1024
  val StorageExpirySeconds: Int =
    2 * 60 * 60 // 2 hours of delay in seconds. Refactored for seconds
  val SparkSettingsRefreshMinutes: Int = 120
  val OneMegaByte: Int = FileUtils.ONE_MB.toInt
  // The restriction from kusto is 50000 rows but 5000 can still be really big
  val DirectQueryUpperBoundRows = 5000
  val TimeoutForCountCheck: FiniteDuration = Duration.create(3, TimeUnit.SECONDS)
  val IngestSkippedTrace =
    s"Ingestion skipped: Provided ingest-by tags are present in the destination table: "
  val MaxSleepOnMoveExtentsMillis: Int = 3 * 60 * 1000
  val DefaultBatchingLimit: Int = 300
  val DefaultExtentsCountForSplitMergePerNode: Int = 400
  val DefaultMaxRetriesOnMoveExtents: Int = 10
  val DefaultTimeoutQueueing: Int = TimeUnit.SECONDS.toMillis(40).toInt
  val MaxIngestRetryAttempts = 2
  val MaxCommandsRetryAttempts = 4
  val EmptyString = ""
  val QueueRetryAttempts = 1

  object Schema {
    val NAME: String = "Name"
    val CSLTYPE: String = "CslType"
    val TYPE: String = "Type"
  }
}

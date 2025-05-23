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
  // val MaxStreamingBytes: Int = 4 * OneMegaByte
  // TODO - make it configureable, user can then fine tune it using the SDK logs reporting fallback to queue (we should
  // make sure its evident that the reason for fallback was the size of the, and that the Max size in the ManagedClient is the same
  // Problem now is actually that the ManagedClient has a limit of 4 mb ... we need to make it configureable in the sdk for this to work
  // TODO when this is configureable - we need 3 tests: 1) max = 10 < one row size  2) row size < max=20< size(all the data), 3) size(data)< max = 4mb
  val DefaultMaxStreamingBytesUncompressed: Int = 4 * OneMegaByte
  val WarnStreamingBytes: Long = 100 * OneMegaByte
  val EmptyString = ""
  val DefaultMaximumIngestionTime: FiniteDuration = FiniteDuration.apply(
    MaxIngestRetryAttempts * (DefaultExecutionQueueing + DefaultTimeoutQueueing) + 2000,
    "millis")
  val QueueRetryAttempts = 1
  val SourceLocationColumnName = "ingestion_source_location_url_blob_internal"

  object Schema {
    val NAME: String = "Name"
    val CSLTYPE: String = "CslType"
    val TYPE: String = "Type"
  }
}

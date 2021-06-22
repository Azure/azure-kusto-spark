package com.microsoft.kusto.spark.utils

import scala.concurrent.duration._

object KustoConstants {
  // Setting high value to have no timeout on Await commands
  val DefaultWaitingIntervalLongRunning: String = (2 days).toSeconds.toString
  val DefaultCleaningInterval: String = (7 days).toSeconds.toString
  val DefaultPeriodicSamplePeriod: FiniteDuration = 1 seconds
  val DefaultIngestionTaskTime: FiniteDuration = 20 seconds
  val NoTimeout: String = (-1 seconds).toSeconds.toString
  val ClientName: String = KustoDataSourceUtils.clientName
  val DefaultBufferSize: Int = 16 * 1024
  val StorageExpiryMinutes: Int = 120
  val SparkSettingsRefreshMinutes: Int = 120
  val OneKiloByte: Int = 1024
  val OneMegaByte: Int = OneKiloByte * OneKiloByte
  val OneGigaByte: Int = OneMegaByte * OneKiloByte
  // The restriction from kusto is 50000 rows but 5000 can still be really big
  val DirectQueryUpperBoundRows = 5000
  val TimeoutForCountCheck: FiniteDuration = 3 seconds
  val IngestByPrefix = "ingest-by:"
  val IngestSkippedTrace = s"Ingestion skipped: Provided ingest-by tags are present in the destination table: "
  val MaxSleepOnMoveExtentsMillis: Int = 3 * 60 * 1000
  val DefaultBatchingLimit: Int = 100
  val DefaultExtentsCountForSplitMergePerNode: Int = 400
  val DefaultMaxRetriesOnMoveExtents: Int = 10
}

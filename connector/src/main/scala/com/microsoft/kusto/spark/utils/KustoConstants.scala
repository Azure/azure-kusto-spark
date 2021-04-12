package com.microsoft.kusto.spark.utils

import scala.concurrent.duration._

object KustoConstants {
  // Setting high value to have no timeout on Await commands
  val DefaultWaitingIntervalLongRunning: String = (2 days).toSeconds.toString
  val DefaultPeriodicSamplePeriod: FiniteDuration = 1 seconds
  val DefaultIngestionTaskTime: FiniteDuration = 20 seconds
  val ClientName: String = KustoDataSourceUtils.clientName
  val DefaultBufferSize: Int = 16 * 1024
  val StorageExpiryMinutes: Int = 120
  val SparkSettingsRefreshMinutes: Int = 120
  val OneKilo: Int = 1024
  val OneMega: Int = OneKilo * 1024
  val OneGiga: Int = OneMega * 1024
  // The restriction from kusto is 50000 rows but 5000 can still be really big
  val DirectQueryUpperBoundRows = 5000
  val TimeoutForCountCheck : FiniteDuration = 3 seconds
  val IngestByPrefix = "ingest-by:"
}

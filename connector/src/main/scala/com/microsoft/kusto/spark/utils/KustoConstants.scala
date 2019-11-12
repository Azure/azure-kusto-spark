package com.microsoft.kusto.spark.utils

import scala.concurrent.duration._

object KustoConstants {
  // Setting high value to have no timeout on Await commands
  val defaultWaitingIntervalLongRunning: String = (2 days).toSeconds.toString
  val defaultPeriodicSamplePeriod: FiniteDuration = 1 seconds
  val defaultIngestionTaskTime: FiniteDuration = 20 seconds
  val clientName: String = KustoDataSourceUtils.ClientName
  val defaultBufferSize: Int = 16 * 1024
  val storageExpiryMinutes: Int = 120
  val sparkSettingsRefreshMinutes: Int = 120
  val oneKilo: Int = 1024
  val oneMega: Int = oneKilo * 1024
  val oneGiga: Int = oneMega * 1024
  val directQueryUpperBoundRows = 1000
}

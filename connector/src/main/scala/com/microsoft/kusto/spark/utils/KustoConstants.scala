package com.microsoft.kusto.spark.utils

import scala.concurrent.duration._

object KustoConstants {
  val DefaultTimeoutLongRunning: FiniteDuration = 90 minutes
  val DefaultTimeoutAsString: String = DefaultTimeoutLongRunning.toSeconds.toString
  val DefaultPeriodicSamplePeriod: FiniteDuration = 2 seconds
  val ClientName: String = "Kusto.Spark.Connector"
  val StorageExpiryMinutes: Int = 120
}

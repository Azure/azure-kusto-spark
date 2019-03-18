package com.microsoft.kusto.spark.utils

import scala.concurrent.duration._

object KustoConstants {
  val defaultTimeoutLongRunning: FiniteDuration = 90 minutes
  val defaultTimeoutAsString: String = defaultTimeoutLongRunning.toSeconds.toString
  val defaultPeriodicSamplePeriod: FiniteDuration = 2 seconds
  val clientName = "Kusto.Spark.Connector"
  val defaultBufferSize: Int = 16 * 1024
}

package com.microsoft.kusto.spark.utils

import scala.concurrent.duration._

object KustoConstants {
  val defaultTimeoutLongRunning: FiniteDuration = 90 minutes
  val defaultTimeoutAsString: String = defaultTimeoutLongRunning.toSeconds.toString
  val defaultPeriodicSamplePeriod: FiniteDuration = 2 seconds
  val clientName = "Kusto.Spark.Connector"
  val defaultBufferSize: Int = 16 * 1024
  val storageExpiryMinutes: Int = 120
  val oneKilo: Int = 1024
  val oneMega: Int = oneKilo * 1024
  val oneGiga: Int = oneMega * 1024
}

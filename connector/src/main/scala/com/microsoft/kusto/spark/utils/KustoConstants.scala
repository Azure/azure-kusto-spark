package com.microsoft.kusto.spark.utils

import scala.concurrent.duration._

object KustoConstants {
  val nonWaitingConst: String = (-1 seconds).toSeconds.toString
  val defaultPeriodicSamplePeriod: FiniteDuration = 1 seconds
  val defaultIngestionTaskTime: FiniteDuration = 10 seconds
  val clientName = "Kusto.Spark.Connector"
  val defaultBufferSize: Int = 16 * 1024
  val storageExpiryMinutes: Int = 120
  val sparkSettingsRefreshMinutes: Int = 120
  val oneKilo: Int = 1024
  val oneMega: Int = oneKilo * 1024
  val oneGiga: Int = oneMega * 1024
  val directQueryUpperBoundRows = 1000
}

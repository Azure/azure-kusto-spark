package com.microsoft.kusto.spark

import java.security.InvalidParameterException

import com.microsoft.azure.kusto.data.Client
import com.microsoft.kusto.spark.datasource.KustoDataSourceUtils._
import com.microsoft.kusto.spark.datasource.{KustoDataSourceUtils => KDSU}

import scala.collection.JavaConverters._
import scala.concurrent.TimeoutException

object KustoTestUtils {
  private val myName = this.getClass.getSimpleName
  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  private [kusto] def validateResultsAndCleanup(
    kustoAdminClient: Client,
    table: String,
    database: String,
    expectedNumberOfRows: Int,
    timeoutMs: Int,
    cleanupAllTables: Boolean = true,
    tableCleanupPrefix: String = "") : Unit = {

    var rowCount = 0
    var timeElapsedMs = 0
    val sleepPeriodMs = timeoutMs / 10

    val query = s"$table | count"

    while (rowCount < expectedNumberOfRows && timeElapsedMs < timeoutMs) {
      val result = kustoAdminClient.execute(database, query)
      rowCount = result.getValues.get(0).get(0).toInt
      Thread.sleep(sleepPeriodMs)
      timeElapsedMs += sleepPeriodMs
    }

    if (cleanupAllTables){
      if (tableCleanupPrefix.isEmpty) throw new InvalidParameterException("Tables cleanup prefix must be set if 'cleanupAllTables' is 'true'")
      dropAllTablesByPrefix(kustoAdminClient, database, tableCleanupPrefix)
    }
    else {
      kustoAdminClient.execute(database, generateDropTablesCommand(table))
    }

    if (rowCount == expectedNumberOfRows) {
      KDSU.logInfo(myName, s"KustoSinkStreamingE2E: Ingestion results validated for table '$table'")
    } else {
      throw new TimeoutException(s"KustoSinkStreamingE2E: Timed out waiting for ingest. $rowCount rows found in database '$database' table '$table', expected: $expectedNumberOfRows. Elapsed time:$timeElapsedMs")
    }
  }

  private[kusto] def dropAllTablesByPrefix(kustoAdminClient: Client, database: String, tablePrefix: String): Unit =
  {
    val res = kustoAdminClient.execute(database, generateFindCurrentTempTablesCommand(tablePrefix))
    val tablesToCleanup = res.getValues.asScala.map(row => row.get(0))

    if (tablesToCleanup.nonEmpty) {
      kustoAdminClient.execute(database, generateDropTablesCommand(tablesToCleanup.mkString(",")))
    }
  }
}

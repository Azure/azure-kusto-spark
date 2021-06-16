package com.microsoft.kusto.spark

import java.security.InvalidParameterException
import java.util.UUID

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{Client, ClientFactory}
import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.JavaConverters._
import scala.concurrent.TimeoutException

private [kusto] object KustoTestUtils {
  private val myName = this.getClass.getSimpleName
  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  def validateResultsAndCleanup(
    kustoAdminClient: Client,
    table: String,
    database: String,
    expectedNumberOfRows: Int, // Set a negative value to skip validation
    timeoutMs: Int,
    cleanupAllTables: Boolean = true,
    tableCleanupPrefix: String = "") : Unit = {

    var rowCount = 0
    var timeElapsedMs = 0
    val sleepPeriodMs = timeoutMs / 10

    val query = s"$table | count"

    while (rowCount < expectedNumberOfRows && timeElapsedMs < timeoutMs) {
      val result = kustoAdminClient.execute(database, query).getPrimaryResults
      result.next()
      rowCount = result.getInt(0)
      Thread.sleep(sleepPeriodMs)
      timeElapsedMs += sleepPeriodMs
    }

    if (cleanupAllTables) {
      if (tableCleanupPrefix.isEmpty) throw new InvalidParameterException("Tables cleanup prefix must be set if 'cleanupAllTables' is 'true'")
      tryDropAllTablesByPrefix(kustoAdminClient, database, tableCleanupPrefix)
    }
    else {
      kustoAdminClient.execute(database, generateDropTablesCommand(table))
    }

    if (expectedNumberOfRows >= 0) {
      if (rowCount == expectedNumberOfRows) {
        KDSU.logInfo(myName, s"KustoSinkStreamingE2E: Ingestion results validated for table '$table'")
      } else {
        throw new TimeoutException(s"KustoSinkStreamingE2E: Timed out waiting for ingest. $rowCount rows found in database '$database' table '$table', expected: $expectedNumberOfRows. Elapsed time:$timeElapsedMs")
      }
    }
  }


  def tryDropAllTablesByPrefix(kustoAdminClient: Client, database: String, tablePrefix: String): Unit =
  {
    try{
      val res = kustoAdminClient.execute(database, generateFindCurrentTempTablesCommand(Array(tablePrefix)))
      val tablesToCleanup = res.getPrimaryResults.getData.asScala.map(row => row.get(0))

      if (tablesToCleanup.nonEmpty) {
        kustoAdminClient.execute(database, generateDropTablesCommand(tablesToCleanup.mkString(",")))
      }
    }catch {
      case exception: Exception =>  KDSU.logWarn(myName, s"Failed to delete temporary tables with exception: ${exception.getMessage}")
    }
  }

  def ingestWithSchemaAdjustment(cluster: String,
                                 database: String,
                                 appId: String,
                                 appKey: String,
                                 authority: String,
                                 df: DataFrame,
                                 targetSchema: String,
                                 schemaAdjustmentMode: String): Unit = {
    val prefix = "KustoBatchSinkE2E_SchemaAdjust"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(database, generateTempTableCreateCommand(table, targetSchema))

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoSinkOptions.KUSTO_CLUSTER, cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, appId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, appKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, authority)
      .option(KustoSinkOptions.KUSTO_ADJUST_SCHEMA, schemaAdjustmentMode)
      .option(KustoSinkOptions.KUSTO_TIMEOUT_LIMIT, (8 * 60).toString)
      .mode(SaveMode.Append)
      .save()

    val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes

    KustoTestUtils.validateResultsAndCleanup(kustoAdminClient, table, database, -1, timeoutMs, tableCleanupPrefix = prefix)
  }
}

package com.microsoft.kusto.spark

import java.security.InvalidParameterException
import java.util.UUID
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{Client, ClientFactory}
import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.datasource.KustoSourceOptions
import com.microsoft.kusto.spark.sql.extension.SparkExtension.DataFrameReaderExtension
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession, functions}


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

  def createTestTable(kustoConnectionOptions: KustoConnectionOptions,
                      prefix: String,
                      targetSchema: String
                     ): String = {

    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://${kustoConnectionOptions.Cluster}.kusto.windows.net",
      kustoConnectionOptions.AppId,
      kustoConnectionOptions.AppKey,
      kustoConnectionOptions.Authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(kustoConnectionOptions.Database, generateTempTableCreateCommand(table, targetSchema))

    table
  }

  def ingest(kustoConnectionOptions: KustoConnectionOptions,
             df: DataFrame,
             table: String,
             schemaAdjustmentMode: String): Unit = {

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.Cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.Database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, kustoConnectionOptions.AppId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, kustoConnectionOptions.AppKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, kustoConnectionOptions.Authority)
      .option(KustoSinkOptions.KUSTO_ADJUST_SCHEMA, schemaAdjustmentMode)
      .option(KustoSinkOptions.KUSTO_TIMEOUT_LIMIT, (8 * 60).toString)
      .mode(SaveMode.Append)
      .save()

   }

  def cleanup(kustoConnectionOptions: KustoConnectionOptions,
              tablePrefix: String): Unit = {

    if (tablePrefix.isEmpty)
      throw new InvalidParameterException("Tables cleanup prefix must be set")

    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://${kustoConnectionOptions.Cluster}.kusto.windows.net",
      kustoConnectionOptions.AppId,
      kustoConnectionOptions.AppKey,
      kustoConnectionOptions.Authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    tryDropAllTablesByPrefix(kustoAdminClient, kustoConnectionOptions.Database, tablePrefix)

  }

  def validateTargetTable(kustoConnectionOptions: KustoConnectionOptions,
                          tableName: String,
                          expectedRows: DataFrame,
                          spark: SparkSession) : Boolean = {

    val conf = Map[String, String](
      KustoSourceOptions.KUSTO_AAD_APP_ID -> kustoConnectionOptions.AppId,
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> kustoConnectionOptions.AppKey,
      KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID -> kustoConnectionOptions.Authority
    )

    val tableRows = spark.read
      .kusto(s"https://${kustoConnectionOptions.Cluster}.kusto.windows.net",
      kustoConnectionOptions.Database, tableName, conf)

    tableRows.count() == tableRows.intersectAll(expectedRows).count()

  }

  case class KustoConnectionOptions(Cluster: String, Database: String, AppId: String, AppKey: String, Authority: String)
}

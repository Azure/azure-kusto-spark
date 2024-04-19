// Copyright (c) 2017 Microsoft Corporation
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.microsoft.kusto.spark

import com.azure.core.credential.TokenRequestContext
import com.azure.identity.{AzureCliCredential, AzureCliCredentialBuilder}
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{Client, ClientFactory}
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.datasink.{
  KustoSinkOptions,
  SinkTableCreationMode,
  SparkIngestionProperties
}
import com.microsoft.kusto.spark.datasource.KustoSourceOptions
import com.microsoft.kusto.spark.sql.extension.SparkExtension.DataFrameReaderExtension
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.{
  generateDropTablesCommand,
  generateFindCurrentTempTablesCommand,
  generateTempTableCreateCommand
}
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.security.InvalidParameterException
import java.util.{Collections, UUID}
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.concurrent.TimeoutException

private[kusto] object KustoTestUtils {
  private val className = this.getClass.getSimpleName
  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  loggingLevel match {
    case Some(level) => KDSU.setLoggingLevel(level)
    // default to warn for tests
    case None => KDSU.setLoggingLevel("WARN")
  }

  def validateResultsAndCleanup(
      kustoAdminClient: Client,
      table: String,
      database: String,
      expectedNumberOfRows: Int, // Set a negative value to skip validation
      timeoutMs: Int,
      cleanupAllTables: Boolean = true,
      tableCleanupPrefix: String = ""): Unit = {

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
      if (tableCleanupPrefix.isEmpty) {
        throw new InvalidParameterException(
          "Tables cleanup prefix must be set if 'cleanupAllTables' is 'true'")
      }
      tryDropAllTablesByPrefix(kustoAdminClient, database, tableCleanupPrefix)
    } else {
      kustoAdminClient.execute(database, generateDropTablesCommand(table))
    }

    if (expectedNumberOfRows >= 0) {
      if (rowCount == expectedNumberOfRows) {
        KDSU.logInfo(
          className,
          s"KustoSinkStreamingE2E: Ingestion results validated for table '$table'")
      } else {
        throw new TimeoutException(
          s"KustoSinkStreamingE2E: Timed out waiting for ingest. $rowCount rows " +
            s"found in database '$database' table '$table', expected: " +
            s"$expectedNumberOfRows. Elapsed time:$timeElapsedMs")
      }
    }
  }

  def tryDropAllTablesByPrefix(
      kustoAdminClient: Client,
      database: String,
      tablePrefix: String): Unit = {
    try {
      val res = kustoAdminClient.execute(
        database,
        generateFindCurrentTempTablesCommand(Array(tablePrefix)))
      val tablesToCleanup = res.getPrimaryResults.getData.asScala.map(row => row.get(0))

      if (tablesToCleanup.nonEmpty) {
        kustoAdminClient.execute(
          database,
          generateDropTablesCommand(tablesToCleanup.mkString(",")))
      }
    } catch {
      case exception: Exception =>
        KDSU.logWarn(
          className,
          s"Failed to delete temporary tables with exception: ${exception.getMessage}")
    }
  }

  def createTestTable(
      kustoConnectionOptions: KustoConnectionOptions,
      prefix: String,
      targetSchema: String): String = {

    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      s"https://${kustoConnectionOptions.cluster}.kusto.windows.net",
      kustoConnectionOptions.accessToken)

    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.execute(
      kustoConnectionOptions.database,
      generateTempTableCreateCommand(table, targetSchema))
    table
  }

  def ingest(
      kustoConnectionOptions: KustoConnectionOptions,
      df: DataFrame,
      table: String,
      schemaAdjustmentMode: String,
      sparkIngestionProperties: SparkIngestionProperties = new SparkIngestionProperties())
      : Unit = {

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoSinkOptions.KUSTO_CLUSTER, kustoConnectionOptions.cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, kustoConnectionOptions.database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoConnectionOptions.accessToken)
      .option(KustoSinkOptions.KUSTO_ADJUST_SCHEMA, schemaAdjustmentMode)
      .option(KustoSinkOptions.KUSTO_TIMEOUT_LIMIT, (8 * 60).toString)
      .option(
        KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON,
        sparkIngestionProperties.toString)
      .option(
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS,
        kustoConnectionOptions.createTableIfNotExists.toString)
      .mode(SaveMode.Append)
      .save()

  }

  def cleanup(kustoConnectionOptions: KustoConnectionOptions, tablePrefix: String): Unit = {
    if (tablePrefix.isEmpty) {
      throw new InvalidParameterException("Tables cleanup prefix must be set")
    }
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      clusterToKustoFQDN(kustoConnectionOptions.cluster),
      kustoConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    tryDropAllTablesByPrefix(kustoAdminClient, kustoConnectionOptions.database, tablePrefix)
  }

  def validateTargetTable(
      kustoConnectionOptions: KustoConnectionOptions,
      tableName: String,
      expectedRows: DataFrame,
      spark: SparkSession): Boolean = {

    val conf = Map[String, String](
      KustoSourceOptions.KUSTO_ACCESS_TOKEN -> kustoConnectionOptions.accessToken)

    val tableRows = spark.read
      .kusto(
        clusterToKustoFQDN(kustoConnectionOptions.cluster),
        kustoConnectionOptions.database,
        tableName,
        conf)

    tableRows.count() == tableRows.intersectAll(expectedRows).count()

  }

  def getSystemTestOptions: KustoConnectionOptions = {
    val cluster: String = KustoTestUtils.getSystemVariable(KustoSinkOptions.KUSTO_CLUSTER)
    val authority: String =
      System.getProperty(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com")
    val clusterScope = clusterToKustoFQDN(cluster)
    val tokenRequestContext = new TokenRequestContext()
      .setScopes(Collections.singletonList(clusterScope))
      .setTenantId(authority)
    val accessToken: String =
      new AzureCliCredentialBuilder().build().getTokenSync(tokenRequestContext).getToken
    val database: String = KustoTestUtils.getSystemVariable(KustoSinkOptions.KUSTO_DATABASE)
    var table: String = KustoTestUtils.getSystemVariable(KustoSinkOptions.KUSTO_TABLE)
    if (table == null) {
      table = "SparkTestTable"
    }
    KustoConnectionOptions(cluster, database, accessToken)
  }

  private def getSystemVariable(key: String) = {
    var value = System.getenv(key)
    if (value == null) {
      value = System.getProperty(key)
    }
    value
  }

  private def clusterToKustoFQDN(cluster: String): String = {
    if (cluster.startsWith("https://")) {
      cluster
    } else {
      s"https://$cluster.kusto.windows.net"
    }
  }

  final case class KustoConnectionOptions(
      cluster: String,
      database: String,
      accessToken: String,
      createTableIfNotExists: SinkTableCreationMode = SinkTableCreationMode.CreateIfNotExist)
}

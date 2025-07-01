// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.azure.core.credential.{AccessToken, TokenRequestContext}
import com.azure.identity.AzureCliCredentialBuilder
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{Client, ClientFactory}
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.datasink.{
  IngestionStorageParameters,
  KustoSinkOptions,
  SinkTableCreationMode,
  SparkIngestionProperties
}
import com.microsoft.kusto.spark.datasource.{KustoSourceOptions, TransientStorageCredentials}
import com.microsoft.kusto.spark.sql.extension.SparkExtension.DataFrameReaderExtension
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.{
  generateDropTablesCommand,
  generateFindCurrentTempTablesCommand,
  generateTempTableCreateCommand
}
import com.microsoft.kusto.spark.utils.{
  ContainerAndSas,
  ContainerProvider,
  KustoQueryUtils,
  KustoDataSourceUtils => KDSU
}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.security.InvalidParameterException
import java.util.{Collections, UUID}
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.concurrent.TimeoutException
import scala.util.Try

private[kusto] object KustoTestUtils {
  private val className = this.getClass.getSimpleName
  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  private val cachedToken: mutable.Map[String, KustoConnectionOptions] =
    new mutable.HashMap[String, KustoConnectionOptions]()
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
      val result = kustoAdminClient.executeQuery(database, query).getPrimaryResults
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
      kustoAdminClient.executeMgmt(database, generateDropTablesCommand(table))
    }

    val someSchema = List(
      StructField(
        "mapToArray",
        MapType(
          StringType,
          new StructType()
            .add("arrayStrings", ArrayType(StringType, containsNull = true), nullable = true),
          valueContainsNull = true),
        nullable = true))
    val someDateSchema = List(
      StructField(
        "mapToStruct",
        MapType(
          StringType,
          new StructType()
            .add("date", DateType, nullable = true)
            .add("time", TimestampType)
            .add("booly", BooleanType)
            .add("deci", DataTypes.createDecimalType(20, 14)),
          valueContainsNull = true),
        nullable = true))
    val someEmptyArraysSchema = List(
      StructField(
        "emptyStruct",
        new StructType()
          .add("emptyArray", ArrayType(StringType, containsNull = true), nullable = true)
          .add("emptyString", StringType)))
    val someDecimalSchema = List(StructField("BigDecimals", DataTypes.createDecimalType(38, 10)))

  def tryDropAllTablesByPrefix(
      kustoAdminClient: Client,
      database: String,
      tablePrefix: String): Unit = {
    try {
      val res = kustoAdminClient.executeMgmt(
        database,
        generateFindCurrentTempTablesCommand(Array(tablePrefix)))
      val tablesToCleanup = res.getPrimaryResults.getData.asScala.map(row => row.get(0))

      if (tablesToCleanup.nonEmpty) {
        kustoAdminClient.executeMgmt(
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

    val conf = Map[String, String](
      KustoSourceOptions.KUSTO_ACCESS_TOKEN -> kustoConnectionOptions.accessToken)

    val tableRows = spark.read
      .kusto(
        clusterToKustoFQDN(kustoConnectionOptions.cluster),
        kustoConnectionOptions.database,
        tableName,
        conf)

    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    kustoAdminClient.executeMgmt(
      kustoConnectionOptions.database,
      generateTempTableCreateCommand(table, targetSchema))
    table
  }

  }

  def getSystemTestOptions: KustoConnectionOptions = {
    val cluster: String = clusterToKustoFQDN(getSystemVariable(KustoSinkOptions.KUSTO_CLUSTER))
    val database: String = getSystemVariable(KustoSinkOptions.KUSTO_DATABASE)
    val table: String =
      Option(getSystemVariable(KustoSinkOptions.KUSTO_TABLE)).getOrElse("SparkTestTable")
    KDSU.logInfo(
      className,
      s"Getting AZCli token for cluster $cluster , database $database & table $table")
    val key = s"$cluster"
    if (cachedToken.contains(key)) {
      cachedToken(key)
    } else {
      val maybeAccessTokenEnv = Option(getSystemVariable(KustoSinkOptions.KUSTO_ACCESS_TOKEN))
      val authority: String = getSystemVariable(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID)
      val clusterScope = s"$cluster/.default"
      KDSU.logWarn(className, s"Using scope $clusterScope and authority $authority")
      val accessToken: String = maybeAccessTokenEnv match {
        case Some(at) =>
          KDSU.logDebug(
            className,
            s"Using access token from environment variable ${KustoSinkOptions.KUSTO_ACCESS_TOKEN}")
          at
        case None =>
          val tokenRequestContext = new TokenRequestContext()
            .setScopes(Collections.singletonList(clusterScope))
            .setTenantId(authority)
          val value =
            new AzureCliCredentialBuilder().build().getToken(tokenRequestContext).block()
          Try(value) match {
            case scala.util.Success(token: AccessToken) =>
              val azCliToken: AccessToken = token
              azCliToken.getToken
            case scala.util.Failure(exception) =>
              KDSU.reportExceptionAndThrow(
                s"Failed to get access token for cluster $cluster, database $database & table $table at scope $clusterScope",
                exception)
              throw exception
          }
      }
      val storageAccountUrl: String = getSystemVariable("storageAccountUrl")
      val connectionOptions = KustoConnectionOptions(
        cluster,
        database,
        accessToken,
        authority,
        storageContainerUrl = Some(storageAccountUrl))
      cachedToken.put(key, connectionOptions)
      KDSU.logDebug(
        className,
        s"Generated token for cluster $cluster, database $database & table $table")
      cachedToken(key)
    }
  }

  def getSystemVariable(key: String): String = {
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

  def generateSasDelegationWithAzCli(storageContainerUrl: String): String = {
    val containerName = storageContainerUrl match {
      case TransientStorageCredentials.SasPattern(_, _, _, container, _) =>
        container
      case _ => throw new InvalidParameterException("Storage url is invalid")
    }
    val ingestionStorageParam =
      new IngestionStorageParameters(storageContainerUrl, containerName, "", "")
    val sas = ContainerProvider.getUserDelegatedSas(
      listPermissions = true,
      cacheExpirySeconds = 1 * 60 * 60,
      ingestionStorageParameter = ingestionStorageParam)
    sas
  }

  final case class KustoConnectionOptions(
                                           cluster: String,
                                           database: String,
                                           accessToken: String,
                                           tenantId: String,
                                           createTableIfNotExists: SinkTableCreationMode = SinkTableCreationMode.CreateIfNotExist,
                                           storageContainerUrl: Option[String] = None)
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.azure.core.credential.{AccessToken, TokenCredential, TokenRequestContext}
import com.azure.identity.{AzureCliCredentialBuilder, ClientAssertionCredentialBuilder}
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.sas.{BlobSasPermission, BlobServiceSasSignatureValues}
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{Client, ClientFactory}
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SinkTableCreationMode, SparkIngestionProperties}
import com.microsoft.kusto.spark.datasource.{KustoSourceOptions, TransientStorageCredentials}
import com.microsoft.kusto.spark.sql.extension.SparkExtension.DataFrameReaderExtension
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.{generateDropTablesCommand, generateFindCurrentTempTablesCommand, generateTempTableCreateCommand}
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import reactor.core.publisher.Mono

import java.security.InvalidParameterException
import java.time.OffsetDateTime
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

  def getSystemTestOptions(isSourceE2E: Boolean = false): KustoConnectionOptions = {
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
          KDSU.logInfo(
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
      if (isSourceE2E) {
        setVariablesForSourceE2EToCashByKey(cluster, database, accessToken, authority, key)
      } else {
        cachedToken.put(key, KustoConnectionOptions(
          cluster,
          database,
          accessToken,
          authority))
      }
      cachedToken(key)
    }
  }

  private def setVariablesForSourceE2EToCashByKey(cluster: String, database: String, accessToken: String,
                                                  authority: String, key: String) = {
    val maybeAccessTokenEnv2 = Option(
      getSystemVariable(KustoSinkOptions.KUSTO_ACCESS_TOKEN + 2))
    val storageAccessToken = maybeAccessTokenEnv2 match {
      case Some(at) =>
        at
      case None =>
        val storageScope = "https://storage.azure.com/.default"
        val tokenRequestContext = new TokenRequestContext()
          .setScopes(Collections.singletonList(storageScope))
          .setTenantId(authority)
        val value =
          new AzureCliCredentialBuilder().build().getToken(tokenRequestContext).block()
        Try(value) match {
          case scala.util.Success(token: AccessToken) =>
            val azCliToken: AccessToken = token
            azCliToken.getToken
          case scala.util.Failure(exception) =>
            KDSU.reportExceptionAndThrow(
              s"Failed to get access token for storage at scope $storageScope",
              exception)
            throw exception
        }
    }

    val storageAccountUrl: String = getSystemVariable("storageAccountUrl")
    cachedToken.put(key, KustoConnectionOptions(cluster, database, accessToken, authority,
      storageAccessToken = Some(storageAccessToken), storageContainerUrl = Some(storageAccountUrl)))
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

  def generateSasDelegationWithAzCli(token:String, storageContainerUrl: String): String = {
    val clientId = getSystemVariable(KustoSinkOptions.KUSTO_AAD_APP_ID)
    val tenantId = getSystemVariable(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID)

    val containerName = storageContainerUrl match {
      case TransientStorageCredentials.SasPattern(
      _, _, _, container, _) =>
        container
      case _ => throw new InvalidParameterException("Storage url is invalid")
    }

    val blobServiceClient = new BlobServiceClientBuilder()
      .endpoint(storageContainerUrl)
      .credential(new AzureCliCredentialBuilder().build())
      .buildClient()

    val containerClient = blobServiceClient.getBlobContainerClient(containerName)

    // Get the user delegation key
    val userDelegationKey = blobServiceClient.getUserDelegationKey(
      OffsetDateTime.now(), OffsetDateTime.now().plusHours(1))

    val blobSasPermission = new BlobSasPermission()
      .setReadPermission(true)
      .setWritePermission(true)
      .setListPermission(true)

    val sasSignatureValues = new BlobServiceSasSignatureValues(
      OffsetDateTime.now().plusDays(1), blobSasPermission)
      .setStartTime(OffsetDateTime.now().minusMinutes(5))

    containerClient.generateUserDelegationSas(sasSignatureValues, userDelegationKey)
  }

  final case class KustoConnectionOptions(
      cluster: String,
      database: String,
      accessToken: String,
      tenantId: String,
      createTableIfNotExists: SinkTableCreationMode = SinkTableCreationMode.CreateIfNotExist,
      storageAccessToken: Option[String] = None, // Used in SourceE2E
      storageContainerUrl: Option[String] = None)
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.source

import com.microsoft.azure.kusto.data.StringUtils
import com.microsoft.kusto.spark.authentication.{KeyVaultAuthentication, KustoAuthentication}
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.datasource._
import com.microsoft.kusto.spark.utils.{
  KustoClientCache,
  KustoConstants,
  KustoQueryUtils,
  KeyVaultUtils,
  OperationMetrics,
  KustoDataSourceUtils => KDSU
}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * V2 Scan that holds the read configuration and implements Batch planning. Determines whether to
 * use single-mode (direct Kusto query) or distributed-mode (export to blob → read parquet).
 */
final class KustoScan(
    readSchema: StructType,
    options: CaseInsensitiveStringMap,
    filtering: KustoFiltering)
    extends Scan
    with Batch {

  private val className = this.getClass.getSimpleName

  override def readSchema(): StructType = readSchema

  override def description(): String = {
    val query = Option(options.get(KustoSourceOptions.KUSTO_QUERY)).getOrElse("")
    val db = Option(options.get(KustoSinkOptions.KUSTO_DATABASE)).getOrElse("")
    s"KustoScan(database=$db, query=${query.take(80)})"
  }

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val planStartNanos = System.nanoTime()
    // Build params from asCaseSensitiveMap, filtering out null values
    val parameters = {
      val jMap = options.asCaseSensitiveMap()
      jMap
        .entrySet()
        .asScala
        .iterator
        .filter(e => Option(e.getValue).isDefined)
        .map(e => e.getKey -> e.getValue)
        .toMap
    }
    val sourceParams = KDSU.parseSourceParameters(parameters, allowProxy = true)

    val authentication: KustoAuthentication =
      resolveAuthentication(sourceParams.keyVaultAuth, sourceParams.authenticationParameters)

    val coordinates = sourceParams.kustoCoordinates
    val requestId = sourceParams.requestId
    val query =
      Option(options.get(KustoSourceOptions.KUSTO_QUERY)).getOrElse("")
    val timeout = new FiniteDuration(
      Option(options.get(KustoSourceOptions.KUSTO_TIMEOUT_LIMIT))
        .getOrElse(KustoConstants.DefaultWaitingIntervalLongRunning)
        .toLong,
      TimeUnit.SECONDS)

    val kustoClient = KustoClientCache.getClient(
      coordinates.clusterUrl,
      authentication,
      coordinates.ingestionUrl,
      coordinates.clusterAlias)

    val readModeOpt = parameters
      .get(KustoSourceOptions.KUSTO_READ_MODE)
      .flatMap(v => Option(v))
      .map(ReadMode.withName)
    val isUserForceSingle = readModeOpt.contains(ReadMode.ForceSingleMode)

    val useSingleMode: Boolean = readModeOpt match {
      case Some(_) => isUserForceSingle
      case None =>
        val estimate = Try(
          KDSU.estimateRowsCount(
            kustoClient.engineClient,
            query,
            coordinates.database,
            sourceParams.clientRequestProperties))
        estimate match {
          case Success(count) => count <= KustoConstants.DirectQueryUpperBoundRows
          case Failure(_) => false
        }
    }

    val partitions: Array[InputPartition] = if (useSingleMode) {
      KDSU.logInfo(className, s"Planning single-mode read for requestId: $requestId")
      Array(
        KustoSingleModeInputPartition(
          coordinates = coordinates,
          authentication = authentication,
          query = query,
          readSchema = readSchema,
          filtering = filtering,
          requestId = requestId))
    } else {
      KDSU.logInfo(className, s"Planning distributed-mode read for requestId: $requestId")
      planDistributedPartitions(
        parameters,
        coordinates,
        authentication,
        query,
        timeout,
        requestId)
    }

    OperationMetrics.logMetric(
      className,
      "v2.scan.planInputPartitions",
      (System.nanoTime() - planStartNanos) / 1000000L,
      Map(
        "mode" -> (if (useSingleMode) "single" else "distributed"),
        "partitions" -> partitions.length.toString,
        "requestId" -> requestId))
    partitions
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new KustoPartitionReaderFactory(readSchema)
  }

  private def planDistributedPartitions(
      parameters: Map[String, String],
      coordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      query: String,
      timeout: FiniteDuration,
      requestId: String): Array[InputPartition] = {

    val kustoClient = KustoClientCache.getClient(
      coordinates.clusterUrl,
      authentication,
      coordinates.ingestionUrl,
      coordinates.clusterAlias)

    val storageOption = parameters.get(KustoSourceOptions.KUSTO_TRANSIENT_STORAGE)
    val transientStorage: TransientStorageParameters =
      if (storageOption.isDefined) {
        val params = TransientStorageParameters.fromString(storageOption.get)
        params.storageCredentials.foreach { st =>
          st.validate()
          if (StringUtils.isNotBlank(st.domainSuffix)) {
            params.endpointSuffix = st.domainSuffix
          }
        }
        params
      } else {
        kustoClient.getTempBlobsForExport
      }

    val storageProtocol = parameters
      .getOrElse(KustoSourceOptions.STORAGE_PROTOCOL, KustoConstants.storageProtocolWasbs)

    val schema = KustoSchema(readSchema, Set())
    val filteredQuery = KustoFilter.pruneAndFilter(schema, query, filtering)

    val directory = s"${coordinates.database}/dir${UUID.randomUUID()}/"
      .replaceAll("[^0-9a-zA-Z/]", "_")

    // Export data to blob storage
    val exportStartNanos = System.nanoTime()
    val exportCommand = com.microsoft.kusto.spark.utils.CslCommandsGenerator
      .generateExportDataCommand(
        query = filteredQuery,
        directory = directory,
        partitionId = 0,
        storageParameters = transientStorage,
        partitionPredicate = None,
        additionalExportOptions = Map.empty,
        supportNewParquetWriter = true)

    val commandResult = kustoClient
      .executeEngine(
        coordinates.database,
        exportCommand,
        "v2ExportPartitionToBlob",
        new com.microsoft.azure.kusto.data.ClientRequestProperties())
      .getPrimaryResults

    KDSU.verifyAsyncCommandCompletion(
      kustoClient.engineClient,
      coordinates.database,
      commandResult,
      timeOut = timeout,
      doingWhat = s"V2 export data to blob directory '$directory'",
      loggerName = className,
      requestId = requestId)

    OperationMetrics.logMetric(
      className,
      "v2.scan.exportToBlob",
      (System.nanoTime() - exportStartNanos) / 1000000L,
      Map("directory" -> directory, "requestId" -> requestId))

    // Build parquet paths from storage credentials
    val paths = transientStorage.storageCredentials
      .map { cred =>
        val sasToken = if (cred.sasKey.startsWith("?")) cred.sasKey else s"?${cred.sasKey}"
        KustoDistributedInputPartition(
          blobPath =
            s"$storageProtocol://${cred.blobContainer}@${cred.storageAccountName}.blob.${transientStorage.endpointSuffix}/$directory",
          storageAccountName = cred.storageAccountName,
          storageAccountKey = cred.storageAccountKey,
          sasKey = cred.sasKey,
          blobContainer = cred.blobContainer,
          endpointSuffix = transientStorage.endpointSuffix,
          storageProtocol = storageProtocol,
          readSchema = readSchema,
          requestId = requestId): InputPartition
      }

    if (paths.isEmpty) {
      KDSU.logWarn(className, s"No export paths found for requestId: $requestId")
      Array(KustoEmptyInputPartition(readSchema): InputPartition)
    } else {
      paths
    }
  }

  private def resolveAuthentication(
      keyVaultAuth: Option[KeyVaultAuthentication],
      baseAuth: KustoAuthentication): KustoAuthentication = {
    if (keyVaultAuth.isDefined) {
      KDSU.mergeKeyVaultAndOptionsAuthentication(
        KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultAuth.get),
        Some(baseAuth))
    } else {
      baseAuth
    }
  }
}

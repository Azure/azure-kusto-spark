// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.ingest.v2.client.IngestionOperation
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.{IngestionFormat, WriteMode, WriteOptions}
import com.microsoft.kusto.spark.utils.{
  KustoClientCache,
  KustoConstants => KCONST,
  KustoDataSourceUtils => KDSU
}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.CollectionAccumulator

import java.time.{Clock, Instant}
import scala.jdk.CollectionConverters._

/**
 * Top-level orchestrator for the kusto-ingest-v2 SDK write path. This is the single entry point
 * called from KustoWriter when V2 ingestion is active (the default path).
 *
 * Responsibilities:
 *   - Creates its own IngestV2ClientProvider (self-contained lifecycle)
 *   - Decides queued vs streaming based on WriteMode
 *   - Handles transactional finalization (poll → move extents → cleanup)
 *   - Has NO shared state with the v1 write path
 *
 * Note: Still uses ExtendedKustoClient for management commands (table creation, schema
 * validation, move extents) since those are not ingestion operations.
 */
object IngestV2WriterOrchestrator {
  private val myName = this.getClass.getSimpleName
  private val ConnectorVersion = KCONST.ClientName

  /**
   * Main entry point: write a DataFrame to Kusto using the kusto-ingest-v2 SDK. Called from
   * KustoWriter.write() when V2 ingestion is active (the default path).
   *
   * NOW WITH CONFIG API: Queries config API to validate V2 support and fetch DM capabilities.
   */
  def write(
      data: DataFrame,
      tableCoordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions,
      tmpTableName: String,
      crp: ClientRequestProperties): Unit = {

    val database = tableCoordinates.database
    val table = tableCoordinates.table.get
    val dmUrl = tableCoordinates.ingestionUrl.getOrElse(
      s"https://ingest-${tableCoordinates.clusterUrl.stripPrefix("https://")}")

    KDSU.logInfo(
      myName,
      s"Starting ingest-v2 write to $database.$table (mode: ${writeOptions.writeMode}, format: ${writeOptions.ingestionFormat})")
    KDSU.logDebug(myName, s"DM URL: $dmUrl, requestId: ${writeOptions.requestId}")

    // PHASE 2: Query config API and validate V2 support
    val dmConfig = IngestV2ConfigurationProvider.getConfiguration(dmUrl, authentication)

    // Honor the config API contract: preferredIngestionMethod must be "REST"
    dmConfig match {
      case Some(config) if config.preferredIngestionMethod == "REST" =>
        KDSU.logInfo(
          myName,
          s"Config API: preferredIngestionMethod=REST → proceeding with V2 ingestion")

      case Some(config) =>
        val method = config.preferredIngestionMethod
        val errorMsg =
          s"Config API returned preferredIngestionMethod=$method (expected REST). " +
            s"DM endpoint $dmUrl does not support V2 REST ingestion."
        KDSU.logWarn(myName, errorMsg)
        throw new IngestV2FallbackException(errorMsg)

      case None =>
        val errorMsg =
          s"Config API not available for DM endpoint $dmUrl (404 or error). " +
            s"Cannot validate V2 support."
        KDSU.logWarn(myName, errorMsg)
        throw new IngestV2FallbackException(errorMsg)
    }

    // Serialize configuration for executors (avoid serializing non-serializable SDK clients)
    val dmUrlForExecutors = dmUrl
    val authForExecutors = authentication
    val connectorVersionForExecutors = ConnectorVersion

    // Serialize config for executors (batch limits, storage paths)
    val dmConfigForExecutors = dmConfig

    // We still need the engine client for management commands (table operations,
    // move extents) in transactional mode. Not used for container discovery.
    val kustoClient = KustoClientCache.getClient(
      tableCoordinates.clusterUrl,
      authentication,
      tableCoordinates.ingestionUrl,
      tableCoordinates.clusterAlias)

    val rdd = data.queryExecution.toRdd
    val schema = data.schema
    val sinkStartTime = Instant.now(Clock.systemUTC())

    // Determine the target table for ingestion
    val ingestionTable =
      if (writeOptions.writeMode == WriteMode.Transactional) tmpTableName else table

    val batchIdForTracing = writeOptions.requestId

    // Get cached client provider on driver for transactional finalization
    val clientProvider = IngestV2ClientCache.getClient(dmUrl, authentication, ConnectorVersion)

    try {
      if (writeOptions.ingestionFormat == IngestionFormat.Parquet &&
        writeOptions.writeMode != WriteMode.KustoStreaming) {
        // Parquet mode: use Spark's native Parquet writer → blob → ingest-v2
        KDSU.logInfo(myName, s"Using Parquet ingestion format for $database.$ingestionTable")
        val operations = IngestV2ParquetWriter.ingestDataFrame(
          data,
          database,
          ingestionTable,
          clientProvider.queuedClient,
          dmConfig.get,
          writeOptions,
          batchIdForTracing)

        if (writeOptions.writeMode == WriteMode.Transactional) {
          IngestV2FinalizeHelper.finalizeTransactionalIngestion(
            operations,
            clientProvider.queuedClient,
            kustoClient,
            tableCoordinates,
            tmpTableName,
            writeOptions,
            crp,
            sinkStartTime)
        }
      } else if (writeOptions.writeMode == WriteMode.KustoStreaming) {
        // Streaming mode via kusto-ingest-v2 ManagedStreamingIngestClient
        KDSU.logInfo(myName, s"Using streaming ingestion for $database.$ingestionTable")
        val allOperations = new CollectionAccumulator[List[IngestionOperation]]
        rdd.sparkContext.register(allOperations, "ingestV2StreamingOps")

        rdd.foreachPartition { rows: Iterator[InternalRow] =>
          if (rows.nonEmpty) {
            // Get cached client on executor (avoids serialization)
            val executorClientProvider =
              IngestV2ClientCache.getClient(
                dmUrlForExecutors,
                authForExecutors,
                connectorVersionForExecutors)

            val ops = IngestV2StreamingWriter.ingestPartition(
              rows,
              schema,
              database,
              ingestionTable,
              executorClientProvider.managedStreamingClient,
              writeOptions,
              batchIdForTracing)
            allOperations.add(ops)
          }
        }

        if (writeOptions.writeMode == WriteMode.Transactional) {
          val operations = allOperations.value.asScala.flatten.toList
          IngestV2FinalizeHelper.finalizeTransactionalIngestion(
            operations,
            clientProvider.queuedClient,
            kustoClient,
            tableCoordinates,
            tmpTableName,
            writeOptions,
            crp,
            sinkStartTime)
        }
      } else {
        // Queued mode via kusto-ingest-v2 QueuedIngestClient (default)
        KDSU.logInfo(myName, s"Using queued CSV ingestion for $database.$ingestionTable")
        val allOperations = new CollectionAccumulator[List[IngestionOperation]]
        rdd.sparkContext.register(allOperations, "ingestV2QueuedOps")

        rdd.foreachPartition { rows: Iterator[InternalRow] =>
          if (rows.nonEmpty) {
            // Get cached client on executor (avoids serialization)
            val executorClientProvider =
              IngestV2ClientCache.getClient(
                dmUrlForExecutors,
                authForExecutors,
                connectorVersionForExecutors)

            val ops = IngestV2QueuedWriter.ingestPartition(
              rows,
              schema,
              database,
              ingestionTable,
              executorClientProvider.queuedClient,
              dmConfigForExecutors,
              writeOptions,
              batchIdForTracing
            )
            allOperations.add(ops)
          }
        }

        if (writeOptions.writeMode == WriteMode.Transactional) {
          val operations = allOperations.value.asScala.flatten.toList
          IngestV2FinalizeHelper.finalizeTransactionalIngestion(
            operations,
            clientProvider.queuedClient,
            kustoClient,
            tableCoordinates,
            tmpTableName,
            writeOptions,
            crp,
            sinkStartTime)
        }
      }

      KDSU.logInfo(myName, s"Ingest-v2 write completed successfully for $database.$table")
    } catch {
      case e: Exception =>
        KDSU.logError(myName, s"Ingest-v2 write failed for $database.$table: ${e.getMessage}")
        if (writeOptions.writeMode == WriteMode.Transactional) {
          try {
            kustoClient.cleanupIngestionByProducts(database, tmpTableName, crp)
          } catch {
            case cleanup: Exception =>
              KDSU.logWarn(myName, s"Failed to cleanup after failure: ${cleanup.getMessage}")
          }
        }
        throw e
    } finally {
      clientProvider.close()
    }
  }
}

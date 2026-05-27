// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.v2.{IngestV2ConfigurationProvider, IngestionConfig}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

/**
 * Smart factory that auto-detects V2 support via config API and creates the appropriate ingest
 * client.
 *
 * HONORS THE CONFIG API CONTRACT:
 * - Query /v1/rest/ingestion/configuration
 * - If preferredIngestionMethod == "REST" → create V2 client
 * - If preferredIngestionMethod == "Legacy" or 404 → create V1 client
 *
 * This enables zero-configuration ingestion - users don't need to set useIngestV2.
 */
object SmartIngestClientFactory {
  private val myName = this.getClass.getSimpleName

  /**
   * Create an IngestClient with automatic V2 detection.
   *
   * Decision logic:
   * 1. If writeOptions.useIngestV2 is explicitly set → honor it (manual override)
   * 2. Otherwise, query config API and auto-detect based on preferredIngestionMethod
   *
   * @param tableCoordinates
   *   Kusto cluster, database, table
   * @param authentication
   *   Authentication config
   * @param writeOptions
   *   Write options (may contain explicit useIngestV2 override)
   * @return
   *   IngestClient (either V1 or V2 wrapper)
   */
  def createClient(
      tableCoordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions): IngestClient = {

    val dmUrl = tableCoordinates.ingestionUrl.getOrElse(
      s"https://ingest-${tableCoordinates.clusterUrl.stripPrefix("https://")}")

    // Check for manual override first
    if (writeOptions.useIngestV2) {
      KDSU.logInfo(myName, s"Using ingest-v2 SDK (manual override: useIngestV2=true)")
      return createV2Client(tableCoordinates, authentication, writeOptions, None)
    }

    // No manual override - query config API for auto-detection
    val configOpt = IngestV2ConfigurationProvider.getConfiguration(dmUrl, authentication)

    val shouldUseV2 = configOpt match {
      case Some(config) if config.preferredIngestionMethod == "REST" =>
        KDSU.logInfo(
          myName,
          s"Config API: preferredIngestionMethod=REST → using ingest-v2 SDK (auto-detected)")
        true

      case Some(config) =>
        KDSU.logInfo(
          myName,
          s"Config API: preferredIngestionMethod=${config.preferredIngestionMethod} → using ingest-v1 SDK (auto-fallback)")
        false

      case None =>
        KDSU.logInfo(
          myName,
          s"Config API not available for $dmUrl → using ingest-v1 SDK (auto-fallback)")
        false
    }

    if (shouldUseV2) {
      createV2Client(tableCoordinates, authentication, writeOptions, configOpt)
    } else {
      createV1Client(tableCoordinates, authentication, writeOptions)
    }
  }

  /**
   * Create V2 client wrapper.
   *
   * @param configOpt
   *   Optional config from DM (for batch limits, storage paths)
   */
  private def createV2Client(
      tableCoordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions,
      configOpt: Option[IngestionConfig]): IngestClient = {

    new IngestV2ClientWrapper(tableCoordinates, authentication, writeOptions, configOpt)
  }

  /**
   * Create V1 client wrapper (legacy queue-based ingestion).
   */
  private def createV1Client(
      tableCoordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions): IngestClient = {

    new IngestV1ClientWrapper(tableCoordinates, authentication, writeOptions)
  }
}

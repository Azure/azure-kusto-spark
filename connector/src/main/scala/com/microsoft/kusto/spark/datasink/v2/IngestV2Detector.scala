// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

/**
 * Detector for V2 ingestion support using config API.
 *
 * HONORS THE CONFIG API CONTRACT:
 *   - Query /v1/rest/ingestion/configuration
 *   - If preferredIngestionMethod == "REST" → V2 supported
 *   - If preferredIngestionMethod == "Legacy" or 404 → V1 fallback
 *
 * This enables zero-configuration V2 ingestion as the default path.
 */
object IngestV2Detector {
  private val myName = this.getClass.getSimpleName

  /**
   * Check if V2 ingestion (REST-based) is supported by querying config API.
   *
   * Decision logic (THE CONTRACT):
   *   - Query config API at /v1/rest/ingestion/configuration
   *   - Parse preferredIngestionMethod field
   *   - Return true ONLY if preferredIngestionMethod == "REST"
   *   - Return false for "Legacy", 404, errors, or missing config
   *
   * @param coordinates
   *   Kusto cluster coordinates (ingestionUrl extracted from here)
   * @param authentication
   *   Authentication configuration for config API
   * @return
   *   true if V2 supported (REST), false otherwise (fallback to V1)
   */
  def isV2Supported(
      coordinates: KustoCoordinates,
      authentication: KustoAuthentication): Boolean = {

    // Construct DM URL from coordinates
    val dmUrl = coordinates.ingestionUrl.getOrElse(
      s"https://ingest-${coordinates.clusterUrl.stripPrefix("https://")}")

    KDSU.logDebug(myName, s"Querying config API to detect V2 support for DM: $dmUrl")

    // Query config API (with caching)
    val configOpt = IngestV2ConfigurationProvider.getConfiguration(dmUrl, authentication)

    // Honor the config API contract
    val isSupported = configOpt match {
      case Some(config) if config.preferredIngestionMethod == "REST" =>
        KDSU.logInfo(
          myName,
          s"Config API: preferredIngestionMethod=REST → V2 ingestion enabled (auto-detected)")
        true

      case Some(config) =>
        KDSU.logInfo(
          myName,
          s"Config API: preferredIngestionMethod=${config.preferredIngestionMethod} → V1 ingestion (auto-fallback)")
        false

      case None =>
        KDSU.logInfo(myName, s"Config API unavailable for $dmUrl → V1 ingestion (auto-fallback)")
        false
    }

    isSupported
  }
}

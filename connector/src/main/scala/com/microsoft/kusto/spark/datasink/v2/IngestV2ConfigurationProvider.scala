// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.ConfigurationClient
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

/**
 * Queries and caches DM configuration for ingest-v2.
 *
 * CRITICAL: Honors the config API contract:
 *   - Query /v1/rest/ingestion/configuration
 *   - If preferredIngestionMethod == "REST" → use V2
 *   - If preferredIngestionMethod == "Legacy" or missing or 404 → use V1
 *
 * This is the authoritative source of truth for V1 vs V2 decision.
 */
object IngestV2ConfigurationProvider {
  private val myName = this.getClass.getSimpleName

  // Cache: dmUrl -> Option[IngestionConfig]
  // Both Some(config) and None (404/error) are cached
  private val configCache =
    new ConcurrentHashMap[String, Option[IngestionConfig]]()

  /**
   * Query ingestion configuration from DM, with caching.
   *
   * Returns:
   *   - Some(config) if config API succeeds
   *   - None if config API returns 404, network error, or auth failure
   *
   * Result is cached for session lifetime.
   */
  def getConfiguration(
      dmUrl: String,
      authentication: KustoAuthentication): Option[IngestionConfig] = {

    // Check cache first
//    if (configCache.containsKey(dmUrl)) {
//      val cached = configCache.get(dmUrl)
//      KDSU.logDebug(myName, s"Using cached config for $dmUrl: ${cached.isDefined}")
//      return cached
//    }

    // Query config API
    val configOpt = queryConfigurationAPI(dmUrl, authentication)

    // Cache result (both Some and None)
//    configCache.put(dmUrl, configOpt)

    configOpt
  }

  /**
   * Query /v1/rest/ingestion/configuration endpoint.
   *
   * Uses V2 SDK's ConfigurationClient to query the config API. Logs the response at INFO level
   * for debugging (critical for rollout).
   */
  private def queryConfigurationAPI(
      dmUrl: String,
      authentication: KustoAuthentication): Option[IngestionConfig] = {

    KDSU.logInfo(myName, s"Querying config API for $dmUrl")

    val result = Try {
      // Create token credential
      val tokenCredential =
        IngestV2Authentication.createTokenCredential(authentication)

      // Create ConfigurationClient (no Fabric Private Link for now - use defaults)
      val configClient = new ConfigurationClient(
        dmUrl,
        tokenCredential,
        false, // skipSecurityChecks
        new ClientDetails(
          "Kusto.Spark.Connector",
          KDSU.clientName,
          "" // appName
        ),
        null, // s2sTokenProvider - not used for now
        null // s2sFabricPrivateLinkAccessContext - not used for now
      )

      // Query config endpoint - this is a Kotlin suspend function
      // We need to call it from Java/Scala by treating it as a CompletableFuture
      val future = configClient
        .getConfigurationDetails(null)
        .asInstanceOf[CompletableFuture[
          com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse]]

      // Wait for result with timeout
      val response = future.get(30, java.util.concurrent.TimeUnit.SECONDS)

      // Parse response
      IngestionConfig.fromConfigurationResponse(response)
    }

    result match {
      case Success(Some(config)) =>
        // Config API succeeded and parsed successfully
        logConfigResponse(dmUrl, config)
        Some(config)

      case Success(None) =>
        // Config API succeeded but failed to parse
        KDSU.logWarn(
          myName,
          s"Config API returned response but failed to parse for $dmUrl. Fallback to V1.")
        None

      case Failure(exception) =>
        // Config API failed (404, network error, auth failure)
        KDSU.logDebug(myName, s"Config API not available for $dmUrl: ${exception.getMessage}")
        KDSU.logInfo(myName, s"Fallback to V1 ingestion for $dmUrl")
        None
    }
  }

  /**
   * Log config response at INFO level.
   *
   * CRITICAL: Required for debugging rollout issues. Config may vary across environments, feature
   * flags, etc.
   */
  private def logConfigResponse(dmUrl: String, config: IngestionConfig): Unit = {
    KDSU.logInfo(myName, s"Config API response for $dmUrl:")
    KDSU.logInfo(myName, s"  preferredIngestionMethod: ${config.preferredIngestionMethod}")
    KDSU.logInfo(myName, s"  preferredUploadMethod: ${config.preferredUploadMethod}")
    KDSU.logInfo(myName, s"  maxBlobsPerBatch: ${config.maxBlobsPerBatch}")
    KDSU.logInfo(myName, s"  maxDataSizeBytes: ${config.maxDataSizeBytes}")
    KDSU.logInfo(myName, s"  blobPaths: ${config.blobPaths.size} containers")
    KDSU.logInfo(myName, s"  oneLakePaths: ${config.oneLakePaths.size} paths")
  }

  /**
   * Clear cache (for testing purposes).
   */
  def clearCache(): Unit = {
    configCache.clear()
    KDSU.logDebug(myName, "Config cache cleared")
  }
}

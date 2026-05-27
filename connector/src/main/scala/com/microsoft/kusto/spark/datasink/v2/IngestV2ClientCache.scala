// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

import java.util.concurrent.ConcurrentHashMap

/**
 * Thread-safe singleton cache for IngestV2ClientProvider instances. Follows the same pattern as
 * KustoClientCache to avoid Spark serialization issues while enabling client reuse per executor.
 *
 * Each executor JVM maintains its own cache, so multiple partitions on the same executor reuse
 * the same IngestV2ClientProvider instance (and thus the same SDK clients).
 */
object IngestV2ClientCache {
  private val myName = this.getClass.getSimpleName
  private val cache = new ConcurrentHashMap[String, IngestV2ClientProvider]()

  /**
   * Get a cached IngestV2ClientProvider or create a new one if not present. Thread-safe for
   * concurrent access from multiple Spark tasks on the same executor.
   *
   * @param dmUrl
   *   Data Management (ingest) endpoint URL
   * @param authentication
   *   Authentication configuration (must be serializable)
   * @param connectorVersion
   *   Connector version string for SDK client details
   * @return
   *   Cached or newly created IngestV2ClientProvider
   */
  def getClient(
      dmUrl: String,
      authentication: KustoAuthentication,
      connectorVersion: String): IngestV2ClientProvider = {

    // Cache key based on DM URL and authentication identity
    val key = s"$dmUrl-${authentication.hashCode()}"

    cache.computeIfAbsent(
      key,
      _ => {
        KDSU.logInfo(myName, s"Creating new IngestV2ClientProvider for DM: $dmUrl")
        new IngestV2ClientProvider(dmUrl, authentication, connectorVersion)
      })
  }

  /**
   * Clear all cached clients. Useful for testing or forced refresh scenarios. Calls close() on
   * each cached provider before removing.
   */
  def clearCache(): Unit = {
    KDSU.logInfo(myName, s"Clearing IngestV2ClientCache (${cache.size()} entries)")
    cache.values().forEach(_.close())
    cache.clear()
  }

  /** Get the number of cached client providers. Useful for diagnostics. */
  def size(): Int = cache.size()
}

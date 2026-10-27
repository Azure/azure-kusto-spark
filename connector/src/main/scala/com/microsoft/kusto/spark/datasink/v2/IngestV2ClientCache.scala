// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

import java.util.concurrent.ConcurrentHashMap

/**
 * Per-JVM cache of IngestV2ClientProvider instances, keyed by DM URL and authentication (by
 * value, like KustoClientCache). Cached providers are shared by concurrent writes and tasks, so
 * callers must never close them.
 */
object IngestV2ClientCache {
  private val myName = this.getClass.getSimpleName

  private final case class CacheKey(dmUrl: String, authentication: KustoAuthentication)

  private val cache = new ConcurrentHashMap[CacheKey, IngestV2ClientProvider]()

  def getClient(
      dmUrl: String,
      authentication: KustoAuthentication,
      connectorVersion: String): IngestV2ClientProvider =
    cache.computeIfAbsent(
      CacheKey(dmUrl, authentication),
      _ => {
        KDSU.logInfo(myName, s"Creating IngestV2ClientProvider for DM: $dmUrl")
        new IngestV2ClientProvider(dmUrl, authentication, connectorVersion)
      })

  /** Closes and removes all cached clients (tests). */
  def clearCache(): Unit = {
    cache.values().forEach(_.close())
    cache.clear()
  }

  def size(): Int = cache.size()
}

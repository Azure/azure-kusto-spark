// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.ConfigurationClient
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

import java.time.Duration
import java.util.concurrent.{ConcurrentHashMap, TimeUnit, TimeoutException}
import scala.util.{Failure, Success, Try}

/**
 * Reads the DM ingestion configuration (`/v1/rest/ingestion/configuration`) through the ingest-v2
 * SDK `ConfigurationClient`.
 *
 * Only successful answers are cached (per DM URL and authentication), for the service's
 * `refreshInterval` capped at one hour, like the SDK `DefaultConfigurationCache`. Failures are
 * never cached, so a transient error does not pin a whole session to the V1 path.
 *
 * The raw response is never logged because storage container URLs carry SAS tokens.
 */
object IngestV2ConfigurationProvider {
  private val myName = this.getClass.getSimpleName
  private[v2] val MaxCacheTtl: Duration = Duration.ofHours(1)
  private[v2] val MinCacheTtl: Duration = Duration.ofMinutes(1)
  private val RequestTimeoutSeconds = 30L

  private final case class CacheKey(dmUrl: String, authentication: KustoAuthentication)
  private final case class CachedEntry(config: IngestionConfig, expiresAtNanos: Long)

  private val configCache = new ConcurrentHashMap[CacheKey, CachedEntry]()

  /** Returns the configuration, or the reason it could not be read. */
  def getConfiguration(
      dmUrl: String,
      authentication: KustoAuthentication): Try[IngestionConfig] = {
    val key = CacheKey(dmUrl, authentication)
    val cached = configCache.get(key)
    if (cached != null && System.nanoTime() - cached.expiresAtNanos < 0) {
      return Success(cached.config)
    }

    val result = queryConfigurationApi(dmUrl, authentication)
    result match {
      case Success((config, ttl)) =>
        configCache.put(key, CachedEntry(config, System.nanoTime() + ttl.toNanos))
        KDSU.logInfo(myName, s"Ingestion configuration for $dmUrl: $config")
      case Failure(e) =>
        configCache.remove(key)
        KDSU.logWarn(
          myName,
          s"Could not read the ingestion configuration from $dmUrl: " +
            s"${e.getClass.getSimpleName}: ${e.getMessage}")
    }
    result.map(_._1)
  }

  private def queryConfigurationApi(
      dmUrl: String,
      authentication: KustoAuthentication): Try[(IngestionConfig, Duration)] = Try {
    val configClient = new ConfigurationClient(
      dmUrl,
      IngestV2Authentication.createTokenCredential(authentication),
      false, // skipSecurityChecks
      new ClientDetails(IngestV2ClientProvider.ApplicationName, KDSU.clientName, ""),
      null, // s2sTokenProvider
      null // s2sFabricPrivateLinkAccessContext
    )

    // Bridge the Kotlin suspend function to a CompletableFuture.
    val future = kotlinx.coroutines.future.FutureKt.future(
      kotlinx.coroutines.GlobalScope.INSTANCE,
      kotlin.coroutines.EmptyCoroutineContext.INSTANCE,
      kotlinx.coroutines.CoroutineStart.DEFAULT,
      new kotlin.jvm.functions.Function2[
        kotlinx.coroutines.CoroutineScope,
        kotlin.coroutines.Continuation[_ >: ConfigurationResponse],
        AnyRef] {
        override def invoke(
            scope: kotlinx.coroutines.CoroutineScope,
            cont: kotlin.coroutines.Continuation[_ >: ConfigurationResponse]): AnyRef =
          configClient.getConfigurationDetails(cont)
      })

    val response =
      try future.get(RequestTimeoutSeconds, TimeUnit.SECONDS)
      catch {
        case e: TimeoutException =>
          // Cancels the coroutine so the request does not keep running in the background.
          future.cancel(true)
          throw e
        case e: java.util.concurrent.ExecutionException if e.getCause != null =>
          throw e.getCause
      }

    val config = IngestionConfig
      .fromConfigurationResponse(response)
      .getOrElse(throw new IllegalStateException("The ingestion configuration is empty"))
    val refresh = Option(response.getContainerSettings)
      .flatMap(s => parseTimeSpan(s.getRefreshInterval))
    (config, cacheTtl(refresh))
  }

  private[v2] def cacheTtl(refreshInterval: Option[Duration]): Duration =
    refreshInterval match {
      case Some(d) if d.compareTo(MinCacheTtl) < 0 => MinCacheTtl
      case Some(d) if d.compareTo(MaxCacheTtl) < 0 => d
      case _ => MaxCacheTtl
    }

  /** Parses a .NET TimeSpan string ("hh:mm:ss", "d.hh:mm:ss", optional fraction). */
  private[v2] def parseTimeSpan(value: String): Option[Duration] = {
    val pattern = """^(?:(\d+)\.)?(\d{1,2}):(\d{1,2}):(\d{1,2})(?:\.\d+)?$""".r
    Option(value).map(_.trim).flatMap {
      case pattern(days, h, m, s) =>
        Try(
          Duration
            .ofDays(Option(days).map(_.toLong).getOrElse(0L))
            .plusHours(h.toLong)
            .plusMinutes(m.toLong)
            .plusSeconds(s.toLong)).toOption
      case _ => None
    }
  }

  /** Clears the cache (tests). */
  def clearCache(): Unit = configCache.clear()
}

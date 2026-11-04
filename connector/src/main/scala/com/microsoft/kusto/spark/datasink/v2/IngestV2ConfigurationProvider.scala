// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.ConfigurationClient
import com.microsoft.azure.kusto.ingest.v2.common.{CachedConfigurationData, ConfigurationCache}
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
 * One cache entry is kept per DM URL and authentication and is shared by the planning code on the
 * driver and by the SDK clients (through [[IngestV2ConfigurationCache]]), so the SDK never opens
 * its own configuration clients. At most one request per key is in flight at a time.
 *
 *   - A successful answer is kept for the service's `refreshInterval`, capped at one hour.
 *   - If a refresh fails, the last good answer keeps being served (and the refresh is retried
 *     after [[StaleRetryInterval]]), so a short outage of the configuration API does not move
 *     running jobs to another path.
 *   - If there is no good answer yet, the first read retries transient errors a few times; a
 *     final failure is remembered for [[FailureCacheTtl]] so concurrent writes do not all hit the
 *     service again.
 *
 * The raw response is never logged because storage container URLs carry SAS tokens.
 */
object IngestV2ConfigurationProvider {
  private val myName = this.getClass.getSimpleName
  private[v2] val MaxCacheTtl: Duration = Duration.ofHours(1)
  private[v2] val StaleRetryInterval: Duration = Duration.ofMinutes(1)
  private[v2] val FailureCacheTtl: Duration = Duration.ofSeconds(10)
  private[v2] val InitialAttempts = 3
  private[v2] val InitialRetryBackoff: Duration = Duration.ofSeconds(2)
  private val RequestTimeoutSeconds = 30L

  private final case class CacheKey(dmUrl: String, authentication: KustoAuthentication)
  private[v2] final case class Loaded(response: ConfigurationResponse, config: IngestionConfig)
  private final case class CachedEntry(
      value: Option[Loaded],
      error: Option[Throwable],
      refreshAtNanos: Long)

  private val configCache = new ConcurrentHashMap[CacheKey, CachedEntry]()
  private val locks = new ConcurrentHashMap[CacheKey, Object]()
  private val configClients = new ConcurrentHashMap[CacheKey, ConfigurationClient]()

  /** Returns the configuration, or the reason it could not be read. */
  def getConfiguration(
      dmUrl: String,
      authentication: KustoAuthentication): Try[IngestionConfig] = {
    val result = load(dmUrl, authentication).map(_.config)
    IngestV2TestHooks.configOverride.fold(result)(f => result.map(f))
  }

  /** The raw service response, for the SDK clients. */
  private[v2] def getResponse(
      dmUrl: String,
      authentication: KustoAuthentication): Try[ConfigurationResponse] =
    load(dmUrl, authentication).map(_.response)

  private def load(dmUrl: String, authentication: KustoAuthentication): Try[Loaded] = {
    val key = CacheKey(dmUrl, authentication)
    fresh(configCache.get(key)).getOrElse {
      locks.computeIfAbsent(key, _ => new Object).synchronized {
        // Another thread may have refreshed the entry while this one waited.
        val current = configCache.get(key)
        fresh(current).getOrElse(refresh(key, Option(current).flatMap(_.value)))
      }
    }
  }

  private def fresh(entry: CachedEntry): Option[Try[Loaded]] =
    Option(entry).filter(e => System.nanoTime() - e.refreshAtNanos < 0).map { e =>
      e.value match {
        case Some(v) => Success(v)
        case None => Failure(e.error.get)
      }
    }

  private def refresh(key: CacheKey, stale: Option[Loaded]): Try[Loaded] = {
    val attempts = if (stale.isDefined) 1 else InitialAttempts
    val result = withRetries(attempts)(queryConfigurationApi(key.dmUrl, key.authentication))
    (result, stale) match {
      case (Success((loaded, ttl)), _) =>
        configCache.put(key, CachedEntry(Some(loaded), None, deadline(ttl)))
        KDSU.logInfo(myName, s"Ingestion configuration for ${key.dmUrl}: ${loaded.config}")
        Success(loaded)
      case (Failure(e), Some(previous)) =>
        configCache.put(key, CachedEntry(Some(previous), None, deadline(StaleRetryInterval)))
        KDSU.logWarn(
          myName,
          s"Could not refresh the ingestion configuration from ${key.dmUrl}, using the last one " +
            s"read: ${e.getClass.getSimpleName}: ${e.getMessage}")
        Success(previous)
      case (Failure(e), None) =>
        configCache.put(key, CachedEntry(None, Some(e), deadline(FailureCacheTtl)))
        KDSU.logWarn(
          myName,
          s"Could not read the ingestion configuration from ${key.dmUrl}: " +
            s"${e.getClass.getSimpleName}: ${e.getMessage}")
        Failure(e)
    }
  }

  private def deadline(ttl: Duration): Long = System.nanoTime() + ttl.toNanos

  /** Retries errors that may be transient; a timeout already used the whole budget. */
  private[v2] def withRetries[T](attempts: Int, backoff: Duration = InitialRetryBackoff)(
      call: => Try[T]): Try[T] = {
    var attempt = 1
    var result = call
    while (result.isFailure && attempt < attempts && isRetryable(result.failed.get)) {
      Thread.sleep(backoff.toMillis * (1L << (attempt - 1)))
      attempt += 1
      result = call
    }
    result
  }

  private def isRetryable(e: Throwable): Boolean = e match {
    case _: TimeoutException => false
    case _: InterruptedException => false
    case _: IllegalArgumentException => false
    case _ => true
  }

  private def queryConfigurationApi(
      dmUrl: String,
      authentication: KustoAuthentication): Try[(Loaded, Duration)] = Try {
    // The SDK client owns an HTTP client it never releases, so one is kept per key and reused.
    val configClient = configClients.computeIfAbsent(
      CacheKey(dmUrl, authentication),
      _ =>
        new ConfigurationClient(
          dmUrl,
          IngestV2Authentication.createTokenCredential(authentication, dmUrl),
          false, // skipSecurityChecks
          new ClientDetails(IngestV2ClientProvider.ApplicationName, KDSU.clientName, ""),
          null, // s2sTokenProvider
          null // s2sFabricPrivateLinkAccessContext
        ))

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
    (Loaded(response, config), cacheTtl(refresh))
  }

  private[v2] def cacheTtl(refreshInterval: Option[Duration]): Duration =
    refreshInterval match {
      case Some(d) if !d.isNegative && !d.isZero && d.compareTo(MaxCacheTtl) < 0 => d
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

  /** Puts a configuration in the cache (tests). */
  private[v2] def seed(
      dmUrl: String,
      authentication: KustoAuthentication,
      response: ConfigurationResponse,
      config: IngestionConfig): Unit =
    configCache.put(
      CacheKey(dmUrl, authentication),
      CachedEntry(Some(Loaded(response, config)), None, deadline(MaxCacheTtl)))
}

/**
 * The SDK's configuration cache, backed by [[IngestV2ConfigurationProvider]]. The SDK's default
 * cache opens a new HTTP client on every refresh and never closes it, and lets concurrent callers
 * refresh at the same time.
 */
private[v2] final class IngestV2ConfigurationCache(
    dmUrl: String,
    authentication: KustoAuthentication)
    extends ConfigurationCache {

  override def getRefreshInterval: Duration = IngestV2ConfigurationProvider.MaxCacheTtl

  // A suspend function may return its value directly instead of suspending.
  override def getConfiguration(
      continuation: kotlin.coroutines.Continuation[_ >: CachedConfigurationData]): AnyRef =
    new CachedConfigurationData(IngestV2ConfigurationProvider.getResponse(dmUrl, authentication).get)

  override def close(): Unit = {}
}

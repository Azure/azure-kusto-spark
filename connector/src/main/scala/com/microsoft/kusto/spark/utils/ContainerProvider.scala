package com.microsoft.kusto.spark.utils

import com.microsoft.azure.kusto.data.exceptions.KustoDataExceptionBase
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.retry.RetryConfig

import java.time.{Clock, Duration, Instant}
import scala.collection.JavaConverters._

class ContainerProvider[A](val client: ExtendedKustoClient, val clusterAlias: String, val command: String, cacheEntryCreator: ContainerAndSas => A) {
  private var roundRobinIdx = 0
  private var storageUris: Seq[A] = Seq.empty
  private var lastRefresh: Instant = Instant.now(Clock.systemUTC())
  private val className = this.getClass.getSimpleName
  private val maxCommandsRetryAttempts = 8
  private val retryConfig = buildRetryConfig

  private def buildRetryConfig = {
    val sleepConfig = IntervalFunction.ofExponentialRandomBackoff(
      ExtendedKustoClient.BaseIntervalMs, IntervalFunction.DEFAULT_MULTIPLIER,
      IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR, ExtendedKustoClient.MaxRetryIntervalMs)
    RetryConfig.custom
      .maxAttempts(maxCommandsRetryAttempts)
      .intervalFunction(sleepConfig)
      .retryOnException((e: Throwable) =>
        e.isInstanceOf[IngestionServiceException] && !e.asInstanceOf[KustoDataExceptionBase].isPermanent).build
  }

  def getContainer: A = {
    // Refresh if storageExpiryMinutes have passed since last refresh for this cluster as SAS should be valid for at least 120 minutes
    if (storageUris.isEmpty ||
      Duration.between(lastRefresh,Instant.now(Clock.systemUTC())).toMinutesPart > KustoConstants.StorageExpiryMinutes) {
      refresh
    } else {
      roundRobinIdx = (roundRobinIdx + 1) % storageUris.size
      storageUris(roundRobinIdx)
    }
  }

  def getAllContainers: Seq[A] = {
    if (storageUris.isEmpty ||
      Duration.between(lastRefresh,Instant.now(Clock.systemUTC())).toMinutesPart >
        KustoConstants
        .StorageExpiryMinutes){
      refresh
    }
    storageUris
  }

  private def refresh = {
      val res = client.executeDM(command, null, Some(retryConfig))
      val storage = res.getPrimaryResults.getData.asScala.map(row => {
        val parts = row.get(0).toString.split('?')
        cacheEntryCreator(ContainerAndSas(parts(0), s"?${parts(1)}"))
      })

      if (storage.isEmpty) {
        KDSU.reportExceptionAndThrow(className, new RuntimeException("Failed to allocate temporary storage"), "writing to Kusto", clusterAlias)
      }

      KDSU.logInfo(className, s"Got ${storage.length} storage SAS with command :'$command'. from service 'ingest-$clusterAlias'")
      lastRefresh = Instant.now(Clock.systemUTC())
      storageUris = scala.util.Random.shuffle(storage)
      roundRobinIdx = 0
      storage(roundRobinIdx)
  }
}

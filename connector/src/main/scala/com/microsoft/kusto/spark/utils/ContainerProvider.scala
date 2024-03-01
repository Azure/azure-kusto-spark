//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark.utils

import com.microsoft.azure.kusto.data.exceptions.{DataServiceException, KustoDataExceptionBase}
import com.microsoft.azure.kusto.ingest.exceptions.{
  IngestionClientException,
  IngestionServiceException
}
import com.microsoft.kusto.spark.exceptions.NoStorageContainersException
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.retry.{Retry, RetryConfig}
import io.vavr.CheckedFunction0
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.http.conn.HttpHostConnectException

import java.time.{Clock, Instant}
import java.util.function.Predicate
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class ContainerProvider(
    val client: ExtendedKustoClient,
    val clusterAlias: String,
    val command: String,
    cacheExpirySeconds: Int = KustoConstants.StorageExpirySeconds) { // Refactored for tests with short cache
  private var roundRobinIdx = 0
  private var storageUris: Seq[ContainerAndSas] = Seq.empty
  private var lastRefresh: Instant = Instant.now(Clock.systemUTC())
  private val className = this.getClass.getSimpleName
  private val maxCommandsRetryAttempts = 8
  private val retryConfigExportContainers = buildRetryConfig((e: Throwable) =>
    (e.isInstanceOf[IngestionServiceException] && !e
      .asInstanceOf[KustoDataExceptionBase]
      .isPermanent) ||
      (e.isInstanceOf[DataServiceException] && ExceptionUtils
        .getRootCause(e)
        .isInstanceOf[HttpHostConnectException]))
  private val retryConfigIngestionRefresh = buildRetryConfig((e: Throwable) =>
    (e.isInstanceOf[NoStorageContainersException]
      || e.isInstanceOf[IngestionClientException] || e.isInstanceOf[IngestionServiceException]))

  private def buildRetryConfig(retryException: Predicate[Throwable]) = {
    val sleepConfig = IntervalFunction.ofExponentialRandomBackoff(
      ExtendedKustoClient.BaseIntervalMs,
      IntervalFunction.DEFAULT_MULTIPLIER,
      IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR,
      ExtendedKustoClient.MaxRetryIntervalMs)
    RetryConfig.custom
      // TODO the only difference between this and the one in ExtendedKustoClient is the maxAttempts. Should we refactor ?
      .maxAttempts(maxCommandsRetryAttempts)
      .intervalFunction(sleepConfig)
      .retryOnException(retryException)
      .build
  }

  def getContainer: ContainerAndSas = {
    // Refresh if storageExpiryMinutes have passed since last refresh for this cluster as SAS should be valid for at least 120 minutes
    val now = Instant.now(Clock.systemUTC())
    val secondsElapsed =
      now.getEpochSecond - lastRefresh.getEpochSecond // get the seconds between now and last refresh
    if (storageUris.isEmpty ||
      secondsElapsed > cacheExpirySeconds /* If the cache has elapsed , refresh */ ) {
      refresh()
    } else {
      roundRobinIdx = (roundRobinIdx + 1) % storageUris.size
      storageUris(roundRobinIdx)
    }
  }

  def getExportContainers: Seq[ContainerAndSas] = {
    val now = Instant.now(Clock.systemUTC())
    val secondsElapsed =
      now.getEpochSecond - lastRefresh.getEpochSecond // get the seconds between now and last refresh
    if (storageUris.isEmpty || secondsElapsed > cacheExpirySeconds) {
      refresh(true)
    }
    storageUris
  }

  private def refresh(exportContainer: Boolean = false): ContainerAndSas = {
    if (exportContainer) {
      Try(client.executeDM(command, None, Some(retryConfigExportContainers))) match {
        case Success(res) =>
          val storage = res.getPrimaryResults.getData.asScala.map(row => {
            val parts = row.get(0).toString.split('?')
            ContainerAndSas(parts(0), s"?${parts(1)}")
          })
          processContainerResults(storage)
        case Failure(exception) =>
          KDSU.reportExceptionAndThrow(
            className,
            exception,
            "Error querying for create export containers",
            clusterAlias,
            shouldNotThrow = storageUris.nonEmpty)
          storageUris(roundRobinIdx)
      }
    } else {
      val retryExecute: CheckedFunction0[ContainerAndSas] = Retry.decorateCheckedSupplier(
        Retry.of("refresh ingestion resources", retryConfigIngestionRefresh),
        () => {
          Try(client.ingestClient.getResourceManager.getShuffledContainers) match {
            case Success(res) =>
              val storage = res.asScala.map(row => {
                ContainerAndSas(row.getContainer.getBlobContainerUrl, s"${row.getSas}")
              })
              processContainerResults(storage)
            case Failure(exception) =>
              KDSU.reportExceptionAndThrow(
                className,
                exception,
                "Error querying for create tempstorage",
                clusterAlias,
                shouldNotThrow = storageUris.nonEmpty)
              storageUris(roundRobinIdx)
          }
        })
      retryExecute.apply()
    }
  }

  private def processContainerResults(
      storage: mutable.Buffer[ContainerAndSas]): ContainerAndSas = {
    if (storage.isEmpty) {
      KDSU.reportExceptionAndThrow(
        className,
        NoStorageContainersException(
          "No storage containers received. Failed to allocate temporary storage"),
        "writing to Kusto",
        clusterAlias)
    }
    KDSU.logInfo(
      className,
      s"Got ${storage.length} storage SAS with command :'$command'. from service 'ingest-$clusterAlias'")
    lastRefresh = Instant.now(Clock.systemUTC())
    storageUris = scala.util.Random.shuffle(storage)
    roundRobinIdx = 0
    storage(roundRobinIdx)
  }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.azure.identity.{DefaultAzureCredentialBuilder, ManagedIdentityCredentialBuilder}
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.sas.{BlobContainerSasPermission, BlobServiceSasSignatureValues}
import com.microsoft.azure.kusto.data.StringUtils
import com.microsoft.azure.kusto.data.exceptions.{DataServiceException, KustoDataExceptionBase}
import com.microsoft.azure.kusto.ingest.exceptions.{
  IngestionClientException,
  IngestionServiceException
}
import com.microsoft.kusto.spark.datasink.IngestionStorageParameters
import com.microsoft.kusto.spark.exceptions.NoStorageContainersException
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.retry.{Retry, RetryConfig}
import io.vavr.CheckedFunction0
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.http.conn.HttpHostConnectException

import java.time.{Clock, Instant, OffsetDateTime}
import java.util.concurrent.ConcurrentHashMap
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

  def getContainer(maybeIngestionStorageParams: Option[Array[IngestionStorageParameters]] = None)
      : ContainerAndSas = {
    // Refresh if storageExpiryMinutes have passed since last refresh for this cluster as SAS should be valid for at least 120 minutes
    val now = Instant.now(Clock.systemUTC())
    val secondsElapsed =
      now.getEpochSecond - lastRefresh.getEpochSecond // get the seconds between now and last refresh
    val isCacheExpired = secondsElapsed > cacheExpirySeconds
    maybeIngestionStorageParams match {
      case Some(ingestionStorageParams) =>
        val (isKeyRefreshed, containerWithSas) = refreshUserSas(
          ingestionStorageParams,
          isCacheExpired = isCacheExpired,
          cacheExpirySeconds)
        /*
        Only if the key was refreshed, we need to reset the last refresh time.
        Using process container results may have an issue if for some reason we run into a case
        where the DM container and the user provided container is provided.
         */
        if (isKeyRefreshed) {
          lastRefresh = Instant.now(Clock.systemUTC())
        }
        containerWithSas

      case None =>
        if (storageUris.isEmpty ||
          isCacheExpired /* If the cache has elapsed , refresh */ ) {
          refresh()
        } else {
          roundRobinIdx = (roundRobinIdx + 1) % storageUris.size
          storageUris(roundRobinIdx)
        }
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
      Try(
        client.executeDM(
          command,
          None,
          "refreshContainers",
          Some(retryConfigExportContainers))) match {
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
                ContainerAndSas(row.getAsyncContainer.getBlobContainerUrl, s"${row.getSas}")
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

  private val sasKeyCacheMap = new ConcurrentHashMap[String, ContainerAndSas]()
  def refreshUserSas(
      ingestionStorageParams: Array[IngestionStorageParameters],
      isCacheExpired: Boolean,
      cacheExpirySeconds: Long,
      listPermissions: Boolean = false): (Boolean, ContainerAndSas) = {

    val ingestionStorageParameter =
      IngestionStorageParameters.getRandomIngestionStorage(ingestionStorageParams)

    val key = ingestionStorageParameter.toString

    // If the cache has not expired and the key is already in the cache, return the cached value
    KDSU.logDebug("ContainerProvider", s" Checking cache for Key: $key")
    if (!isCacheExpired && sasKeyCacheMap.containsKey(key)) {
      // we have populated this into the cache. It is already normalized
      KDSU.logInfo(className, "Using SAS token from ingestion storage from cache")
      val normalizedSas = sasKeyCacheMap.get(key).sas
      (
        false,
        ContainerAndSas(
          s"${ingestionStorageParameter.storageUrl}/${ingestionStorageParameter.containerName}",
          s"$normalizedSas"))
    } else {
      if (!StringUtils.isEmpty(ingestionStorageParameter.sas)) {
        KDSU.logInfo(className, "Using SAS token from ingestion storage parameter")
        val normalizedSas = ingestionStorageParameter.sas
        (
          false,
          ContainerAndSas(
            s"${ingestionStorageParameter.storageUrl}/${ingestionStorageParameter.containerName}",
            s"$normalizedSas"))
      } else {
        KDSU.logInfo(
          className,
          s"Using user supplied ingestion storage $ingestionStorageParameter.Expires at " +
            s"${OffsetDateTime.now.plusSeconds(cacheExpirySeconds)}")
        val sasToken: String = normalizeSasKey(
          generateSasKey(cacheExpirySeconds, listPermissions, ingestionStorageParameter))
        // Cache the SAS token for future use
        val containerAndSas = ContainerAndSas(
          s"${ingestionStorageParameter.storageUrl}/${ingestionStorageParameter.containerName}",
          s"$sasToken")
        sasKeyCacheMap.put(key, containerAndSas)
        KDSU.logInfo("ContainerProvider", s"Created SAS for Key: $key and stored in cache")
        (true, containerAndSas)
      }
    }
  }

  def generateSasKey(
      cacheExpirySeconds: Long,
      listPermissions: Boolean,
      ingestionStorageParameter: IngestionStorageParameters): String = {
    ContainerProvider.getUserDelegatedSas(
      cacheExpirySeconds,
      listPermissions,
      ingestionStorageParameter)
  }

  private def normalizeSasKey(sasValue: String) = {
    if (sasValue.startsWith("?")) {
      sasValue
    } else {
      s"?$sasValue"
    }
  }
}

object ContainerProvider {
  private val className = this.getClass.getSimpleName
  protected[kusto] def getUserDelegatedSas(
      cacheExpirySeconds: Long,
      listPermissions: Boolean,
      ingestionStorageParameter: IngestionStorageParameters): String = {
    val credential = if (!StringUtils.isEmpty(ingestionStorageParameter.userMsi)) {
      new ManagedIdentityCredentialBuilder()
        .clientId(ingestionStorageParameter.userMsi)
        .build()
    } else {
      // Use the default credential chain to authenticate
      KDSU.logWarn(
        className,
        "Using default credential chain to authenticate to blob storage. " +
          "This may not work if the environment is not set up correctly.")
      new DefaultAzureCredentialBuilder().build()
    }

    // Create a SAS token that's valid for 8 hours
    val startTime = OffsetDateTime.now.minusMinutes(5)

    val expiryTime = OffsetDateTime.now.plusSeconds(cacheExpirySeconds * 4) // Just to be sure
    // Assign read/write permissions to the SAS token
    val sasPermission =
      new BlobContainerSasPermission().setWritePermission(true).setReadPermission(true)

    if (listPermissions) {
      sasPermission.setListPermission(true)
    }
    val sasSignatureValues = new BlobServiceSasSignatureValues(expiryTime, sasPermission)
      .setStartTime(startTime)

    val blobServiceClient = new BlobServiceClientBuilder()
      .endpoint(ingestionStorageParameter.storageUrl)
      .credential(credential)
      .buildClient
    val containerClient =
      blobServiceClient.getBlobContainerClient(ingestionStorageParameter.containerName)
    val userDelegationKey = blobServiceClient.getUserDelegationKey(startTime, expiryTime)
    val sasToken = containerClient
      .generateUserDelegationSas(sasSignatureValues, userDelegationKey)
    sasToken

  }
}

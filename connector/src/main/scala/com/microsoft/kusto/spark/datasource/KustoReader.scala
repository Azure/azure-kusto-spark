// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import com.azure.core.credential.AzureSasCredential
import com.azure.storage.blob.BlobContainerClientBuilder
import com.azure.storage.common.StorageSharedKeyCredential
import com.microsoft.azure.kusto.data.{ClientRequestProperties, KustoResultSetTable}
import com.microsoft.azure.kusto.data.exceptions.ThrottleException
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasource.ReadMode.ReadMode
import com.microsoft.kusto.spark.exceptions.{
  FailedOperationException,
  TimeoutAwaitingPendingOperationException
}
import com.microsoft.kusto.spark.utils.{
  CslCommandsGenerator,
  ExtendedKustoClient,
  KustoAzureFsSetupCache,
  KustoConstants => KCONST,
  KustoDataSourceUtils => KDSU
}
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.retry.RetryConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.ComparableVersion
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, RuntimeConfig, SparkSession}

import java.net.URI
import java.security.InvalidParameterException
import java.time.{Clock, Instant}
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.{concurrent, mutable}
import scala.concurrent.duration._
import scala.util.Try

private[kusto] case class KustoPartition(predicate: Option[String], idx: Int) extends Partition {
  override def index: Int = idx
}

private[kusto] case class KustoFiltering(
    columns: Array[String] = Array.empty,
    filters: Array[Filter] = Array.empty)

private[kusto] case class KustoReadRequest(
    sparkSession: SparkSession,
    schema: KustoSchema,
    kustoCoordinates: KustoCoordinates,
    query: String,
    authentication: KustoAuthentication,
    timeout: FiniteDuration,
    clientRequestProperties: Option[ClientRequestProperties],
    requestId: String)

private[kusto] case class KustoReadOptions(
    readMode: Option[ReadMode] = None,
    partitionOptions: PartitionOptions,
    distributedReadModeTransientCacheEnabled: Boolean = false,
    queryFilterPushDown: Option[Boolean],
    additionalExportOptions: Map[String, String] = Map.empty,
    storageProtocol: Option[String] = None,
    enableExportStorageApi: Boolean = false)

private[kusto] case class PartitionOptions(
    amount: Int,
    var column: Option[String],
    var mode: Option[String])

private[kusto] case class DistributedReadModeTransientCacheKey(
    query: String,
    kustoCoordinates: KustoCoordinates,
    authentication: KustoAuthentication,
    enableExportStorageApi: Boolean)

/**
 * A previously exported result set: the parquet paths together with the storage credentials and
 * protocol they must be read with. The credentials are needed on every cache hit because the
 * Hadoop/Spark configuration they install is session global and may have been overwritten by
 * another read in the meantime.
 */
private[kusto] case class DistributedReadModeTransientCacheEntry(
    paths: Seq[String],
    storage: TransientStorageParameters,
    storageProtocol: String)

object KustoReader {
  private val className = this.getClass.getSimpleName
  private val sparkHadoopConfigPrefix = "spark.hadoop."
  private val exportCapacityOrigin =
    """(?:^|,\s*)Origin:\s*'CapacityPolicy/Export'(?:[.,\s]|$)""".r
  private[kusto] val MaxExportAttempts = 4
  private val exportRetryInterval = IntervalFunction.ofExponentialRandomBackoff(
    5000L,
    2.0,
    IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR,
    30000L)
  // A lost submission response is not evidence that the export was rejected.
  private val exportSubmissionRetryConfig = RetryConfig.custom.maxAttempts(1).build()

  private def operationField(exception: FailedOperationException, name: String): Option[String] =
    exception.getResult.flatMap(table => Try(table.getString(name)).toOption.flatMap(Option(_)))

  private def withExportReadContext(
      exception: FailedOperationException,
      requestId: String,
      retrySummary: String = ""): FailedOperationException = {
    val result = exception.getResult
    // Read the current operation row without advancing the cursor or masking the original error.
    def field(name: String): Option[String] = operationField(exception, name)

    val throttled = field("State").contains("Throttled")
    val exportCapacityExceeded = throttled && field("Status").exists(status =>
      exportCapacityOrigin.findFirstIn(status).isDefined)
    val reason = if (exportCapacityExceeded) {
      "Kusto Spark read failed because the concurrent export capacity was exceeded."
    } else if (throttled) {
      "Kusto Spark read failed because its distributed export was throttled."
    } else {
      "Kusto Spark read failed during a distributed export."
    }
    val guidance = if (exportCapacityExceeded) {
      " Reduce concurrent reads or exports against the same Kusto cluster or Eventhouse and retry after backoff." +
        " If throttling persists, ask the service administrator to review export capacity."
    } else if (throttled) {
      " Retry the read after backoff. If throttling persists, ask the service administrator to review the applicable limit."
    } else {
      ""
    }
    val enriched = new FailedOperationException(
      s"$reason Distributed reads export query results to temporary storage before Spark reads them." +
        guidance + (if (retrySummary.isEmpty) "" else s" $retrySummary") +
        s" Connector requestId: $requestId. Original error: ${exception.getMessage}",
      result)
    enriched.initCause(exception)
    enriched
  }

  /**
   * Sets a Hadoop configuration key on both the Hadoop Configuration object and the Spark session
   * RuntimeConfig (with spark.hadoop. prefix). This ensures the configuration is propagated to
   * all engines including Gluten/Velox which read from Spark session conf.
   */
  private def setHadoopConf(
      config: Configuration,
      sparkConf: RuntimeConfig,
      key: String,
      value: String): Unit = {
    config.set(key, value)
    sparkConf.set(s"$sparkHadoopConfigPrefix$key", value)
  }

  /**
   * Sets a Hadoop configuration key only when nothing is configured for it yet, so that
   * session-wide settings owned by the hosting runtime are never overwritten by the connector.
   * Only the Hadoop Configuration is probed and written here on purpose: this is used for
   * filesystem implementation keys, which are resolved exclusively from the Hadoop Configuration
   * (spark.hadoop.* is copied into it at session start, never read back), so mirroring an
   * inherited value into the session conf would only pin a runtime owned implementation.
   */
  private def setHadoopConfIfAbsent(
      config: Configuration,
      sparkConf: RuntimeConfig,
      key: String,
      value: String): Unit = {
    if (config.get(key) == null) {
      setHadoopConf(config, sparkConf, key, value)
    } else {
      KDSU.logDebug(className, s"Hadoop config '$key' already set, leaving it untouched")
    }
  }

  private val distributedReadModeTransientCache: concurrent.Map[
    DistributedReadModeTransientCacheKey,
    DistributedReadModeTransientCacheEntry] =
    new concurrent.TrieMap()
  /*
  A new native implementation of Parquet writer that uses new encoding schemes was rolled out on the ADX side. This uses delta byte array for strings and other byte array-based Parquet types (default in Parquet V2 which most modern parquet readers support by default).
  To avoid breaking changes for applications, if the runtime is on a lower version than 3.3.0 of spark runtime we explicitly set the ADX export to not use the useNativeIngestion
  TODO - add test
   */
  private val minimalParquetWriterVersion = "3.3.0"
  private[kusto] def singleBuildScan(
      kustoClient: ExtendedKustoClient,
      request: KustoReadRequest,
      filtering: KustoFiltering): RDD[Row] = {

    KDSU.logInfo(className, s"Executing query in Single mode. requestId: ${request.requestId}")
    val filteredQuery = KustoFilter.pruneAndFilter(
      KustoSchema(request.schema.sparkSchema, Set()),
      request.query,
      filtering)
    val kustoResult: KustoResultSetTable = kustoClient
      .executeEngine(
        request.kustoCoordinates.database,
        filteredQuery,
        "executeQuery",
        request.clientRequestProperties.orNull,
        isMgmtCommand = false)
      .getPrimaryResults

    val serializer = KustoResponseDeserializer(kustoResult)
    request.sparkSession.createDataFrame(serializer.toRows, serializer.getSchema.sparkSchema).rdd
  }

  private def determineFilterPushDown(
      queryFilterPushDown: Option[Boolean],
      queryFilterPushDownDefault: Boolean,
      inputFilter: KustoFiltering): KustoFiltering = {
    if (queryFilterPushDown.getOrElse(queryFilterPushDownDefault)) {
      KDSU.logInfo(className, s"using ${KustoSourceOptions.KUSTO_QUERY_FILTER_PUSH_DOWN}")
      inputFilter
    } else {
      KDSU.logInfo(className, s"not using ${KustoSourceOptions.KUSTO_QUERY_FILTER_PUSH_DOWN}")
      KustoFiltering()
    }
  }

  private[kusto] def distributedBuildScan(
      kustoClient: ExtendedKustoClient,
      request: KustoReadRequest,
      storage: TransientStorageParameters,
      options: KustoReadOptions,
      filtering: KustoFiltering): RDD[Row] = {
    var paths: Seq[String] = Seq()
    // if distributedReadModeTransientCacheEnabled is set to true, then check if path is cached and use it
    // if not export and cache the path for reuse
    if (options.distributedReadModeTransientCacheEnabled) {
      val key = DistributedReadModeTransientCacheKey(
        request.query,
        request.kustoCoordinates,
        request.authentication,
        options.enableExportStorageApi)
      if (distributedReadModeTransientCache.contains(key)) {
        KDSU.logInfo(
          className,
          "Fetching from distributedReadModeTransientCache: hit, reusing cached export paths")
        val cached = distributedReadModeTransientCache(key)
        // ABFS resolves credentials per storage account, so another read of the same account,
        // another Spark session or a fresh Hadoop Configuration may have left the account
        // scoped keys holding a different token since this entry was created - reinstall them.
        // WASBS keys are container scoped and were never clobbered, so nothing is done there,
        // exactly as before.
        if (isAbfsProtocol(cached.storageProtocol)) {
          setupBlobAccess(request, cached.storage, cached.storageProtocol, forceRefresh = true)
        }
        paths = cached.paths
      } else {
        KDSU.logInfo(
          className,
          "distributedReadModeTransientCache: miss, exporting to cache paths")
        val filter = determineFilterPushDown(
          options.queryFilterPushDown,
          queryFilterPushDownDefault = false,
          filtering)
        val exported = exportToStorage(kustoClient, request, storage, options, filter)
        paths = exported.paths
        distributedReadModeTransientCache(key) = exported
      }
    } else {
      val filter = determineFilterPushDown(
        options.queryFilterPushDown,
        queryFilterPushDownDefault = true,
        filtering)
      paths = exportToStorage(kustoClient, request, storage, options, filter).paths
    }

    val rdd =
      try {
        request.sparkSession.read.parquet(paths: _*).rdd
      } catch {
        case ex: Exception =>
          // Check whether the result is empty, causing an IO exception on reading empty parquet file
          // We don't mind generating the filtered query again - it only happens upon exception
          val filteredQuery = KustoFilter.pruneAndFilter(request.schema, request.query, filtering)
          val count = KDSU.countRows(
            kustoClient.engineClient,
            filteredQuery,
            request.kustoCoordinates.database,
            request.clientRequestProperties.orNull)

          if (count == 0) {
            request.sparkSession.emptyDataFrame.rdd
          } else {
            throw ex
          }
      }

    KDSU.logInfo(className, "Transaction data read from blob storage, paths:" + paths)
    rdd
  }

  private def dirExist(
      spark: SparkSession,
      params: TransientStorageCredentials,
      directory: String,
      endpointSuffix: String,
      storageProtocol: String = KCONST.storageProtocolWasbs): Boolean = {
    if (params.isOneLake) {
      // OneLake: probe via Hadoop FileSystem over abfss using ambient AAD (Fabric Spark).
      val base = params.oneLakeAbfssBase
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(new URI(base), hadoopConf)
      val path = new Path(s"$base/$directory")
      fs.exists(path)
    } else if (params.authMethod == AuthMethod.Impersonation) {
      val url =
        s"$storageProtocol://${params.blobContainer}@${params.storageAccountName}.blob.$endpointSuffix"
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(new URI(url), hadoopConf)

      val path = new Path(url + s"/$directory")
      fs.exists(path)
    } else {
      val endpoint = s"https://${params.storageAccountName}.blob.$endpointSuffix"
      val container = params.authMethod match {
        case AuthMethod.Sas =>
          val sas = if (params.sasKey.startsWith("?")) params.sasKey else s"?${params.sasKey}"
          new BlobContainerClientBuilder()
            .endpoint(endpoint)
            .containerName(params.blobContainer)
            .credential(new AzureSasCredential(sas))
            .buildClient()
        case AuthMethod.Key =>
          new BlobContainerClientBuilder()
            .endpoint(endpoint)
            .containerName(params.blobContainer)
            .credential(
              new StorageSharedKeyCredential(params.storageAccountName, params.storageAccountKey))
            .buildClient()
        case _ => throw new InvalidParameterException("")
      }
      val exists = container.listBlobsByHierarchy(directory).stream().count() > 0
      // Existing logic container.exists() && container.getDirectoryReference(directory).listBlobsSegmented().getLength > 0
      exists
    }
  }

  private def exportToStorage(
      kustoClient: ExtendedKustoClient,
      request: KustoReadRequest,
      storage: TransientStorageParameters,
      options: KustoReadOptions,
      filtering: KustoFiltering): DistributedReadModeTransientCacheEntry = {

    KDSU.logInfo(
      className,
      s"Starting exporting data from Kusto to blob storage in Distributed mode. requestId: ${request.requestId}")

    // OneLake transient storage always reads back over abfss; force the protocol when any
    // credential is a OneLake URL so the rest of the read path uses the correct scheme.
    val anyOneLake = storage.storageCredentials.exists(_.isOneLake)
    val protocol =
      if (anyOneLake) KCONST.storageProtocolAbfss
      else options.storageProtocol.getOrElse(KCONST.storageProtocolWasbs)
    // ABFS resolves SAS tokens per storage account (never per container), so two containers
    // of the same account carrying different SAS tokens cannot both be configured. Keep a
    // single container per account in that case to avoid reading with the wrong token.
    val exportStorage =
      if (isAbfsProtocol(protocol)) dedupeConflictingCredentials(storage) else storage
    setupBlobAccess(request, exportStorage, protocol)
    val partitions = calculatePartitions(options.partitionOptions)
    val reader = new KustoReader(kustoClient)
    val directory = s"${request.kustoCoordinates.database}/dir${UUID.randomUUID()}/"
      .replaceAll("[^0-9a-zA-Z/]", "_")

    val directories = reader.exportPartitionsToBlob(
      partitions.toSeq.map(_.asInstanceOf[KustoPartition]),
      request,
      exportStorage,
      directory,
      options,
      filtering)
    val paths = exportedPaths(
      exportStorage,
      directories,
      protocol,
      (params, exportedDirectory) =>
        dirExist(
          request.sparkSession,
          params,
          exportedDirectory,
          exportStorage.endpointSuffix,
          protocol))
    KDSU.logInfo(
      className,
      s"Finished exporting from Kusto to ${paths.mkString(",")}" +
        s", on requestId: ${request.requestId}, will start parquet reading now")
    DistributedReadModeTransientCacheEntry(paths, exportStorage, protocol)
  }

  private[kusto] def exportedPaths(
      storage: TransientStorageParameters,
      directories: Seq[String],
      protocol: String,
      directoryExists: (TransientStorageCredentials, String) => Boolean): Seq[String] = {
    // Never read the parent directory: aborted attempts can leave partial Parquet files there.
    directories.flatMap { directory =>
      storage.storageCredentials.iterator
        .filter(params => directoryExists(params, directory))
        .map(params =>
          if (params.isOneLake) {
            s"${params.oneLakeAbfssBase}/$directory"
          } else {
            s"$protocol://${params.blobContainer}" +
              s"@${params.storageAccountName}.blob.${storage.endpointSuffix}/$directory"
          })
        .toSeq
    }
  }

  private[kusto] def setupBlobAccess(
      request: KustoReadRequest,
      storageParameters: TransientStorageParameters,
      storageProtocol: String = KCONST.storageProtocolWasbs,
      forceRefresh: Boolean = false): Unit = {
    val config = request.sparkSession.sparkContext.hadoopConfiguration
    val sparkConf = request.sparkSession.conf
    val now = Instant.now(Clock.systemUTC())
    val useAbfs = isAbfsProtocol(storageProtocol)

    setHadoopAuth(
      storageParameters,
      storageProtocol,
      config,
      sparkConf,
      now,
      useAbfs,
      forceRefresh)

    if (!KustoAzureFsSetupCache.updateAndGetPrevNativeAzureFs(now)) {
      if (useAbfs) {
        // Hadoop already ships defaults for both schemes (core-default.xml) and runtimes such
        // as Fabric may install their own wrapper - only fill in when nothing is configured so
        // that the connector never clobbers a session-wide filesystem implementation.
        setHadoopConfIfAbsent(
          config,
          sparkConf,
          "fs.abfs.impl",
          "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem")
        setHadoopConfIfAbsent(
          config,
          sparkConf,
          "fs.abfss.impl",
          "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem")
      } else {
        // WASBS uses NativeAzureFileSystem
        setHadoopConf(
          config,
          sparkConf,
          "fs.azure",
          "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      }
    }
  }

  /**
   * Kept as an overload rather than a defaulted parameter so that the pre-existing six argument
   * signature stays binary compatible for anything already compiled against the connector.
   */
  def setHadoopAuth(
      storageParameters: TransientStorageParameters,
      storageProtocol: String,
      config: Configuration,
      sparkConf: RuntimeConfig,
      now: Instant,
      useAbfs: Boolean): Unit =
    setHadoopAuth(
      storageParameters,
      storageProtocol,
      config,
      sparkConf,
      now,
      useAbfs,
      forceRefresh = false)

  def setHadoopAuth(
      storageParameters: TransientStorageParameters,
      storageProtocol: String,
      config: Configuration,
      sparkConf: RuntimeConfig,
      now: Instant,
      useAbfs: Boolean,
      forceRefresh: Boolean): Unit = {
    // Ensure storage endpoint domain is in the valid ABFS endpoints list
    if (useAbfs) {
      whitelistExportContainers(storageParameters, config, sparkConf)
    }
    for (storage <- storageParameters.storageCredentials) {
      storage.authMethod match {
        case AuthMethod.Key =>
          handleAccountKeyAuth(
            storage,
            storageParameters,
            config,
            sparkConf,
            now,
            useAbfs,
            storageProtocol,
            forceRefresh)
        case AuthMethod.Sas =>
          handleSasAuth(storage, storageParameters, config, sparkConf, now, useAbfs, forceRefresh)
        case _ =>
        // Impersonation (including OneLake) relies on ambient AAD configured by the
        // Spark runtime (e.g. Fabric notebooks). Nothing to install here.
      }
    }
  }

  private def handleAccountKeyAuth(
      storage: TransientStorageCredentials,
      storageParameters: TransientStorageParameters,
      config: Configuration,
      sparkConf: RuntimeConfig,
      now: Instant,
      useAbfs: Boolean,
      storageProtocol: String,
      forceRefresh: Boolean): Unit = {
    val wasCached = !forceRefresh && KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(
      storage.storageAccountName,
      storage.storageAccountKey,
      now)

    if (!wasCached) {
      if (useAbfs) {
        throw new InvalidParameterException(
          s"Storage protocol '$storageProtocol' with Account Key authentication is not supported yet. " +
            "Please use SAS based authentication or switch to 'wasbs' protocol.")
      } else {
        setHadoopConf(
          config,
          sparkConf,
          s"fs.azure.account.key.${storage.storageAccountName}.blob.${storageParameters.endpointSuffix}",
          storage.storageAccountKey)
      }
    }
  }

  private def handleSasAuth(
      storage: TransientStorageCredentials,
      storageParameters: TransientStorageParameters,
      config: Configuration,
      sparkConf: RuntimeConfig,
      now: Instant,
      useAbfs: Boolean,
      forceRefresh: Boolean): Unit = {

    val accountHost = storageAccountHost(storage, storageParameters)
    // ABFS (Hadoop's AbfsConfiguration and the Gluten/Velox native reader alike) only resolves
    // account-scoped keys - 'key.<account>.blob.<suffix>'. A container qualified key is never
    // looked up, so the token must be installed under the account key.
    val sasConfigKey = if (useAbfs) {
      s"fs.azure.sas.fixed.token.$accountHost"
    } else {
      s"fs.azure.sas.${storage.blobContainer}.$accountHost"
    }

    KDSU.logInfo(
      className,
      s"Setting up SAS auth for ${storage.storageAccountName}/${storage.blobContainer}, key: $sasConfigKey")

    // The cache must mirror what is actually installed, but ABFS keys are account scoped: a
    // sibling container of the same account, a second Spark session or a fresh Hadoop
    // Configuration can all leave the account key holding a different token while the
    // per-container cache entry still reports a hit. Config writes are cheap, so always
    // (re)install them for ABFS and keep the cache only for the WASBS keys, which are
    // container scoped and therefore never clobber each other.
    val wasCached = !forceRefresh && !useAbfs && KustoAzureFsSetupCache.updateAndGetPrevSas(
      storage.blobContainer,
      storage.storageAccountName,
      storage.sasKey,
      now)

    if (!wasCached) {
      if (useAbfs) {
        setAbfsSasConfig(storage, accountHost, config, sparkConf, sasConfigKey)
      } else {
        setWasbsSasConfig(storage, config, sparkConf, sasConfigKey)
      }
    } else {
      KDSU.logInfo(className, s"SAS config cached for ${storage.storageAccountName}, skipping")
    }

    verifySasConfig(sasConfigKey, sparkConf, config, useAbfs)
  }

  private def setAbfsSasConfig(
      storage: TransientStorageCredentials,
      accountHost: String,
      config: Configuration,
      sparkConf: RuntimeConfig,
      sasConfigKey: String): Unit = {
    // Scope the auth type to this storage account. Setting the account-agnostic
    // 'fs.azure.account.auth.type' forces *every* other ABFS account used by the session
    // (OneLake / lakehouse / customer storage) into SAS auth, which then fails with
    // "At least one of fs.azure.sas.token.provider.type and fs.azure.sas.fixed.token must be set".
    setHadoopConf(config, sparkConf, s"fs.azure.account.auth.type.$accountHost", "SAS")
    setHadoopConf(config, sparkConf, s"fs.azure.account.hns.enabled.$accountHost", "false")
    setHadoopConf(config, sparkConf, sasConfigKey, normalizeSasToken(storage.sasKey))
    // Back-compat: keep publishing the container qualified key that older connector versions
    // wrote, for runtimes that were adapted to look it up.
    setHadoopConf(
      config,
      sparkConf,
      s"fs.azure.sas.fixed.token.${storage.blobContainer}.$accountHost",
      normalizeSasToken(storage.sasKey))
    KDSU.logInfo(className, s"Set ABFS SAS config: $sasConfigKey")
  }

  private def setWasbsSasConfig(
      storage: TransientStorageCredentials,
      config: Configuration,
      sparkConf: RuntimeConfig,
      sasConfigKey: String): Unit = {
    // Remove leading '?' from SAS token if present, as WASBS expects token without it
    setHadoopConf(config, sparkConf, sasConfigKey, normalizeSasToken(storage.sasKey))
    KDSU.logInfo(className, s"Set WASBS SAS config: $sasConfigKey")
  }

  /**
   * Strip a leading '?' from a SAS token. Hadoop's ABFS client tolerates it but the Gluten /
   * Velox native reader concatenates the token onto the URL as "<url>?<token>", producing an
   * unusable "??sv=..." query and a 401 from storage. WASB never accepts the prefix either.
   */
  private[kusto] def normalizeSasToken(sasKey: String): String = {
    if (sasKey != null && sasKey.startsWith("?")) sasKey.substring(1) else sasKey
  }

  private[kusto] def storageAccountHost(
      storage: TransientStorageCredentials,
      storageParameters: TransientStorageParameters): String =
    s"${storage.storageAccountName}.blob.${storageParameters.endpointSuffix}"

  private[kusto] def isAbfsProtocol(storageProtocol: String): Boolean =
    KCONST.storageProtocolAbfs.equalsIgnoreCase(storageProtocol) ||
      KCONST.storageProtocolAbfss.equalsIgnoreCase(storageProtocol)

  private def credentialFingerprint(
      credential: TransientStorageCredentials): (AuthMethod.AuthMethod, String) =
    (credential.authMethod, normalizeSasToken(credential.sasKey))

  /**
   * ABFS auth settings (auth type and SAS token alike) are configured per storage account, so
   * several containers of the same account cannot be served when they carry different SAS tokens,
   * or when they mix SAS with impersonation. Elect one credential per account and drop the
   * conflicting siblings, so the export only targets locations that can be read back afterwards.
   * A self contained SAS token always wins over impersonation, which depends on the ambient
   * identity of the runtime and would otherwise make the readback depend on the order in which
   * the credentials were returned; between credentials of the same auth method the first one
   * wins. OneLake credentials (which have no storage account) and accounts whose containers agree
   * on one credential are left untouched.
   */
  private[kusto] def dedupeConflictingCredentials(
      storageParameters: TransientStorageParameters): TransientStorageParameters = {
    val credentials = storageParameters.storageCredentials
    if (credentials == null || credentials.length < 2) {
      storageParameters
    } else {
      val electedCredentialByAccount = mutable.Map.empty[String, (AuthMethod.AuthMethod, String)]
      credentials.foreach { credential =>
        if (credential != null && !credential.isOneLake) {
          val host = storageAccountHost(credential, storageParameters)
          val fingerprint = credentialFingerprint(credential)
          val elected = electedCredentialByAccount.get(host)
          if (elected.isEmpty ||
            (elected.get._1 != AuthMethod.Sas && fingerprint._1 == AuthMethod.Sas)) {
            electedCredentialByAccount.put(host, fingerprint)
          }
        }
      }
      val kept = credentials.filter { credential =>
        if (credential == null || credential.isOneLake) {
          true
        } else {
          val host = storageAccountHost(credential, storageParameters)
          val elected = electedCredentialByAccount(host)
          if (credentialFingerprint(credential) == elected) {
            true
          } else {
            KDSU.logWarn(
              className,
              s"Dropping export container '${credential.blobContainer}' of storage account " +
                s"'$host': ABFS resolves storage credentials per account and another container " +
                s"of the same account is already in use with ${elected._1} credentials.")
            false
          }
        }
      }
      if (kept.length == credentials.length) {
        storageParameters
      } else {
        new TransientStorageParameters(kept, storageParameters.endpointSuffix)
      }
    }
  }

  private def verifySasConfig(
      sasConfigKey: String,
      sparkConf: RuntimeConfig,
      config: Configuration,
      useAbfs: Boolean): Unit = {
    val hadoopConfigSet = config.get(sasConfigKey) != null
    val sparkConfSet = sparkConf.getOption(s"$sparkHadoopConfigPrefix$sasConfigKey").isDefined

    if (!hadoopConfigSet && !sparkConfSet) {
      KDSU.logWarn(
        className,
        s"WARNING: SAS config key '$sasConfigKey' NOT found in HadoopConf or SparkConf after setup!")
    }
  }

  private def whitelistExportContainers(
      storageParameters: TransientStorageParameters,
      config: Configuration,
      sparkConf: RuntimeConfig): Unit = {
    val endpointKey = "fs.azure.abfs.valid.endpoints"
    val currentEndpoints = config.get(endpointKey, "")
    val existingSet = currentEndpoints
      .split(',')
      .iterator
      .map(_.trim)
      .filter(_.nonEmpty)
      .toSet
    // Default storage domain (e.g. core.windows.net) plus any per-credential OneLake hosts
    // (e.g. onelake.dfs.fabric.microsoft.com) that the Fabric/MWC abfss client must accept.
    val oneLakeEndpoints = storageParameters.storageCredentials
      .filter(_.isOneLake)
      .map(_.oneLakeEndpoint)
      .filter(_ != null)
      .distinct
      .toSeq
    val candidates = Seq(storageParameters.endpointSuffix) ++ oneLakeEndpoints
    val toAdd = candidates.filter(d => d != null && d.nonEmpty && !existingSet.contains(d.trim))
    if (toAdd.nonEmpty) {
      val updatedEndpoints =
        (Seq(currentEndpoints).filter(_.nonEmpty) ++ toAdd).mkString(",")
      setHadoopConf(config, sparkConf, endpointKey, updatedEndpoints)
      KDSU.logInfo(
        className,
        s"Updated $endpointKey from '$currentEndpoints' to '$updatedEndpoints'")
    }
  }

  private def calculatePartitions(partitionInfo: PartitionOptions): Array[Partition] = {
    partitionInfo.mode.get match {
      case "hash" => calculateHashPartitions(partitionInfo)
      case _ =>
        throw new InvalidParameterException(
          s"Partitioning mode '${partitionInfo.mode}' is not valid")
    }
  }

  private def calculateHashPartitions(partitionInfo: PartitionOptions): Array[Partition] = {
    // Single partition
    if (partitionInfo.amount <= 1) Array[Partition](KustoPartition(None, 0))

    val partitions = new Array[Partition](partitionInfo.amount)
    for (partitionId <- 0 until partitionInfo.amount) {
      partitionInfo.column match {
        case Some(columnName) =>
          val partitionPredicate = s" hash($columnName, ${partitionInfo.amount}) == $partitionId"
          partitions(partitionId) = KustoPartition(Some(partitionPredicate), partitionId)
        case None => KDSU.logWarn(className, "Column name is empty when requesting for export")
      }
    }
    partitions
  }
}

private[kusto] class KustoReader(client: ExtendedKustoClient) {
  private val myName = this.getClass.getSimpleName

  private[kusto] def exportPartitionsToBlob(
      partitions: Seq[KustoPartition],
      request: KustoReadRequest,
      storage: TransientStorageParameters,
      directory: String,
      options: KustoReadOptions,
      filtering: KustoFiltering): Seq[String] =
    partitions.map(partition =>
      exportPartitionToBlobWithRetries(
        partition,
        request,
        storage,
        directory,
        options,
        filtering))

  private[kusto] def exportRetryDelayMillis(retryNumber: Int): Long =
    KustoReader.exportRetryInterval.apply(retryNumber)

  private[kusto] def exportTimeNanos(): Long = System.nanoTime()

  private[kusto] def waitBeforeExportRetry(delayMillis: Long): Unit = Thread.sleep(delayMillis)

  private[kusto] def exportPartitionToBlobWithRetries(
      partition: KustoPartition,
      request: KustoReadRequest,
      storage: TransientStorageParameters,
      directory: String,
      options: KustoReadOptions,
      filtering: KustoFiltering): String = {
    val started = exportTimeNanos()
    var lastFailure: Throwable = null

    def remainingTimeout(): FiniteDuration = {
      if (Thread.currentThread().isInterrupted) {
        throw new InterruptedException("Interrupted while exporting a Kusto read partition")
      }
      if (request.timeout < Duration.Zero) {
        request.timeout
      } else {
        val remaining = request.timeout - (exportTimeNanos() - started).nanos
        if (remaining <= Duration.Zero) {
          val error = new TimeoutAwaitingPendingOperationException(
            s"Timed out exporting partition ${partition.idx}. Connector requestId: ${request.requestId}")
          if (lastFailure != null) error.initCause(lastFailure)
          throw error
        }
        remaining
      }
    }

    def fail(exception: Throwable, summary: String): Nothing = {
      KDSU.logWarn(myName, s"$summary Connector requestId: ${request.requestId}")
      exception match {
        case failed: FailedOperationException =>
          throw KustoReader.withExportReadContext(failed, request.requestId, summary)
        case other => throw other
      }
    }

    @tailrec
    def attempt(attemptNumber: Int): String = {
      remainingTimeout()
      val attemptDirectory =
        s"${directory.stripSuffix("/")}/part${partition.idx}/attempt${attemptNumber}_${UUID.randomUUID()}/"
      val failure =
        try {
          exportPartitionToBlobAttempt(
            partition,
            request,
            storage,
            attemptDirectory,
            options,
            filtering,
            () => remainingTimeout(),
            Some(KustoReader.exportSubmissionRetryConfig))
          None
        } catch {
          case exception: FailedOperationException =>
            if (!KustoReader.operationField(exception, "State").contains("Throttled")) {
              throw KustoReader.withExportReadContext(exception, request.requestId)
            }
            Some(exception)
          case exception: ThrottleException => Some(exception)
          case exception: InterruptedException =>
            Thread.currentThread().interrupt()
            throw exception
        }

      failure match {
        case None => attemptDirectory
        case Some(exception) =>
          lastFailure = exception
          if (attemptNumber >= KustoReader.MaxExportAttempts) {
            fail(exception, s"Automatic export retries exhausted after $attemptNumber attempts.")
          }
          val delayMillis = exportRetryDelayMillis(attemptNumber)
          if (request.timeout >= Duration.Zero && remainingTimeout() <= delayMillis.millis) {
            fail(
              exception,
              s"Automatic export retry stopped at attempt $attemptNumber: the next backoff does not fit within timeoutLimit.")
          }
          val operationId = exception match {
            case failed: FailedOperationException =>
              KustoReader.operationField(failed, "OperationId")
            case _ => None
          }
          KDSU.logInfo(
            myName,
            s"Export partition ${partition.idx} was throttled" +
              operationId.map(id => s" (OperationId: $id)").getOrElse("") +
              s"; retry ${attemptNumber}/${KustoReader.MaxExportAttempts - 1} in ${delayMillis}ms." +
              s" Connector requestId: ${request.requestId}")
          try waitBeforeExportRetry(delayMillis)
          catch {
            case interrupted: InterruptedException =>
              Thread.currentThread().interrupt()
              throw interrupted
          }
          attempt(attemptNumber + 1)
      }
    }

    attempt(1)
  }

  // Preserve the single-attempt API and its caller-supplied output directory.
  private[kusto] def exportPartitionToBlob(
      partition: KustoPartition,
      request: KustoReadRequest,
      storage: TransientStorageParameters,
      directory: String,
      options: KustoReadOptions,
      filtering: KustoFiltering): Unit = {
    try {
      exportPartitionToBlobAttempt(
        partition,
        request,
        storage,
        directory,
        options,
        filtering,
        () => request.timeout,
        None)
    } catch {
      case exception: FailedOperationException =>
        throw KustoReader.withExportReadContext(exception, request.requestId)
    }
  }

  private def exportPartitionToBlobAttempt(
      partition: KustoPartition,
      request: KustoReadRequest,
      storage: TransientStorageParameters,
      directory: String,
      options: KustoReadOptions,
      filtering: KustoFiltering,
      remainingTimeout: () => FiniteDuration,
      retryConfig: Option[RetryConfig]): Unit = {
    val supportNewParquetWriter = new ComparableVersion(request.sparkSession.version)
      .compareTo(new ComparableVersion(KustoReader.minimalParquetWriterVersion)) > 0
    if (!supportNewParquetWriter) {
      KDSU.logWarn(
        myName,
        "Setting useNativeParquetWriter=false. Users are advised to move to Spark versions >= 3.3.0 to leverage the performance and cost improvements of" +
          "new encoding schemes introduced in both Kusto parquet files write and Spark parquet read")
    }
    val exportCommand = CslCommandsGenerator.generateExportDataCommand(
      query = KustoFilter.pruneAndFilter(request.schema, request.query, filtering),
      directory = directory,
      partitionId = partition.idx,
      storageParameters = storage,
      partitionPredicate = partition.predicate,
      additionalExportOptions = options.additionalExportOptions,
      supportNewParquetWriter = supportNewParquetWriter)

    val commandResult: KustoResultSetTable = client
      .executeEngine(
        request.kustoCoordinates.database,
        exportCommand,
        "exportPartitionToBlob",
        request.clientRequestProperties.orNull,
        retryConfig = retryConfig)
      .getPrimaryResults
    awaitExportCompletion(request, commandResult, directory, remainingTimeout())
  }

  private[kusto] def awaitExportCompletion(
      request: KustoReadRequest,
      commandResult: KustoResultSetTable,
      directory: String,
      timeout: FiniteDuration): Unit = {
    KDSU.verifyAsyncCommandCompletion(
      client.engineClient,
      request.kustoCoordinates.database,
      commandResult,
      timeOut = timeout,
      doingWhat = s"export data to blob directory: ('$directory') preparing it for reading.",
      loggerName = myName,
      requestId = request.requestId)
  }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import com.azure.core.credential.AzureSasCredential
import com.azure.storage.blob.{BlobContainerClient, BlobContainerClientBuilder}
import com.azure.storage.blob.models.ListBlobsOptions
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
import org.apache.spark.sql.{DataFrame, Row, RuntimeConfig, SparkSession}

import java.net.URI
import java.security.InvalidParameterException
import java.time.{Clock, Instant}
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.{concurrent, mutable}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
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
 * Cached export paths and storage settings. Reuse requires matching storage credentials; Parquet
 * reads carry their own configuration rather than reusing cached ABFS clients.
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
      exception: Exception,
      requestId: String,
      retrySummary: String = ""): FailedOperationException = {
    val result = exception match {
      case failed: FailedOperationException => failed.getResult
      case _ => None
    }
    // Read the current operation row without advancing the cursor or masking the original error.
    def field(name: String): Option[String] =
      result.flatMap(table => Try(table.getString(name)).toOption.flatMap(Option(_)))

    val submissionThrottled = exception.isInstanceOf[ThrottleException]
    val throttled = submissionThrottled || field("State").contains("Throttled")
    val exportCapacityExceeded = throttled && field("Status").exists(status =>
      exportCapacityOrigin.findFirstIn(status).isDefined)
    val reason = if (submissionThrottled) {
      "Kusto Spark read failed because its distributed export submission was throttled (HTTP 429)."
    } else if (exportCapacityExceeded) {
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
   * Preserves an existing Hadoop configuration value. When absent, sets the default in both
   * Hadoop and Spark session configuration without mirroring an inherited implementation.
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

  private case class InstalledAccountSetting(previous: Option[String], value: String)

  // Account-scoped auth settings the connector wrote into session-wide configuration (kept there
  // for native readers), with the values they replaced. Tracked per configuration object, and
  // dropped with it, so impersonation readback can exclude them from its private view.
  private val installedHadoopAuth =
    new java.util.WeakHashMap[Configuration, mutable.Map[String, InstalledAccountSetting]]()
  private val installedSparkAuth =
    new java.util.WeakHashMap[RuntimeConfig, mutable.Map[String, InstalledAccountSetting]]()

  private def recordInstall[K](
      registry: java.util.WeakHashMap[K, mutable.Map[String, InstalledAccountSetting]],
      owner: K,
      key: String,
      current: Option[String],
      value: String): Unit = {
    val records = registry.computeIfAbsent(owner, _ => mutable.Map.empty)
    val previous = records.get(key) match {
      case Some(installed) if current.contains(installed.value) => installed.previous
      case _ => current
    }
    records.put(key, InstalledAccountSetting(previous, value))
  }

  private def installAccountAuth(
      config: Configuration,
      sparkConf: RuntimeConfig,
      key: String,
      value: String): Unit = installedHadoopAuth.synchronized {
    recordInstall(installedHadoopAuth, config, key, Option(config.get(key)), value)
    val sparkKey = s"$sparkHadoopConfigPrefix$key"
    recordInstall(installedSparkAuth, sparkConf, key, sparkConf.getOption(sparkKey), value)
    setHadoopConf(config, sparkConf, key, value)
  }

  /**
   * Session Hadoop configuration and options as they were before the connector installed explicit
   * storage credentials; settings changed since installation are kept.
   */
  private def ambientReadConfiguration(
      spark: SparkSession): (Configuration, Map[String, String], Set[String]) =
    installedHadoopAuth.synchronized {
      val shared = spark.sparkContext.hadoopConfiguration
      val config = new Configuration(shared)
      Option(installedHadoopAuth.get(shared)).foreach(_.foreach { case (key, installed) =>
        if (Option(config.get(key)).contains(installed.value)) {
          installed.previous match {
            case Some(value) => config.set(key, value)
            case None => config.unset(key)
          }
        }
      })
      val sessionOptions = Option(installedSparkAuth.get(spark.conf))
        .map(_.foldLeft(spark.conf.getAll) { case (options, (key, installed)) =>
          val sparkKey = s"$sparkHadoopConfigPrefix$key"
          if (!options.get(sparkKey).contains(installed.value)) options
          else installed.previous.fold(options - sparkKey)(value => options + (sparkKey -> value))
        })
        .getOrElse(spark.conf.getAll)
      val recorded = Seq(installedHadoopAuth.get(shared), installedSparkAuth.get(spark.conf))
        .flatMap(Option(_))
        .flatMap(_.keys)
        .toSet
      (config, sessionOptions, recorded)
    }

  private val distributedReadModeTransientCache: concurrent.Map[
    DistributedReadModeTransientCacheKey,
    DistributedReadModeTransientCacheEntry] =
    new concurrent.TrieMap()
  // Unless explicitly overridden, disable the native Parquet writer at or below this version.
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
    val readStorage = snapshotStorage(storage)
    // OneLake transient storage always reads back over abfss.
    val protocol =
      if (readStorage.storageCredentials.exists(_.isOneLake)) KCONST.storageProtocolAbfss
      else KDSU.resolveStorageProtocol(options.storageProtocol)
    val exportStorage =
      if (isAbfsProtocol(protocol)) dedupeConflictingCredentials(readStorage) else readStorage
    // if distributedReadModeTransientCacheEnabled is set to true, then check if path is cached and use it
    // if not export and cache the path for reuse
    val exported = if (options.distributedReadModeTransientCacheEnabled) {
      val key = DistributedReadModeTransientCacheKey(
        request.query,
        request.kustoCoordinates,
        request.authentication,
        options.enableExportStorageApi)
      val cachedExport = distributedReadModeTransientCache
        .get(key)
        .filter(cachedExportMatches(_, exportStorage, protocol))
      if (cachedExport.isDefined) {
        KDSU.logInfo(
          className,
          "Fetching from distributedReadModeTransientCache: hit, reusing cached export paths")
        val cached = cachedExport.get
        // Reinstall account-scoped ABFS credentials before reusing cached paths.
        if (isAbfsProtocol(cached.storageProtocol)) {
          setupBlobAccess(request, cached.storage, cached.storageProtocol, forceRefresh = true)
        }
        cached
      } else {
        KDSU.logInfo(
          className,
          "distributedReadModeTransientCache: miss, exporting to cache paths")
        val filter = determineFilterPushDown(
          options.queryFilterPushDown,
          queryFilterPushDownDefault = false,
          filtering)
        val exported =
          exportToStorage(kustoClient, request, exportStorage, options, filter, protocol)
        distributedReadModeTransientCache(key) = exported
        exported
      }
    } else {
      val filter = determineFilterPushDown(
        options.queryFilterPushDown,
        queryFilterPushDownDefault = true,
        filtering)
      exportToStorage(kustoClient, request, exportStorage, options, filter, protocol)
    }
    val paths = exported.paths

    val rdd =
      try {
        readParquet(request.sparkSession, paths, exported.storage, exported.storageProtocol).rdd
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

  private[kusto] def readParquet(
      spark: SparkSession,
      paths: Seq[String],
      storage: TransientStorageParameters,
      storageProtocol: String): DataFrame = {
    val options = blobReadOptions(spark, storage, storageProtocol)
    def plan(): DataFrame = {
      val frame = spark.read.options(options).parquet(paths: _*)
      // Complete file-index/physical-plan construction while driver delegates are owned.
      frame.rdd
      frame
    }
    options.get(KustoBlobReadFileSystem.ReadId) match {
      case Some(id) => KustoBlobReadFileSystem.withReadScope(id)(plan())
      case None => spark.read.options(options).parquet(paths: _*)
    }
  }

  private[kusto] def blobReadOptions(
      spark: SparkSession,
      storage: TransientStorageParameters,
      storageProtocol: String): Map[String, String] = {
    if (!isAbfsProtocol(storageProtocol) || storage.storageCredentials.exists(_.isOneLake)) {
      Map.empty
    } else {
      val (config, sessionOptions, recorded) = ambientReadConfiguration(spark)
      sessionOptions.filter(_._1.startsWith("fs.")).foreach { case (key, value) =>
        config.set(key, value)
      }
      sessionOptions.filter(_._1.startsWith(sparkHadoopConfigPrefix)).foreach {
        case (key, value) =>
          config.set(key.stripPrefix(sparkHadoopConfigPrefix), value)
      }
      val credentials = storage.storageCredentials.iterator.flatMap { credential =>
        val host = storageAccountHost(credential, storage)
        credential.authMethod match {
          case AuthMethod.Key =>
            abfsAccountKeyOptions(credential, host) ++ Map(
              s"fs.azure.account.keyprovider.$host" ->
                classOf[org.apache.hadoop.fs.azurebfs.services.SimpleKeyProvider].getName)
          case AuthMethod.Sas =>
            abfsSasOptions(credential, host) ++ Map(
              s"fs.azure.sas.token.provider.type.$host" ->
                classOf[KustoFixedSasTokenProvider].getName)
          case _ => Map.empty[String, String]
        }
      }.toMap
      val protocol = storageProtocol.toLowerCase(java.util.Locale.ROOT)
      val implementation = FileSystem.getFileSystemClass(protocol, config).getName
      val hosts = storage.storageCredentials.map(storageAccountHost(_, storage))
      val ambient = config
        .iterator()
        .asScala
        .filter(entry => hosts.exists(host => entry.getKey.endsWith(s".$host")))
        .map(entry => entry.getKey -> entry.getValue)
        .toMap
      // Read options cannot remove session settings, so the readback filesystem drops
      // connector-installed credentials that have no ambient value of their own.
      val removed = recorded
        .filter(key => hosts.exists(host => key.endsWith(s".$host")))
        .filter(key => config.get(key) == null && !credentials.contains(key))
      ambient ++ credentials ++ Map(
        KustoBlobReadFileSystem.UnsetKeys -> removed.toSeq.sorted.mkString(","),
        KustoBlobReadFileSystem.ReadId -> UUID.randomUUID().toString,
        KustoBlobReadFileSystem.DelegateImplementation -> implementation,
        s"fs.$protocol.impl" -> classOf[KustoBlobReadFileSystem].getName,
        s"fs.$protocol.impl.disable.cache" -> "true")
    }
  }

  private[kusto] def snapshotStorage(
      storage: TransientStorageParameters): TransientStorageParameters =
    new TransientStorageParameters(
      storage.storageCredentials.map { source =>
        val copy = new TransientStorageCredentials()
        copy.blobContainer = source.blobContainer
        copy.storageAccountName = source.storageAccountName
        copy.storageAccountKey = source.storageAccountKey
        copy.sasKey = source.sasKey
        copy.sasUrl = source.sasUrl
        copy.domainSuffix = source.domainSuffix
        copy.oneLakeUrl = source.oneLakeUrl
        copy.oneLakeWorkspace = source.oneLakeWorkspace
        copy.oneLakeEndpoint = source.oneLakeEndpoint
        copy.oneLakeArtifactPath = source.oneLakeArtifactPath
        copy
      },
      storage.endpointSuffix)

  private[kusto] def cachedExportMatches(
      cached: DistributedReadModeTransientCacheEntry,
      storage: TransientStorageParameters,
      protocol: String): Boolean = {
    def identities(parameters: TransientStorageParameters) =
      parameters.storageCredentials.toSeq.map(credential =>
        (
          credential.oneLakeAbfssBase,
          credential.storageAccountName,
          credential.blobContainer,
          credentialFingerprint(credential)))
    cached.storageProtocol == protocol && cached.storage.endpointSuffix == storage.endpointSuffix &&
    identities(cached.storage) == identities(storage)
  }

  private[kusto] def dirExist(
      spark: SparkSession,
      params: TransientStorageCredentials,
      directory: String,
      endpointSuffix: String,
      storageProtocol: String = KCONST.storageProtocolAbfss): Boolean = {
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
      val hadoopConf = new Configuration(spark.sparkContext.hadoopConfiguration)
      blobReadOptions(
        spark,
        new TransientStorageParameters(Array(params), endpointSuffix),
        storageProtocol)
        .foreach { case (key, value) => hadoopConf.set(key, value) }
      val fs = FileSystem.newInstance(new URI(url), hadoopConf)
      val path = new Path(url + s"/$directory")
      try fs.exists(path)
      finally fs.close()
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
      blobDirectoryExists(container, directory)
    }
  }

  private[kusto] def blobDirectoryExists(
      container: BlobContainerClient,
      directory: String): Boolean =
    container.listBlobs(new ListBlobsOptions().setPrefix(directory), null).iterator().hasNext

  private def exportToStorage(
      kustoClient: ExtendedKustoClient,
      request: KustoReadRequest,
      exportStorage: TransientStorageParameters,
      options: KustoReadOptions,
      filtering: KustoFiltering,
      protocol: String): DistributedReadModeTransientCacheEntry = {

    KDSU.logInfo(
      className,
      s"Starting exporting data from Kusto to blob storage in Distributed mode. requestId: ${request.requestId}")

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
      storageProtocol: String = KCONST.storageProtocolAbfss,
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
            forceRefresh)
        case AuthMethod.Sas =>
          handleSasAuth(storage, storageParameters, config, sparkConf, now, useAbfs, forceRefresh)
        case _ =>
        // Impersonation (including OneLake) relies on ambient AAD configured by the
        // Spark runtime (e.g. Fabric notebooks). Nothing to install here; JVM readback
        // excludes connector-installed explicit credentials (see ambientReadConfiguration).
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
      forceRefresh: Boolean): Unit = {
    val wasCached = !forceRefresh && !useAbfs &&
      KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(
        storage.storageAccountName,
        storage.storageAccountKey,
        now)

    if (!wasCached) {
      val accountHost = storageAccountHost(storage, storageParameters)
      if (useAbfs) {
        abfsAccountKeyOptions(storage, accountHost).foreach { case (key, value) =>
          installAccountAuth(config, sparkConf, key, value)
        }
      } else {
        setHadoopConf(
          config,
          sparkConf,
          s"fs.azure.account.key.$accountHost",
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

    // Always install account-scoped ABFS settings: a cache hit does not verify session state.
    // WASBS retains its container-scoped setup cache.
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
    abfsSasOptions(storage, accountHost).foreach { case (key, value) =>
      installAccountAuth(config, sparkConf, key, value)
    }
    KDSU.logInfo(className, s"Set ABFS SAS config: $sasConfigKey")
  }

  private def abfsAccountKeyOptions(
      storage: TransientStorageCredentials,
      host: String): Map[String, String] =
    Map(
      s"fs.azure.account.auth.type.$host" -> "SharedKey",
      s"fs.azure.account.hns.enabled.$host" -> "false",
      s"fs.azure.account.key.$host" -> storage.storageAccountKey)

  private def abfsSasOptions(
      storage: TransientStorageCredentials,
      host: String): Map[String, String] =
    Map(
      s"fs.azure.account.auth.type.$host" -> "SAS",
      s"fs.azure.account.hns.enabled.$host" -> "false",
      s"fs.azure.sas.fixed.token.$host" -> normalizeSasToken(storage.sasKey),
      // Back-compat for runtimes adapted to the older container-qualified key.
      s"fs.azure.sas.fixed.token.${storage.blobContainer}.$host" -> normalizeSasToken(
        storage.sasKey))

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
    (
      credential.authMethod,
      if (credential.authMethod == AuthMethod.Key) credential.storageAccountKey
      else normalizeSasToken(credential.sasKey))

  /**
   * ABFS auth settings (auth type and SAS token alike) are configured per storage account, so
   * several containers of the same account cannot be served when they carry different
   * credentials. Elect one credential per account and drop the conflicting siblings, so the
   * export only targets locations that can be read back afterwards. SAS takes precedence over
   * account keys, and both take precedence over ambient impersonation; between credentials of the
   * same auth method the first one wins. OneLake credentials (which have no storage account) and
   * accounts whose containers agree on one credential are left untouched.
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
            (elected.get._1 != AuthMethod.Sas && fingerprint._1 == AuthMethod.Sas) ||
            (elected.get._1 == AuthMethod.Impersonation && fingerprint._1 == AuthMethod.Key)) {
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
        case throttled: ThrottleException =>
          throw KustoReader.withExportReadContext(throttled, request.requestId, summary)
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

  // Uses the caller-supplied output directory and the existing submission retry policy.
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
        "For this Spark version, useNativeParquetWriter defaults to false unless explicitly overridden.")
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
    val pollingTimeout =
      try remainingTimeout()
      catch {
        case timeout: TimeoutAwaitingPendingOperationException =>
          // Leave the result cursor untouched for the completion helper.
          val operationId = Try(commandResult.getData.get(0).get(0).toString).toOption
            .filter(_.nonEmpty)
            .getOrElse("<unavailable>")
          val error = new TimeoutAwaitingPendingOperationException(
            s"Timed out exporting partition ${partition.idx} after submission. OperationId: '$operationId'." +
              s" Connector requestId: ${request.requestId}. The operation may still be running.")
          error.initCause(timeout)
          throw error
      }
    awaitExportCompletion(request, commandResult, directory, pollingTimeout)
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

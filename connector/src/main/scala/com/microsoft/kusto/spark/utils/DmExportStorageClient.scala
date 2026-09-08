// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.azure.core.http.{HttpClient, HttpHeaderName, HttpMethod, HttpRequest, HttpResponse}
import com.azure.core.util.Context
import com.fasterxml.jackson.databind.JsonNode
import com.microsoft.azure.kusto.data.{StringUtils, UriUtils, Utils}
import com.microsoft.azure.kusto.data.auth.endpoints.KustoTrustedEndpoints
import com.microsoft.azure.kusto.data.exceptions.KustoClientInvalidConnectionStringException
import com.microsoft.azure.kusto.data.auth.{
  ConnectionStringBuilder,
  TokenProviderBase,
  TokenProviderFactory
}
import com.microsoft.azure.kusto.data.format.CslTimespanFormat
import com.microsoft.azure.kusto.data.http.{HttpRequestBuilder, HttpStatus}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.retry.RetryConfig

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZonedDateTime}
import java.util.UUID
import java.util.zip.{GZIPInputStream, InflaterInputStream}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/** ExportStorage targets: SAS blob containers and OneLake folders. */
private[kusto] final case class ExportStorageTargets(
    containers: Seq[String],
    lakeFolders: Seq[String]) {
  def isEmpty: Boolean = containers.isEmpty && lakeFolders.isEmpty
}

private[kusto] sealed trait ExportStorageMode {
  def select(targets: ExportStorageTargets): ExportStorageTargets
}

private[kusto] object ExportStorageMode {
  case object LakeFolders extends ExportStorageMode {
    override def select(targets: ExportStorageTargets): ExportStorageTargets =
      targets.copy(containers = Seq.empty)
  }

  case object BlobContainers extends ExportStorageMode {
    override def select(targets: ExportStorageTargets): ExportStorageTargets =
      targets.copy(lakeFolders = Seq.empty)
  }

  case object LegacyCommand extends ExportStorageMode {
    override def select(targets: ExportStorageTargets): ExportStorageTargets =
      ExportStorageTargets(Seq.empty, Seq.empty)
  }
}

private[kusto] final case class ExportStorageDecision(
    mode: ExportStorageMode,
    refreshInterval: Duration)

private[kusto] final case class ExportStorageResolution(
    targets: Option[ExportStorageTargets],
    refreshInterval: Duration)

/** A value cached until its monotonic deadline. */
private[kusto] final case class Memo[A](value: A, expiresAtNanos: Long) {
  def unexpired: Option[A] =
    if (System.nanoTime() - expiresAtNanos < 0) Some(value) else None
}

private[kusto] object Memo {
  def of[A](value: A, ttl: Duration): Memo[A] = Memo(value, System.nanoTime() + ttl.toNanos)
}

/** Configuration fields used to select the export-storage path. */
private[kusto] final case class IngestionConfiguration(
    lakeFolders: Seq[String],
    preferredUploadMethod: Option[String],
    preferredIngestionMethod: Option[String],
    refreshInterval: Option[Duration] = None,
    invalidRefreshInterval: Boolean = false) {

  def exportStorageMode: ExportStorageMode =
    (preferredIngestionMethod, preferredUploadMethod) match {
      case (Some(ingestion), Some(upload))
          if ingestion.equalsIgnoreCase(IngestionConfiguration.IngestionMethodRest) &&
            upload.equalsIgnoreCase(IngestionConfiguration.UploadMethodLake) =>
        ExportStorageMode.LakeFolders
      case (Some(ingestion), Some(upload))
          if ingestion.equalsIgnoreCase(IngestionConfiguration.IngestionMethodRest) &&
            upload.equalsIgnoreCase(IngestionConfiguration.UploadMethodStorage) =>
        ExportStorageMode.BlobContainers
      // Missing, unknown, and all non-Rest combinations retain the legacy path.
      case _ => ExportStorageMode.LegacyCommand
    }
}

private[kusto] object IngestionConfiguration {
  val UploadMethodLake = "Lake"
  val UploadMethodStorage = "Storage"
  val IngestionMethodRest = "Rest"
}

/** Resolves export storage through the Data Management REST APIs. */
private[kusto] class DmExportStorageClient(
    val ingestKcsb: ConnectionStringBuilder,
    val clusterAlias: String) {

  private val className = this.getClass.getSimpleName
  private val objectMapper = Utils.getObjectMapper

  /** Avoids repeated export-storage 404s within one refresh window. */
  @volatile private var endpointUnsupported: Option[Memo[Duration]] = None

  private def unsupportedRefreshInterval: Option[Duration] =
    endpointUnsupported.flatMap(_.unexpired)

  /** Caches the selected mode for the service-provided refresh interval. */
  @volatile private var exportStorageModeState: Option[Memo[ExportStorageDecision]] = None
  private val exportStorageModeLock = new Object

  protected lazy val httpClient: HttpClient = HttpClient.createDefault()

  private lazy val maybeTokenProvider: Option[TokenProviderBase] =
    Try(Option(TokenProviderFactory.createTokenProvider(ingestKcsb, httpClient))) match {
      case Success(provider) => provider
      case Failure(exception) =>
        KDSU.logWarn(
          className,
          s"Could not create a token provider for the export storage API on '$clusterAlias': " +
            s"${exception.getMessage}. Falling back to the export containers command.")
        None
    }

  private lazy val normalizedClusterUrl: String =
    UriUtils.createClusterURLFrom(ingestKcsb.getClusterUrl.trim)

  private lazy val exportStorageUrl: String =
    UriUtils.appendPathToUri(normalizedClusterUrl, KustoConstants.ExportStorageRestApiPath)

  private lazy val ingestionConfigurationUrl: String =
    UriUtils.appendPathToUri(
      normalizedClusterUrl,
      KustoConstants.IngestionConfigurationRestApiPath)

  private val retryConfig = RetryConfig.custom
    .maxAttempts(DmExportStorageClient.MaxRetryAttempts)
    .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(
      ExtendedKustoClient.BaseIntervalMs,
      IntervalFunction.DEFAULT_MULTIPLIER,
      IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR,
      ExtendedKustoClient.MaxRetryIntervalMs))
    .retryOnException((e: Throwable) =>
      !e.isInstanceOf[PermanentExportStorageApiException] &&
        !e.isInstanceOf[InterruptedException])
    .build

  private val GateMaxAttempts = 2
  private val gateRetryConfig = RetryConfig.from(retryConfig).maxAttempts(GateMaxAttempts).build

  /** Returns selected API targets, or `None` to use `.show export containers`. */
  def getExportStorage: Option[ExportStorageTargets] = resolveExportStorage.targets

  /** Resolves targets and the interval after which a new resolution is required. */
  private[kusto] def resolveExportStorage: ExportStorageResolution = {
    unsupportedRefreshInterval match {
      case Some(refreshInterval) =>
        ExportStorageResolution(None, refreshInterval)
      case None if maybeTokenProvider.isEmpty =>
        ExportStorageResolution(None, DmExportStorageClient.DefaultRefreshInterval)
      case None =>
        resolveSupportedExportStorage()
    }
  }

  private def resolveSupportedExportStorage(): ExportStorageResolution = {
    val decision = exportStorageDecision
    if (decision.mode == ExportStorageMode.LegacyCommand) {
      ExportStorageResolution(None, decision.refreshInterval)
    } else {
      Try(
        preserveInterrupt(
          KDSU.retryApplyFunction(
            attempt => fetchExportStorage(attempt),
            retryConfig,
            "Get export storage from DM with retries"))) match {
        case Success(targets) =>
          val selected = decision.mode.select(targets)
          if (!selected.isEmpty) {
            KDSU.logInfo(
              className,
              s"Selected ${selected.containers.size} export container(s) and " +
                s"${selected.lakeFolders.size} lake folder(s) from the export storage API of " +
                s"'ingest-$clusterAlias' using mode ${decision.mode}")
            ExportStorageResolution(Some(selected), decision.refreshInterval)
          } else {
            KDSU.logWarn(
              className,
              s"The export storage API of 'ingest-$clusterAlias' returned no targets for " +
                s"mode ${decision.mode}. Falling back to the export containers command.")
            ExportStorageResolution(None, decision.refreshInterval)
          }
        case Failure(_: UnsupportedExportStorageApiException) =>
          endpointUnsupported = Some(Memo.of(decision.refreshInterval, decision.refreshInterval))
          KDSU.logInfo(
            className,
            s"The export storage API is not available on 'ingest-$clusterAlias' (HTTP 404). " +
              "Using the export containers command for this refresh window.")
          ExportStorageResolution(None, decision.refreshInterval)
        case Failure(exception: UntrustedEndpointException) =>
          throw exception
        case Failure(exception: PermanentExportStorageApiException) =>
          KDSU.logWarn(
            className,
            s"Cannot use the export storage API of 'ingest-$clusterAlias': " +
              s"${exception.getMessage}. Using the export containers command for this refresh window.")
          ExportStorageResolution(None, decision.refreshInterval)
        case Failure(exception) =>
          KDSU.logWarn(
            className,
            s"Failed to get export storage from the API of 'ingest-$clusterAlias': " +
              s"${exception.getMessage}. Falling back to the export containers command and " +
              s"retrying discovery after ${DmExportStorageClient.TransientFailureRefreshInterval}.")
          ExportStorageResolution(None, DmExportStorageClient.TransientFailureRefreshInterval)
      }
    }
  }

  private def preserveInterrupt[T](operation: => T): T =
    try {
      operation
    } catch {
      case interrupted: InterruptedException =>
        Thread.currentThread().interrupt()
        throw interrupted
    }

  /** Sends bearer tokens only to HTTPS endpoints trusted by the Kusto SDK. */
  private def validateEndpoint(): Unit = {
    val clusterUrl = ingestKcsb.getClusterUrl
    if (!clusterUrl.toLowerCase.startsWith("https://")) {
      throw new UntrustedEndpointException(
        s"Refusing to send a bearer token over a non-https endpoint: $clusterUrl")
    }
    Try(KustoTrustedEndpoints.validateTrustedEndpoint(clusterUrl)) match {
      case Failure(rejection: KustoClientInvalidConnectionStringException) =>
        throw new UntrustedEndpointException(
          s"Refusing to send a bearer token to '$clusterUrl': the Kusto SDK does not recognise " +
            s"it as a trusted endpoint (${rejection.getMessage}). The export containers command " +
            "applies the same check to the same url, so it will report the same problem.")
      case Failure(undecided) =>
        throw new PermanentExportStorageApiException(
          s"Could not establish whether '$clusterUrl' is a trusted Kusto endpoint " +
            s"(${undecided.getMessage}). Not sending a token until it is established.")
      case _ =>
    }
  }

  /** Bounds both compressed and decoded response bodies. */
  private def readBody(response: HttpResponse, api: String): String = {
    val declared =
      Option(response.getHeaderValue(HttpHeaderName.CONTENT_LENGTH)).flatMap(value =>
        Try(value.toLong).toOption)
    declared.filter(_ > DmExportStorageClient.MaxResponseBodyBytes).foreach { length =>
      throw new PermanentExportStorageApiException(
        s"$api returned a $length byte body, above the " +
          s"${DmExportStorageClient.MaxResponseBodyBytes} byte limit. Refusing to buffer it.")
    }
    val encoded = readLimited(response.getBody.toIterable.asScala, api)
    val contentEncoding =
      Option(response.getHeaderValue(HttpHeaderName.CONTENT_ENCODING)).map(_.toLowerCase)
    val decoded = contentEncoding match {
      case Some(value) if value.contains("gzip") =>
        new GZIPInputStream(new ByteArrayInputStream(encoded))
      case Some(value) if value.contains("deflate") =>
        new InflaterInputStream(new ByteArrayInputStream(encoded))
      case _ => new ByteArrayInputStream(encoded)
    }
    try {
      new String(readLimited(decoded, api), StandardCharsets.UTF_8)
    } finally {
      decoded.close()
    }
  }

  private def readLimited(input: InputStream, api: String): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    var total = 0L
    var count = input.read(buffer)
    while (count >= 0) {
      if (count > 0) {
        total += count
        ensureWithinBodyLimit(total, api)
        output.write(buffer, 0, count)
      }
      count = input.read(buffer)
    }
    output.toByteArray
  }

  private def readLimited(chunks: Iterable[ByteBuffer], api: String): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    var total = 0L
    chunks.foreach { buffer =>
      val count = buffer.remaining()
      total += count
      ensureWithinBodyLimit(total, api)
      val bytes = new Array[Byte](count)
      buffer.get(bytes)
      output.write(bytes)
    }
    output.toByteArray
  }

  private def ensureWithinBodyLimit(total: Long, api: String): Unit =
    if (total > DmExportStorageClient.MaxResponseBodyBytes) {
      throw new PermanentExportStorageApiException(
        s"$api exceeded the ${DmExportStorageClient.MaxResponseBodyBytes} byte body limit.")
    }

  private def honourRetryAfter(
      response: HttpResponse,
      api: String,
      attempt: Int,
      maxAttempts: Int): Unit =
    if (attempt >= maxAttempts) {
      ()
    } else
      DmExportStorageClient
        .retryAfterMillis(Option(response.getHeaders.getValue("Retry-After")))
        .foreach { waitMs =>
          KDSU.logInfo(
            className,
            s"$api throttled 'ingest-$clusterAlias'; honouring Retry-After of ${waitMs}ms " +
              "before the next attempt.")
          try {
            Thread.sleep(waitMs)
          } catch {
            case interrupted: InterruptedException =>
              Thread.currentThread().interrupt()
              throw interrupted
          }
        }

  private def buildJsonGetRequest(
      url: String,
      clientRequestId: String,
      operation: String): HttpRequest = {
    val tokenProvider = maybeTokenProvider.getOrElse(
      throw new PermanentExportStorageApiException(
        s"No token provider is available for the $operation"))
    val token = tokenProvider.acquireAccessToken().block()
    if (StringUtils.isBlank(token)) {
      throw new RuntimeException(s"Acquired an empty access token for the $operation")
    }
    new HttpRequestBuilder(HttpMethod.GET, url)
      .withAuthorization(s"Bearer $token")
      .withHeaders(
        Map(
          "x-ms-client-request-id" -> clientRequestId,
          "x-ms-client-version" -> KustoConstants.ClientName).asJava)
      .build()
  }

  /** Returns the cached configuration decision, using legacy mode for inconclusive calls. */
  private def exportStorageDecision: ExportStorageDecision =
    exportStorageModeState.flatMap(_.unexpired) match {
      case Some(known) => known
      case None =>
        exportStorageModeLock.synchronized {
          exportStorageModeState.flatMap(_.unexpired).getOrElse {
            detectExportStorageDecision() match {
              case Some(detected) =>
                exportStorageModeState = Some(Memo.of(detected, detected.refreshInterval))
                detected
              case None =>
                ExportStorageDecision(
                  ExportStorageMode.LegacyCommand,
                  DmExportStorageClient.TransientFailureRefreshInterval)
            }
          }
        }
    }

  private[kusto] def exportStorageMode: ExportStorageMode = exportStorageDecision.mode

  /** Returns `None` when configuration discovery was inconclusive. */
  protected def detectExportStorageDecision(): Option[ExportStorageDecision] = {
    Try(
      preserveInterrupt(
        KDSU.retryApplyFunction(
          attempt => fetchIngestionConfiguration(attempt),
          gateRetryConfig,
          "Get ingestion configuration from DM with retries"))) match {
      case Success(configuration) =>
        val mode = configuration.exportStorageMode
        val refreshInterval =
          DmExportStorageClient.effectiveRefreshInterval(configuration.refreshInterval)
        val modeName = mode match {
          case ExportStorageMode.LakeFolders => "Rest/Lake"
          case ExportStorageMode.BlobContainers => "Rest/Storage"
          case ExportStorageMode.LegacyCommand => "LegacyCommand"
        }
        if (configuration.invalidRefreshInterval) {
          KDSU.logWarn(
            className,
            s"The ingestion configuration of 'ingest-$clusterAlias' returned an invalid " +
              s"refreshInterval; using ${DmExportStorageClient.DefaultRefreshInterval}.")
        }
        configuration.refreshInterval.filter(_ != refreshInterval).foreach { serviceInterval =>
          KDSU.logWarn(
            className,
            s"The ingestion configuration refreshInterval $serviceInterval for " +
              s"'ingest-$clusterAlias' was bounded to $refreshInterval.")
        }
        KDSU.logInfo(
          className,
          s"Selected export storage mode $modeName for 'ingest-$clusterAlias': " +
            s"preferredUploadMethod=" +
            s"${configuration.preferredUploadMethod.getOrElse("<absent>")}, " +
            s"preferredIngestionMethod=" +
            s"${configuration.preferredIngestionMethod.getOrElse("<absent>")} " +
            s"(${configuration.lakeFolders.size} configuration lake folder(s), ignored for " +
            s"selection), refreshInterval=$refreshInterval.")
        Some(ExportStorageDecision(mode, refreshInterval))
      case Failure(_: UnsupportedExportStorageApiException) =>
        KDSU.logInfo(
          className,
          s"The ingestion configuration API is not available on 'ingest-$clusterAlias' " +
            "(HTTP 404). Using the export containers command for this refresh window.")
        Some(
          ExportStorageDecision(
            ExportStorageMode.LegacyCommand,
            DmExportStorageClient.DefaultRefreshInterval))
      case Failure(exception: UntrustedEndpointException) =>
        throw exception
      case Failure(exception: PermanentExportStorageApiException) =>
        KDSU.logWarn(
          className,
          s"Cannot use the ingestion configuration API of 'ingest-$clusterAlias': " +
            s"${exception.getMessage}. Using the export containers command for this refresh window.")
        Some(
          ExportStorageDecision(
            ExportStorageMode.LegacyCommand,
            DmExportStorageClient.DefaultRefreshInterval))
      case Failure(exception) =>
        KDSU.logWarn(
          className,
          s"Could not read the ingestion configuration of 'ingest-$clusterAlias': " +
            s"${exception.getMessage}. Falling back to the export containers command and " +
            s"retrying discovery after ${DmExportStorageClient.TransientFailureRefreshInterval}.")
        None
    }
  }

  private def fetchIngestionConfiguration(attempt: Int): IngestionConfiguration = {
    validateEndpoint()
    val clientRequestId = s"KSPARK.getIngestionConfiguration;${UUID.randomUUID()}"
    val request =
      buildJsonGetRequest(
        ingestionConfigurationUrl,
        clientRequestId,
        "ingestion configuration API")

    KDSU.logDebug(
      className,
      s"Calling ingestion configuration API $ingestionConfigurationUrl, id: $clientRequestId")

    val response = httpClient.sendSync(request, Context.NONE)
    try {
      val status = response.getStatusCode
      val body = readBody(response, "Ingestion configuration API")
      if (status == HttpStatus.NOT_FOUND) {
        throw new UnsupportedExportStorageApiException(
          s"$ingestionConfigurationUrl is not available on this cluster")
      }
      if (status != HttpStatus.OK) {
        val message = s"Ingestion configuration API returned HTTP $status. Body: " +
          DmExportStorageClient.redactSecrets(
            body.take(DmExportStorageClient.MaxLoggedErrorBodyChars))
        if (DmExportStorageClient.isAuthorizationFailure(status)) {
          throw new ExportStorageAuthorizationException(message)
        }
        if (DmExportStorageClient.isPermanentFailure(status)) {
          throw new PermanentExportStorageApiException(message)
        }
        honourRetryAfter(response, "Ingestion configuration API", attempt, GateMaxAttempts)
        throw new RuntimeException(message)
      }
      Try(DmExportStorageClient.parseIngestionConfiguration(objectMapper.readTree(body))) match {
        case Success(configuration) => configuration
        case Failure(exception) =>
          throw new RuntimeException(
            s"Failed to parse the ingestion configuration API response, id: $clientRequestId, " +
              s"${exception.getClass.getSimpleName}")
      }
    } finally {
      response.close()
    }
  }

  protected def fetchExportStorage(attempt: Int): ExportStorageTargets = {
    validateEndpoint()

    val clientRequestId = s"KSPARK.getExportStorage;${UUID.randomUUID()}"
    val request = buildJsonGetRequest(exportStorageUrl, clientRequestId, "export storage API")

    KDSU.logDebug(
      className,
      s"Calling export storage API $exportStorageUrl, id: $clientRequestId")

    val response = httpClient.sendSync(request, Context.NONE)
    try {
      val status = response.getStatusCode
      val body = readBody(response, "Export storage API")
      if (status == HttpStatus.NOT_FOUND) {
        throw new UnsupportedExportStorageApiException(
          s"$exportStorageUrl is not available on this cluster")
      }
      if (status != HttpStatus.OK) {
        val message = s"Export storage API returned HTTP $status. Body: " +
          DmExportStorageClient.redactSecrets(
            body.take(DmExportStorageClient.MaxLoggedErrorBodyChars))
        if (DmExportStorageClient.isAuthorizationFailure(status)) {
          throw new ExportStorageAuthorizationException(message)
        }
        if (DmExportStorageClient.isPermanentFailure(status)) {
          throw new PermanentExportStorageApiException(message)
        }
        honourRetryAfter(
          response,
          "Export storage API",
          attempt,
          DmExportStorageClient.MaxRetryAttempts)
        throw new RuntimeException(message)
      }
      Try(DmExportStorageClient.parseExportStorageResponse(objectMapper.readTree(body))) match {
        case Success(targets) => targets
        case Failure(exception) =>
          throw new RuntimeException(
            s"Failed to parse the export storage API response, id: $clientRequestId, " +
              s"${exception.getClass.getSimpleName}")
      }
    } finally {
      response.close()
    }
  }
}

private[kusto] class PermanentExportStorageApiException(message: String)
    extends RuntimeException(message)

private[kusto] class UnsupportedExportStorageApiException(message: String)
    extends PermanentExportStorageApiException(message)

private[kusto] class ExportStorageAuthorizationException(message: String)
    extends PermanentExportStorageApiException(message)

private[kusto] class UntrustedEndpointException(message: String)
    extends PermanentExportStorageApiException(message)

private[kusto] object DmExportStorageClient {
  private val MaxRetryAttempts = 3
  private[kusto] val DefaultRefreshInterval: Duration =
    Duration.ofSeconds(KustoConstants.StorageExpirySeconds.toLong)
  private[kusto] val TransientFailureRefreshInterval: Duration = Duration.ofSeconds(5)
  private val MinimumRefreshInterval = Duration.ofMinutes(10)

  private val MaxLoggedErrorBodyChars = 1024

  private[kusto] val MaxResponseBodyBytes = 8L * 1024 * 1024
  private val RequestTimeoutStatus = 408

  private val MaxRetryAfterSeconds = 5L
  private val QueryStringPattern = """\?[^\s"'<>]+""".r

  private[kusto] def redactSecrets(text: String): String =
    QueryStringPattern.replaceAllIn(text, "?<redacted>")
  private val ClientErrorStatusRange = 400 until 500
  private val RedirectStatusRange = 300 until 400

  private[kusto] def isPermanentFailure(status: Int): Boolean =
    RedirectStatusRange.contains(status) ||
      (ClientErrorStatusRange.contains(status) &&
        status != HttpStatus.TOO_MANY_REQS &&
        status != RequestTimeoutStatus)

  private[kusto] def isAuthorizationFailure(status: Int): Boolean =
    status == HttpStatus.UNAUTHORIZED || status == HttpStatus.FORBIDDEN

  private[kusto] def retryAfterMillis(
      header: Option[String],
      now: Instant = Instant.now()): Option[Long] =
    header.map(_.trim).filter(_.nonEmpty).flatMap { value =>
      Try(Math.multiplyExact(value.toLong, 1000L)).toOption
        .orElse(
          Try(
            ZonedDateTime
              .parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
              .toInstant
              .toEpochMilli - now.toEpochMilli).toOption)
        .filter(_ > 0)
        .map(waitMs => Math.min(waitMs, MaxRetryAfterSeconds * 1000L))
    }

  private[kusto] def effectiveRefreshInterval(refreshInterval: Option[Duration]): Duration =
    refreshInterval match {
      case Some(interval) if interval.compareTo(MinimumRefreshInterval) < 0 =>
        MinimumRefreshInterval
      case Some(interval) if interval.compareTo(DefaultRefreshInterval) > 0 =>
        DefaultRefreshInterval
      case Some(interval) => interval
      case None => DefaultRefreshInterval
    }

  private[kusto] def parseExportStorageResponse(root: JsonNode): ExportStorageTargets = {
    val exportStorage = field(root, "exportStorage")
      .filter(_.isObject)
      .getOrElse(throw new RuntimeException(
        "Export storage API response does not contain an 'exportStorage' object"))
    ExportStorageTargets(
      containers = paths(exportStorage, "containers"),
      lakeFolders = paths(exportStorage, "lakeFolders"))
  }

  private[kusto] def parseIngestionConfiguration(root: JsonNode): IngestionConfiguration = {
    val containerSettings = field(root, "containerSettings")
      .filter(_.isObject)
      .getOrElse(throw new RuntimeException(
        "Ingestion configuration API response does not contain a 'containerSettings' object"))
    val (refreshInterval, invalidRefreshInterval) =
      parseRefreshInterval(field(containerSettings, "refreshInterval"))
    IngestionConfiguration(
      lakeFolders = paths(containerSettings, "lakeFolders"),
      preferredUploadMethod = text(containerSettings, "preferredUploadMethod"),
      preferredIngestionMethod =
        field(root, "ingestionSettings").flatMap(text(_, "preferredIngestionMethod")),
      refreshInterval = refreshInterval,
      invalidRefreshInterval = invalidRefreshInterval)
  }

  private def parseRefreshInterval(value: Option[JsonNode]): (Option[Duration], Boolean) =
    value match {
      case None => (None, false)
      case Some(node) =>
        val parsed = Try {
          if (node.isIntegralNumber) {
            Duration.ofSeconds(node.longValue())
          } else if (node.isTextual) {
            new CslTimespanFormat(node.asText()).getValue
          } else {
            null
          }
        }.toOption.flatMap(Option(_)).filter(interval => !interval.isZero && !interval.isNegative)
        (parsed, parsed.isEmpty)
    }

  private def text(node: JsonNode, name: String): Option[String] =
    field(node, name).map(_.asText()).filter(StringUtils.isNotBlank)

  private def paths(node: JsonNode, arrayName: String): Seq[String] = {
    field(node, arrayName) match {
      case Some(array) if array.isArray =>
        array
          .elements()
          .asScala
          .flatMap(element => field(element, "path"))
          .map(_.asText())
          .filter(StringUtils.isNotBlank)
          .toSeq
      case _ => Seq.empty
    }
  }

  private def field(node: JsonNode, name: String): Option[JsonNode] = {
    if (node == null || node.isNull) {
      None
    } else {
      Option(node.get(name))
        .orElse(
          node
            .fieldNames()
            .asScala
            .find(_.equalsIgnoreCase(name))
            .flatMap(actual => Option(node.get(actual))))
        .filterNot(_.isNull)
    }
  }
}

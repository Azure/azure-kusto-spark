// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, PropertyAccessor}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.microsoft.azure.kusto.data.StringUtils
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils

import java.net.URI
import java.security.InvalidParameterException
import scala.util.matching.Regex
class TransientStorageParameters(
    val storageCredentials: scala.Array[TransientStorageCredentials],
    var endpointSuffix: String = KustoDataSourceUtils.DefaultDomainPostfix) {

  // C'tor for serialization
  def this() = {
    this(Array())
  }

  /**
   * Validate cross-credential invariants. Currently: refuse mixing OneLake credentials with
   * non-OneLake (Blob SAS/Key) credentials in the same array — read-back protocol coercion can
   * only honor one storage flavor per export.
   */
  def validate(): Unit = {
    if (storageCredentials != null && storageCredentials.length > 0) {
      // NB: do not call per-credential validate() here. parseOneLake() invokes per-cred
      // validate() for OneLake credentials, and historically blob credentials supplied
      // via fromString are not re-validated (they may legitimately have only sasUrl set
      // when ;impersonate auth is used). This wrapper only enforces cross-credential
      // invariants.
      val anyOneLake = storageCredentials.exists(c => c != null && c.isOneLake)
      val anyNonOneLake = storageCredentials.exists(c => c != null && !c.isOneLake)
      if (anyOneLake && anyNonOneLake) {
        throw new InvalidParameterException(
          "transientStorage cannot mix OneLake and non-OneLake credentials. " +
            "Provide either all OneLake URLs or all Azure Storage (blob) credentials.")
      }
    }
  }

  override def toString: String = {
    storageCredentials
      .map(tsc => tsc.toString)
      .mkString("[", System.lineSeparator(), s", domain: $endpointSuffix]")
  }

  def toInsecureString: String = {
    new ObjectMapper()
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(this)
  }

}

final case class TransientStorageCredentials() {
  var blobContainer: String = _
  var storageAccountName: String = _
  var storageAccountKey: String = _
  var sasKey: String = _

  // TODO next breaking - change to Option[String]
  var sasUrl: String = _
  var domainSuffix: String = _

  // OneLake transient storage support. When set, this credential targets a Fabric OneLake
  // (Lakehouse Files) location instead of an Azure Storage account. Kusto .export accepts
  // OneLake URIs with ;impersonate and Spark reads back over abfss using ambient AAD auth
  // already configured by the Fabric Spark runtime.
  //
  // `oneLakeUrl` is the only user-supplied OneLake field; the derived fields below
  // (oneLakeWorkspace, oneLakeEndpoint, oneLakeArtifactPath) are always recomputed from
  // `oneLakeUrl` and are marked @JsonIgnore so untrusted JSON cannot inject inconsistent
  // values.
  var oneLakeUrl: String = _
  @JsonIgnore var oneLakeWorkspace: String = _
  @JsonIgnore var oneLakeEndpoint: String = _
  @JsonIgnore var oneLakeArtifactPath: String = _

  // Explicit storage kind selector (JSON key "type"): "onelake" or "blob". When set it is
  // authoritative; otherwise the kind is inferred from the populated field (oneLakeUrl => OneLake).
  // The URL is then only validated, never used to classify.
  @JsonProperty("type") var storageType: String = _

  @JsonIgnore
  private def normalizedType: String = if (storageType == null) "" else storageType.trim

  @JsonIgnore
  def declaredOneLake: Boolean =
    if (normalizedType.nonEmpty) normalizedType.equalsIgnoreCase("onelake")
    else StringUtils.isNotBlank(oneLakeUrl)

  @JsonIgnore
  def hasUnknownType: Boolean =
    normalizedType.nonEmpty && !normalizedType.equalsIgnoreCase("onelake") &&
      !normalizedType.equalsIgnoreCase("blob")

  def this(storageAccountName: String, storageAccountKey: String, blobContainer: String) = {
    this()
    this.blobContainer = blobContainer
    this.storageAccountName = storageAccountName
    this.storageAccountKey = storageAccountKey
    validate()
  }

  def this(sas: String) = {
    this()
    sasUrl = sas
    if (TransientStorageCredentials.isOneLakeUrl(sas)) {
      parseOneLake(sas)
    } else {
      parseSas(sas)
    }
  }

  @JsonIgnore
  def isOneLake: Boolean =
    StringUtils.isNotBlank(oneLakeUrl) &&
      StringUtils.isNotBlank(oneLakeWorkspace) &&
      StringUtils.isNotBlank(oneLakeEndpoint) &&
      StringUtils.isNotBlank(oneLakeArtifactPath)

  @JsonIgnore
  def authMethod: AuthMethod.AuthMethod = {
    if (isOneLake) {
      AuthMethod.Impersonation
    } else {
      sasKey match {
        case null => AuthMethod.Key
        case TransientStorageParameters.ImpersonationString => AuthMethod.Impersonation
        case _ => AuthMethod.Sas
      }
    }
  }

  /**
   * Canonical abfss base URL for OneLake credentials, of the form
   * abfss://{workspace}@{endpoint}/{artifactPath}. Returns null for non-OneLake credentials.
   * Spark reads back parquet output via this URL.
   */
  @JsonIgnore
  def oneLakeAbfssBase: String = {
    if (!isOneLake) null
    else if (oneLakeArtifactPath == null || oneLakeArtifactPath.isEmpty) {
      s"abfss://$oneLakeWorkspace@$oneLakeEndpoint"
    } else {
      s"abfss://$oneLakeWorkspace@$oneLakeEndpoint/$oneLakeArtifactPath"
    }
  }

  def validate(): Unit = {
    if (isOneLake) {
      // Defense in depth: reject any blob-flavor field that would conflict with OneLake.
      val conflictingFields = Seq(
        ("storageAccountName", storageAccountName),
        ("storageAccountKey", storageAccountKey),
        ("blobContainer", blobContainer),
        ("sasKey", sasKey)).collect {
        case (name, value) if StringUtils.isNotBlank(value) => name
      }
      if (conflictingFields.nonEmpty) {
        throw new InvalidParameterException(
          s"OneLake credential cannot also specify blob storage fields: ${conflictingFields.mkString(", ")}")
      }
      // `sasUrl` may legitimately be the OneLake URL (input alias); only reject if it
      // points at a non-OneLake account.
      if (StringUtils.isNotBlank(sasUrl) &&
        !TransientStorageCredentials.isOneLakeUrl(sasUrl)) {
        throw new InvalidParameterException(
          "OneLake credential's sasUrl, if provided, must itself be a OneLake URL")
      }
    } else if (StringUtils.isNotBlank(oneLakeUrl)) {
      // oneLakeUrl was set but parsing failed to produce derived fields — reject loudly.
      throw new InvalidParameterException(
        s"oneLakeUrl is not a recognized Fabric OneLake URL: $oneLakeUrl")
    } else
      authMethod match {
        case AuthMethod.Sas =>
          if (sasUrl.isEmpty) throw new InvalidParameterException("sasUrl is null or empty")
        case AuthMethod.Key =>
          if (StringUtils.isBlank(storageAccountName)) {
            throw new InvalidParameterException("storageAccount name is null or empty")
          }
          if (StringUtils.isBlank(storageAccountKey)) {
            throw new InvalidParameterException("storageAccount key is null or empty")
          }
          if (StringUtils.isBlank(blobContainer)) {
            throw new InvalidParameterException("blob container name is null or empty")
          }
        case _ =>
      }
  }

  private[kusto] def parseSas(url: String): Unit = {
    url match {
      case TransientStorageCredentials.SasPattern(
            storageAccountName,
            maybeZone,
            cloud,
            container,
            sasKey) =>
        this.storageAccountName = storageAccountName + (if (maybeZone == null) "" else maybeZone)
        this.blobContainer = container
        this.sasKey = sasKey
        domainSuffix = cloud
      case _ =>
        throw new InvalidParameterException(
          "SAS url couldn't be parsed. Should be https://<storage-account>.blob.<domainEndpointSuffix>/<container>?<SAS-Token>")
    }
  }

  private[kusto] def parseOneLake(url: String): Unit = {
    val trimmedSuffix = url.stripSuffix(TransientStorageParameters.ImpersonationString)
    val parsed =
      try new java.net.URI(trimmedSuffix)
      catch {
        case e: java.net.URISyntaxException =>
          throw new InvalidParameterException(
            s"OneLake URL couldn't be parsed (malformed): $url (${e.getMessage})")
      }

    val scheme = Option(parsed.getScheme).map(_.toLowerCase).getOrElse("")
    val host = parsed.getHost
    val rawPath = Option(parsed.getRawPath).getOrElse("")
    val port = parsed.getPort

    if (parsed.getRawQuery != null) {
      throw new InvalidParameterException(
        s"OneLake URL must not include a query string (no SAS/secrets are supported here): $url")
    }
    if (parsed.getRawFragment != null) {
      throw new InvalidParameterException(s"OneLake URL must not include a fragment: $url")
    }
    if (port != -1) {
      throw new InvalidParameterException(s"OneLake URL must not include an explicit port: $url")
    }
    if (rawPath.contains("/../") || rawPath.contains("/./") ||
      rawPath.endsWith("/..") || rawPath.endsWith("/.")) {
      throw new InvalidParameterException(s"OneLake URL path must not contain '..' or '.': $url")
    }

    val (endpoint, workspace, artifactPath) = scheme match {
      case "abfss" | "abfs" =>
        val ws = parsed.getUserInfo
        if (StringUtils.isBlank(ws) || StringUtils.isBlank(host) || rawPath.length <= 1) {
          throw new InvalidParameterException(
            s"OneLake abfss URL must be 'abfss://<workspace>@<endpoint>/<artifact>/Files/...': $url")
        }
        (host, ws, rawPath.stripPrefix("/").stripSuffix("/"))
      case "https" =>
        if (parsed.getUserInfo != null) {
          throw new InvalidParameterException(
            s"OneLake https URL must not contain userInfo: $url")
        }
        if (StringUtils.isBlank(host) || rawPath.length <= 1) {
          throw new InvalidParameterException(
            s"OneLake https URL must be 'https://<endpoint>/<workspace>/<artifact>/Files/...': $url")
        }
        val parts = rawPath.stripPrefix("/").stripSuffix("/").split("/", 2)
        if (parts.length < 2 || parts(0).isEmpty || parts(1).isEmpty) {
          throw new InvalidParameterException(
            s"OneLake https URL must include workspace and artifact path: $url")
        }
        (host, parts(0), parts(1))
      case other =>
        throw new InvalidParameterException(
          s"Unsupported scheme '$other' for OneLake URL (expected 'https' or 'abfss'): $url")
    }

    // Trust the user-supplied OneLake host. Only require a OneLake service label, reject Azure Storage hosts, and
    // reject IP literals / localhost / single-label hosts.
    val hostLower = endpoint.toLowerCase
    if (!TransientStorageParameters.isOneLakeHost(endpoint, scheme) ||
      hostLower == "localhost" || !hostLower.contains(".") ||
      TransientStorageParameters.isIpLiteral(hostLower)) {
      throw new InvalidParameterException(
        s"oneLakeUrl is not a recognized Fabric OneLake URL: $url")
    }

    // Enforce Lakehouse Files path shape: '<artifact>/Files/<subpath>' with non-empty
    // segments and no consecutive slashes. This blocks accidental targeting of Tables/
    // (which would clash with Delta semantics) and catches typos early.
    val artifactSegments = artifactPath.split("/", -1)
    if (artifactSegments.exists(_.isEmpty)) {
      throw new InvalidParameterException(
        s"OneLake artifact path must not contain empty segments: $url")
    }
    if ((workspace +: artifactSegments).exists { seg =>
        val decoded = java.net.URLDecoder.decode(seg, "UTF-8")
        decoded == "." || decoded == ".." || decoded.contains("/") || decoded.contains("\\")
      }) {
      throw new InvalidParameterException(s"OneLake URL path must not contain '..' or '.': $url")
    }
    if (artifactSegments.length < 3 || !artifactSegments(1).equalsIgnoreCase("Files")) {
      throw new InvalidParameterException(
        s"OneLake artifact path must be '<lakehouse>(.Lakehouse)?/Files/<subpath>': $url")
    }

    this.oneLakeEndpoint = endpoint
    this.oneLakeWorkspace = workspace
    this.oneLakeArtifactPath = artifactPath
    // Always store the canonical https form. Kusto .export has been verified to accept
    // 'https://<onelake-host>/<workspace>/<artifact-path>'; abfss is converted here so
    // CSL emission stays uniform.
    this.oneLakeUrl = s"https://$endpoint/$workspace/$artifactPath"
    validate()
  }

  override def toString: String = {
    // TODO next breaking - change to "authMethod:"
    if (isOneLake) {
      s"OneLake workspace: $oneLakeWorkspace, endpoint: $oneLakeEndpoint, artifactPath: $oneLakeArtifactPath"
    } else {
      s"BlobContainer: $blobContainer ,Storage: $storageAccountName , IsSasKeyDefined: ${authMethod == AuthMethod.Sas}"
    }
  }

}

object TransientStorageParameters {
  val ImpersonationString = ";impersonate"

  // Cloud-agnostic OneLake host detection that trusts any cloud/domain (like the blob path trusts
  // any *.blob.<domain>): a supported scheme on a host carrying a Fabric OneLake service label
  // (.dfs. / .onelake. / .blob.fabric.). These are product-level labels, stable across sovereign
  // clouds; no cloud-specific domains are hardcoded. Matched on label boundaries (leading dot) so
  // unrelated hosts like "notonelake.example.com" are not treated as OneLake.
  private[kusto] def isOneLakeHost(host: String, scheme: String): Boolean = {
    if (StringUtils.isBlank(host)) return false
    val h = host.toLowerCase
    val supportedScheme = scheme.toLowerCase match {
      case "https" | "abfss" | "abfs" => true
      case _ => false
    }
    supportedScheme &&
    (h.contains(".dfs.") || h.contains(".onelake.") || h.contains(".blob.fabric."))
  }

  private[kusto] def isIpLiteral(host: String): Boolean =
    host.startsWith("[") || host.matches("""\d{1,3}(\.\d{1,3}){3}""")

  private[kusto] def fromString(json: String): TransientStorageParameters = {
    val params = new ObjectMapper()
      .registerModule(new JavaTimeModule())
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .readValue(json, classOf[TransientStorageParameters])

    // Classify each credential explicitly (by `type` or the populated field), never by sniffing
    // the URL. For OneLake, re-derive the fields from the user-supplied URL so attacker-controlled
    // JSON cannot pre-populate `oneLakeWorkspace`/`oneLakeEndpoint`/`oneLakeArtifactPath` to a
    // different target than the URL. The derived fields are also @JsonIgnore for defense in depth.
    if (params.storageCredentials != null) {
      params.storageCredentials.foreach { cred =>
        if (cred != null) {
          // Reset any user-supplied derived fields before parsing.
          cred.oneLakeWorkspace = null
          cred.oneLakeEndpoint = null
          cred.oneLakeArtifactPath = null

          if (cred.declaredOneLake) {
            val sourceUrl =
              if (StringUtils.isNotBlank(cred.oneLakeUrl)) cred.oneLakeUrl
              else cred.sasUrl
            if (StringUtils.isBlank(sourceUrl)) {
              throw new InvalidParameterException(
                "OneLake transientStorage requires a 'oneLakeUrl' (or 'sasUrl') value")
            }
            cred.parseOneLake(sourceUrl)
          } else if (cred.hasUnknownType) {
            throw new InvalidParameterException(
              s"Unknown transientStorage type '${cred.storageType}' (expected 'onelake' or 'blob')")
          } else if (StringUtils.isNotBlank(cred.oneLakeUrl)) {
            throw new InvalidParameterException(
              "transientStorage type 'blob' cannot specify a oneLakeUrl")
          }
        }
      }
    }
    params.validate()
    params
  }
}

object TransientStorageCredentials {
  val SasPattern: Regex =
    raw"https:\/\/([^.]+)(\.[^.]+)?\.blob\.([^\/]+)\/([^?]+)(;impersonate|[\?].+)".r

  /**
   * Detect whether the given storage URL targets a Fabric OneLake location. Trusts any
   * cloud/domain (like the blob path trusts any *.blob.<domain>): https/abfss scheme on a host
   * carrying a Fabric OneLake service label (.dfs. / .onelake. / .blob.fabric.). Real Azure blob
   * hosts (*.blob.core.*) lack that label and so remain blob. This is detection only;
   * parseOneLake performs the authoritative validation.
   */
  def isOneLakeUrl(url: String): Boolean = {
    if (url == null || url.isEmpty) return false
    val trimmed = url.stripSuffix(TransientStorageParameters.ImpersonationString)
    val q = trimmed.indexOf('?')
    val base = if (q >= 0) trimmed.substring(0, q) else trimmed
    try {
      val uri = new URI(base)
      val scheme = Option(uri.getScheme).map(_.toLowerCase).getOrElse("")
      val host = Option(uri.getHost).getOrElse("")
      TransientStorageParameters.isOneLakeHost(host, scheme)
    } catch {
      case _: Throwable => false
    }
  }
}

object AuthMethod extends Enumeration {
  type AuthMethod = Value
  val Sas, Key, Impersonation = Value
}

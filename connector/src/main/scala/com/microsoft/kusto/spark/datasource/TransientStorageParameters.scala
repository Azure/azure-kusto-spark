// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.{JsonIgnore, PropertyAccessor}
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
      if (storageCredentials.exists(_ == null)) {
        throw new InvalidParameterException(
          "transientStorage.storageCredentials must not contain null entries")
      }
      // NB: do not call per-credential validate() here. parseOneLake() invokes per-cred
      // validate() for OneLake credentials, and historically blob credentials supplied
      // via fromString are not re-validated (they may legitimately have only sasUrl set
      // when ;impersonate auth is used). This wrapper only enforces cross-credential
      // invariants.
      val anyOneLake = storageCredentials.exists(c => c.isOneLake)
      val anyNonOneLake = storageCredentials.exists(c => !c.isOneLake)
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
    parseSas(sas)
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
      // sasUrl is for blob/ADLS2 only — reject if set on a OneLake credential.
      if (StringUtils.isNotBlank(sasUrl)) {
        throw new InvalidParameterException(
          "OneLake credential must not specify a sasUrl (use oneLakeUrl instead)")
      }
    } else if (StringUtils.isNotBlank(oneLakeUrl)) {
      // oneLakeUrl was set but parsing failed to produce derived fields.
      // Attempt parsing now rather than rejecting — supports any valid OneLake-like host.
      parseOneLake(oneLakeUrl)
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
    val decodedPath = Option(parsed.getPath).getOrElse("")
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
    if (decodedPath.contains("/../") || decodedPath.contains("/./") ||
      decodedPath.endsWith("/..") || decodedPath.endsWith("/.") ||
      decodedPath == ".." || decodedPath == ".") {
      throw new InvalidParameterException(s"OneLake URL path must not contain '..' or '.': $url")
    }

    val (endpoint, workspace, artifactPath) = scheme match {
      case "abfss" | "abfs" =>
        val ws = parsed.getUserInfo
        if (StringUtils.isBlank(ws) || ws.contains(":") || StringUtils.isBlank(host) || rawPath.length <= 1) {
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

    // Lightweight host validation: host must contain a known OneLake-like segment
    // (dfs, blob, or onelake) to guard against completely unrelated URLs being treated as OneLake.
    // Also reject IP-literal, localhost, and single-label hosts for security.
    val hostLower = endpoint.toLowerCase
    if (!hostLower.contains(".dfs.") && !hostLower.contains(".blob.") && !hostLower.contains(
        ".onelake.") &&
      !hostLower.contains("onelake")) {
      throw new InvalidParameterException(
        s"OneLake URL host '$endpoint' is not a recognized Fabric OneLake host")
    }
    if (hostLower == "localhost" || !hostLower.contains(".") ||
      hostLower.startsWith("[") || hostLower.matches("""\d{1,3}(\.\d{1,3}){3}""")) {
      throw new InvalidParameterException(
        s"OneLake URL host must not be localhost, an IP literal, or a single-label host: $url")
    }

    // Enforce Lakehouse Files path shape: '<artifact>/Files/<subpath>' with non-empty
    // segments and no consecutive slashes. This blocks accidental targeting of Tables/
    // (which would clash with Delta semantics) and catches typos early.
    val artifactSegments = artifactPath.split("/", -1)
    if (artifactSegments.exists(_.isEmpty)) {
      throw new InvalidParameterException(
        s"OneLake artifact path must not contain empty segments: $url")
    }
    // URL-decoded traversal check on workspace and all artifact segments
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

  // Known OneLake host suffixes. Matched with endsWith on the lowercased host so we
  // don't false-positive on e.g. "foo.blob.fabric.microsoft.test.com".
  private[kusto] val OneLakeHostSuffixes: Array[String] = Array(
    ".dfs.fabric.microsoft.com",
    ".dfs.fabric.microsoft.us",
    ".blob.fabric.microsoft.com",
    ".blob.fabric.microsoft.us",
    ".onelake.fabric.microsoft.com",
    ".onelake.fabric.microsoft.us",
    ".dfs.pbidedicated.windows-int.net")

  private[kusto] def fromString(json: String): TransientStorageParameters = {
    val params = new ObjectMapper()
      .registerModule(new JavaTimeModule())
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .configure(
        com.fasterxml.jackson.databind.MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES,
        true)
      .readValue(json, classOf[TransientStorageParameters])

    // Always re-derive OneLake fields from the user-supplied URL so attacker-controlled
    // JSON cannot pre-populate `oneLakeWorkspace`/`oneLakeEndpoint`/`oneLakeArtifactPath`
    // to a different target than `oneLakeUrl`. The derived fields are also @JsonIgnore on
    // the credential class for defense in depth.
    if (params.storageCredentials != null) {
      params.storageCredentials.foreach { cred =>
        if (cred != null) {
          // Reset any user-supplied derived fields before parsing.
          cred.oneLakeWorkspace = null
          cred.oneLakeEndpoint = null
          cred.oneLakeArtifactPath = null

          // OneLake is only triggered by the explicit oneLakeUrl field.
          // sasUrl remains exclusively for blob/ADLS2 storage.
          if (StringUtils.isNotBlank(cred.oneLakeUrl)) {
            cred.parseOneLake(cred.oneLakeUrl)
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
   * Detect whether the given storage URL targets a Fabric OneLake location. Recognizes the
   * following host suffixes (matched with endsWith on lowercased host) for both https:// and
   * abfss:// schemes:
   *   - *.dfs.fabric.microsoft.com (e.g. onelake.dfs.fabric.microsoft.com)
   *   - *.blob.fabric.microsoft.com
   *   - *.onelake.fabric.microsoft.com
   */
  def isOneLakeUrl(url: String): Boolean = {
    if (url == null || url.isEmpty) return false
    val trimmed = url.stripSuffix(TransientStorageParameters.ImpersonationString)
    val q = trimmed.indexOf('?')
    val base = if (q >= 0) trimmed.substring(0, q) else trimmed
    try {
      val uri = new URI(base)
      val scheme = Option(uri.getScheme).map(_.toLowerCase).getOrElse("")
      val host = Option(uri.getHost).map(_.toLowerCase).getOrElse("")
      val supportedScheme = scheme == "https" || scheme == "abfss" || scheme == "abfs"
      val supportedHost = TransientStorageParameters.OneLakeHostSuffixes.exists(host.endsWith)
      supportedScheme && supportedHost
    } catch {
      case _: Throwable => false
    }
  }
}

object AuthMethod extends Enumeration {
  type AuthMethod = Value
  val Sas, Key, Impersonation = Value
}

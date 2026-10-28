// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.azure.core.credential.{AccessToken, TokenCredential, TokenRequestContext}
import com.azure.identity.{
  ClientCertificateCredentialBuilder,
  ClientSecretCredentialBuilder,
  ManagedIdentityCredentialBuilder
}
import com.microsoft.azure.kusto.data.auth.CloudInfo
import com.microsoft.azure.kusto.data.auth.endpoints.KustoTrustedEndpoints
import com.microsoft.azure.kusto.data.exceptions.{DataClientException, DataServiceException}
import com.microsoft.kusto.spark.authentication._
import com.microsoft.kusto.spark.utils.{KeyVaultUtils, KustoDataSourceUtils => KDSU}
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.kusto.spark.datasink.StorageTokenProvider
import reactor.core.publisher.Mono

import java.nio.charset.StandardCharsets
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.Base64
import scala.util.Try

/**
 * Self-contained authentication module for the ingest-v2 SDK path. Converts KustoAuthentication
 * instances directly to azure-identity TokenCredential objects required by the kusto-ingest-v2
 * SDK.
 *
 * This is intentionally a standalone implementation (not a bridge) so that the old v1
 * authentication flow can be removed independently in the future.
 */
object IngestV2Authentication {
  private val myName = this.getClass.getSimpleName

  /**
   * @param dmUrl
   *   the ingestion endpoint. Like the .NET Kusto.Ingest.V2 client, every credential first checks
   *   that this endpoint is trusted, and credentials that ask Entra ID for a token request the
   *   Kusto resource published by the endpoint (see [[IngestV2Audience]]) instead of the scope
   *   "dmUrl/.default" that the ingest-v2 SDK asks for. Tokens supplied by the user (raw token or
   *   callback) are passed through as they are and must already be for that resource.
   */
  def createTokenCredential(
      authentication: KustoAuthentication,
      dmUrl: String): TokenCredential = {
    KDSU.logDebug(
      myName,
      s"Creating TokenCredential from auth type: ${authentication.getClass.getSimpleName}")

    authentication match {
      case app: AadApplicationAuthentication =>
        new KustoResourceCredential(
          new ClientSecretCredentialBuilder()
            .clientId(app.ID)
            .clientSecret(app.password)
            .tenantId(app.authority)
            .build(),
          dmUrl)

      case app: AadApplicationCertificateAuthentication =>
        new KustoResourceCredential(
          new ClientCertificateCredentialBuilder()
            .clientId(app.appId)
            .pfxCertificate(app.certFilePath, app.certPassword)
            .tenantId(app.authority)
            .build(),
          dmUrl)

      case mi: ManagedIdentityAuthentication =>
        val builder = new ManagedIdentityCredentialBuilder()
        mi.clientId.foreach(id => builder.clientId(id))
        new KustoResourceCredential(builder.build(), dmUrl)

      case token: KustoAccessTokenAuthentication =>
        new KustoResourceCredential(
          new StaticTokenCredential(token.token),
          dmUrl,
          rescope = false)

      case provider: KustoTokenProviderAuthentication =>
        new KustoResourceCredential(
          new CallbackTokenCredential(provider.tokenProviderCallback),
          dmUrl,
          rescope = false)

      case kv: KeyVaultAppAuthentication =>
        val app = KeyVaultUtils.getAadAppParametersFromKeyVault(kv)
        new KustoResourceCredential(
          new ClientSecretCredentialBuilder()
            .clientId(app.ID)
            .clientSecret(app.password)
            .tenantId(app.authority)
            .build(),
          dmUrl)

      case _ =>
        KDSU.logError(
          myName,
          s"Unsupported auth type for ingest-v2: ${authentication.getClass.getSimpleName}")
        throw new UnsupportedOperationException(
          s"Authentication type ${authentication.getClass.getSimpleName} is not supported with ingest-v2 SDK")
    }
  }
}

/**
 * Builds AccessTokens with the real expiry of the token. azure-core caches tokens until shortly
 * before `expiresAt`, so a made-up expiry would keep serving a stale token. When the token is not
 * a readable JWT, it is reported as already expiring so that it is fetched again for every
 * request.
 */
private[v2] object AccessTokens {
  private val mapper = new ObjectMapper()

  def fromRawToken(token: String): AccessToken =
    new AccessToken(token, expiryOf(token).getOrElse(OffsetDateTime.now(ZoneOffset.UTC)))

  private[v2] def expiryOf(token: String): Option[OffsetDateTime] = {
    Try {
      val parts = token.split('.')
      val payload = new String(Base64.getUrlDecoder.decode(parts(1)), StandardCharsets.UTF_8)
      val exp = mapper.readTree(payload).get("exp")
      if (exp != null && exp.canConvertToLong) {
        Some(OffsetDateTime.ofInstant(Instant.ofEpochSecond(exp.asLong()), ZoneOffset.UTC))
      } else None
    }.toOption.flatten
  }
}

/**
 * Finds the token audience of an ingestion endpoint the way the .NET Kusto.Ingest.V2 client does
 * (IngestHttpClient.AuthenticateAsync with AuthenticationHelpers.TrustKustoUriOrThrow):
 *   - read the endpoint metadata (`/v1/rest/auth/metadata`, shared CloudInfo cache);
 *   - when the endpoint answers with an error status or without AAD details, use the public cloud
 *     defaults; when it cannot be reached at all, fail;
 *   - reject endpoints that are not trusted Kusto endpoints (same rule and overrides as
 *     KustoTrustedEndpoints, which the Java client already applies on the V1 path);
 *   - audience = KustoServiceResourceId, or "https://kusto.kusto.windows.net" when it is empty.
 *     Unlike CloudInfo.determineScope, the audience is not rewritten for MFA, matching .NET.
 */
private[v2] object IngestV2Audience {
  private val myName = this.getClass.getSimpleName

  def resolve(dmUrl: String): String = {
    val info = cloudInfo(dmUrl)
    KustoTrustedEndpoints.validateTrustedEndpoint(dmUrl, info.getLoginEndpoint)
    // CloudInfo stores a missing resource as "" where .NET has null.
    Option(info.getKustoServiceResourceId)
      .filter(_.nonEmpty)
      .getOrElse(CloudInfo.DEFAULT_KUSTO_SERVICE_RESOURCE_ID)
  }

  // Same as .NET ExtendedString.Join("/", audience, ".default"): the audience is kept as is.
  def scopeOf(audience: String): String = s"$audience/.default"

  // Text CloudInfo (kusto-data) puts in the error for a non-success HTTP status.
  private val ErrorStatusMessage = "Error in metadata endpoint, got code: "

  private def cloudInfo(dmUrl: String): CloudInfo = {
    try {
      CloudInfo.retrieveCloudInfoForCluster(dmUrl)
    } catch {
      // CloudInfo reports an error status (or an empty body) from the endpoint as a
      // DataServiceException without a cause. A 429 is retried first and, if it persists, is
      // rethrown as a DataClientException carrying the original message. Network failures,
      // timeouts and unreadable bodies are rethrown, as in .NET.
      case e: DataServiceException if e.getCause == null => useDefaults(dmUrl, e)
      case e: DataClientException
          if e.getCause == null && Option(e.getMessage).exists(_.contains(ErrorStatusMessage)) =>
        useDefaults(dmUrl, e)
    }
  }

  private def useDefaults(dmUrl: String, e: Exception): CloudInfo = {
    KDSU.logWarn(
      myName,
      s"Could not read authentication metadata of '$dmUrl', using the public cloud defaults: ${e.getMessage}")
    CloudInfo.DEFAULT_CLOUD
  }
}

/**
 * Checks once that the ingestion endpoint is trusted and, when `rescope` is set, requests tokens
 * for the endpoint audience (see [[IngestV2Audience]]) whatever scope the SDK asks for. Claims
 * and tenant of the request are kept. The audience is resolved on first use and then reused, like
 * the .NET client; a failed resolution is retried on the next request.
 */
private[v2] class KustoResourceCredential(
    inner: TokenCredential,
    dmUrl: String,
    rescope: Boolean = true)
    extends TokenCredential {
  private lazy val scope: String = IngestV2Audience.scopeOf(IngestV2Audience.resolve(dmUrl))

  private def scopedRequest(request: TokenRequestContext): TokenRequestContext = {
    val resolved = scope
    if (!rescope) {
      Option(request).getOrElse(new TokenRequestContext())
    } else {
      val scoped = new TokenRequestContext().addScopes(resolved)
      if (request != null) {
        scoped.setClaims(request.getClaims).setTenantId(request.getTenantId)
      }
      scoped
    }
  }

  override def getToken(request: TokenRequestContext): Mono[AccessToken] =
    Mono.fromCallable(() => scopedRequest(request)).flatMap(r => inner.getToken(r))

  override def getTokenSync(request: TokenRequestContext): AccessToken =
    inner.getTokenSync(scopedRequest(request))
}

/** TokenCredential for a token supplied by the user. */
private[v2] class StaticTokenCredential(token: String) extends TokenCredential {
  override def getToken(request: TokenRequestContext): Mono[AccessToken] =
    Mono.fromCallable(() => getTokenSync(request))

  override def getTokenSync(request: TokenRequestContext): AccessToken =
    AccessTokens.fromRawToken(token)
}

/** TokenCredential that delegates to a user-provided callback. */
private[v2] class CallbackTokenCredential(callback: java.util.concurrent.Callable[String])
    extends TokenCredential {
  override def getToken(request: TokenRequestContext): Mono[AccessToken] =
    Mono.fromCallable(() => getTokenSync(request))

  override def getTokenSync(request: TokenRequestContext): AccessToken =
    AccessTokens.fromRawToken(callback.call())
}

/** TokenCredential backed by a StorageTokenProvider (OneLake uploads). */
private[v2] class StorageProviderTokenCredential(provider: StorageTokenProvider)
    extends TokenCredential {
  override def getToken(request: TokenRequestContext): Mono[AccessToken] =
    Mono.fromCallable(() => getTokenSync(request))

  override def getTokenSync(request: TokenRequestContext): AccessToken =
    AccessTokens.fromRawToken(provider.getToken)
}

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
   *   the ingestion endpoint. Credentials that ask Entra ID for a token request the Kusto
   *   resource published in its metadata (`/v1/rest/auth/metadata`), like the existing Java and
   *   .NET clients, instead of the scope "dmUrl/.default" that the ingest-v2 SDK asks for.
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
        new StaticTokenCredential(token.token)

      case provider: KustoTokenProviderAuthentication =>
        new CallbackTokenCredential(provider.tokenProviderCallback)

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
 * Requests tokens for the Kusto resource of the endpoint (from its cached cloud metadata),
 * whatever scope the SDK asks for. Claims and tenant of the request are kept.
 */
private[v2] class KustoResourceCredential(inner: TokenCredential, dmUrl: String)
    extends TokenCredential {
  private def scopedRequest(request: TokenRequestContext): TokenRequestContext = {
    val scoped = new TokenRequestContext()
      .addScopes(CloudInfo.retrieveCloudInfoForCluster(dmUrl).determineScope())
    if (request != null) {
      scoped.setClaims(request.getClaims).setTenantId(request.getTenantId)
    }
    scoped
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

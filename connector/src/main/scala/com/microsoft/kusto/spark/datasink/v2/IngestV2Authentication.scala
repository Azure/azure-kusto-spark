// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.azure.core.credential.{AccessToken, TokenCredential, TokenRequestContext}
import com.azure.identity.{
  ClientCertificateCredentialBuilder,
  ClientSecretCredentialBuilder,
  ManagedIdentityCredentialBuilder
}
import com.microsoft.kusto.spark.authentication._
import com.microsoft.kusto.spark.utils.KeyVaultUtils
import reactor.core.publisher.Mono

import java.time.OffsetDateTime

/**
 * Self-contained authentication module for the ingest-v2 SDK path. Converts KustoAuthentication
 * instances directly to azure-identity TokenCredential objects required by the kusto-ingest-v2
 * SDK.
 *
 * This is intentionally a standalone implementation (not a bridge) so that the old v1
 * authentication flow can be removed independently in the future.
 */
object IngestV2Authentication {

  def createTokenCredential(authentication: KustoAuthentication): TokenCredential = {
    authentication match {
      case app: AadApplicationAuthentication =>
        new ClientSecretCredentialBuilder()
          .clientId(app.ID)
          .clientSecret(app.password)
          .tenantId(app.authority)
          .build()

      case app: AadApplicationCertificateAuthentication =>
        new ClientCertificateCredentialBuilder()
          .clientId(app.appId)
          .pfxCertificate(app.certFilePath, app.certPassword)
          .tenantId(app.authority)
          .build()

      case mi: ManagedIdentityAuthentication =>
        val builder = new ManagedIdentityCredentialBuilder()
        mi.clientId.foreach(id => builder.clientId(id))
        builder.build()

      case token: KustoAccessTokenAuthentication =>
        new StaticTokenCredential(token.token)

      case provider: KustoTokenProviderAuthentication =>
        new CallbackTokenCredential(provider.tokenProviderCallback)

      case kv: KeyVaultAppAuthentication =>
        val app = KeyVaultUtils.getAadAppParametersFromKeyVault(kv)
        new ClientSecretCredentialBuilder()
          .clientId(app.ID)
          .clientSecret(app.password)
          .tenantId(app.authority)
          .build()

      case _ =>
        throw new UnsupportedOperationException(
          s"Authentication type ${authentication.getClass.getSimpleName} is not supported with ingest-v2 SDK")
    }
  }
}

/**
 * TokenCredential that returns a static access token. Used when the user provides a pre-obtained
 * token.
 */
private[v2] class StaticTokenCredential(token: String) extends TokenCredential {
  override def getToken(request: TokenRequestContext): Mono[AccessToken] = {
    Mono.just(new AccessToken(token, OffsetDateTime.now().plusHours(1)))
  }

  override def getTokenSync(request: TokenRequestContext): AccessToken = {
    new AccessToken(token, OffsetDateTime.now().plusHours(1))
  }
}

/**
 * TokenCredential that delegates to a user-provided callback (Callable[String]) for token
 * acquisition.
 */
private[v2] class CallbackTokenCredential(callback: java.util.concurrent.Callable[String])
    extends TokenCredential {
  override def getToken(request: TokenRequestContext): Mono[AccessToken] = {
    Mono.fromCallable(() => getTokenSync(request))
  }

  override def getTokenSync(request: TokenRequestContext): AccessToken = {
    val token = callback.call()
    new AccessToken(token, OffsetDateTime.now().plusHours(1))
  }
}

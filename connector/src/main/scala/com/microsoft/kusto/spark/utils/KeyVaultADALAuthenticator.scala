package com.microsoft.kusto.spark.utils

import com.azure.core.http.policy.{HttpLogDetailLevel, HttpLogOptions}
import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.security.keyvault.secrets.{SecretClient, SecretClientBuilder}

/**
 * Authenticates to Azure Key Vault by providing a callback to authenticate
 * using ADAL.
 */
class KeyVaultADALAuthenticator(uri: String, clientId: String, clientKey: String, authority: String) {
  val authorityId: String = if (authority == null) "72f988bf-86f1-41af-91ab-2d7cd011db47" else authority

  def getAuthenticatedClient: SecretClient = {
    new SecretClientBuilder()
      .credential(new ClientSecretCredentialBuilder()
        .clientId(clientId)
        .clientSecret(clientKey)
        .tenantId(authorityId)
        .build())
      .vaultUrl(uri)
      .httpLogOptions(new HttpLogOptions().setLogLevel(HttpLogDetailLevel.BODY_AND_HEADERS)).buildClient
  }
}

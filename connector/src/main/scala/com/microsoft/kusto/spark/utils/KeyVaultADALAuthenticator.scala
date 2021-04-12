package com.microsoft.kusto.spark.utils

import com.azure.core.http.policy.{HttpLogDetailLevel, HttpLogOptions}
import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.security.keyvault.secrets.{SecretClient, SecretClientBuilder}
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils.DefaultMicrosoftTenant

/**
 * Authenticates to Azure Key Vault by providing a callback to authenticate
 * using ADAL.
 */
class KeyVaultADALAuthenticator(uri: String, clientId: String, clientKey: String, authority: String) {
  val authorityId: String = if (authority == null) DefaultMicrosoftTenant else authority

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

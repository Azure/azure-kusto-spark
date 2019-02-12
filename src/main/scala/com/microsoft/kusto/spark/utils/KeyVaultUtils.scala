package com.microsoft.kusto.spark.utils

import java.io.IOException

import com.microsoft.azure.CloudException
import com.microsoft.azure.keyvault.KeyVaultClient
import com.microsoft.kusto.spark.datasource.AadApplicationAuthentication

object KeyVaultUtils {
  val AppId = "kustoAppId"
  val AppKey = "kustoAppKey"
  val AppAuthority = "kustoAppAuthority"

  def getAadParamsFromKeyVaultCertAuth: AadApplicationAuthentication ={
    // not implemented yet
    throw new UnsupportedOperationException("does not support cert files yet")
  }

  @throws[CloudException]
  @throws[IOException]
  def getAadParamsFromKeyVaultAppAuth(clientID: String, clientPassword: String, uri: String): AadApplicationAuthentication = {
      var client: KeyVaultClient = new KeyVaultADALAuthenticator(clientID, clientPassword).getAuthenticatedClient
      getAadParamsFromClient(client, uri)
  }

  private def getAadParamsFromClient(client: KeyVaultClient, uri: String): AadApplicationAuthentication ={
    val id = client.getSecret(uri, AppId).value()
    val key = client.getSecret(uri, AppKey).value()
    assert(!id.isEmpty && !key.isEmpty, "keyVault must contain secrets 'kustoAppId', 'kustoAppKey' secrets with non-empty values.")

    var authority = client.getSecret(uri, AppAuthority).value()
    if(authority.isEmpty){
      authority = "microsoft.com"
    }

    AadApplicationAuthentication(id, key, authority)
  }
}

package com.microsoft.kusto.spark.utils

import java.io.IOException

import com.azure.security.keyvault.secrets.SecretClient
import com.microsoft.kusto.spark.authentication._
import com.microsoft.kusto.spark.datasource._

object KeyVaultUtils {
  val AppId = "kustoAppId"
  val AppKey = "kustoAppKey"
  val AppAuthority = "kustoAppAuthority"
  val SasUrl = "blobStorageSasUrl"
  val StorageAccountId = "blobStorageAccountName"
  val StorageAccountKey = "blobStorageAccountKey"
  val Container = "blobContainer"
  var cachedClient: SecretClient = _

  private def getClient(uri: String, clientID: String, clientPassword: String): SecretClient ={
    if(cachedClient == null) {
      cachedClient = new KeyVaultADALAuthenticator(uri, clientID, clientPassword).getAuthenticatedClient
    }
    cachedClient
  }

  @throws[IOException]
  def getStorageParamsFromKeyVault(keyVaultAuthentication: KeyVaultAuthentication): KustoStorageParameters = {
    keyVaultAuthentication match {
      case app: KeyVaultAppAuthentication =>
        val client = getClient(app.uri, app.keyVaultAppID, app.keyVaultAppKey)
        getStorageParamsFromKeyVaultImpl(client, app.uri)
      case certificate: KeyVaultCertificateAuthentication => throw new UnsupportedOperationException("certificates are not yet supported")
    }
  }

  @throws[IOException]
  def getAadAppParametersFromKeyVault(keyVaultAuthentication: KeyVaultAuthentication): AadApplicationAuthentication={
    keyVaultAuthentication match {
      case app: KeyVaultAppAuthentication =>
        val client = getClient(app.uri, app.keyVaultAppID, app.keyVaultAppKey)
        getAadAppParamsFromKeyVaultImpl(client, app.uri)
      case certificate: KeyVaultCertificateAuthentication => throw new UnsupportedOperationException("certificates are not yet supported")
    }
  }

  private def getAadAppParamsFromKeyVaultImpl(client: SecretClient, uri: String): AadApplicationAuthentication ={
    val id = client.getSecret(uri, AppId)
    val key = client.getSecret(uri, AppKey)

    var authority = client.getSecret(uri, AppAuthority).getValue()
    if(authority.isEmpty){
      authority = "microsoft.com"
    }

    AadApplicationAuthentication(
      ID = if (id == null) null else id.getValue(),
      password = if (key == null) null else key.getValue(),
      authority = authority)
  }

  private def getStorageParamsFromKeyVaultImpl(client: SecretClient, uri: String): KustoStorageParameters = {
    val sasUrl = Option(client.getSecret(uri, SasUrl))

    val accountId =  Option(client.getSecret(uri, StorageAccountId))
    val accountKey = Option(client.getSecret(uri, StorageAccountKey))
    val container = Option(client.getSecret(uri, Container))

    if(sasUrl.isEmpty) {
      KustoStorageParameters(
        account = if(accountId.isDefined) accountId.get.getValue else "",
        secret = if (accountKey.isDefined) accountKey.get.getValue else "",
        container = if (container.isDefined) container.get.getValue else "",
        secretIsAccountKey = true)
    } else {
      KustoDataSourceUtils.parseSas(sasUrl.get.getValue)
    }
  }
}

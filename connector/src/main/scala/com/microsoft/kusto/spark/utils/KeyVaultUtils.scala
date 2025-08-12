// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.azure.security.keyvault.secrets.SecretClient
import com.microsoft.kusto.spark.authentication.{
  AadApplicationAuthentication,
  KeyVaultAppAuthentication,
  KeyVaultAuthentication,
  KeyVaultCertificateAuthentication
}
import com.microsoft.kusto.spark.datasource.TransientStorageCredentials

import java.io.IOException
import com.azure.security.keyvault.secrets.SecretClient
import com.azure.security.keyvault.secrets.models.KeyVaultSecret
import com.microsoft.kusto.spark.authentication.{
  AadApplicationAuthentication,
  KeyVaultAppAuthentication,
  KeyVaultAuthentication,
  KeyVaultCertificateAuthentication
}
import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import org.apache.commons.lang3.StringUtils

import java.util.Objects
import scala.util.{Failure, Success, Try}

object KeyVaultUtils {
  private val appId = "kustoAppId"
  private val appKey = "kustoAppKey"
  private val appAuthority = "kustoAppAuthority"
  private val sasUrl = "blobStorageSasUrl"
  private val storageAccountName = "blobStorageAccountName"
  private val storageAccountKey = "blobStorageAccountKey"
  private val container = "blobContainer"
  private var cachedClient: SecretClient = _

  private def getClient(
      uri: String,
      clientID: String,
      clientPassword: String,
      authority: String): SecretClient = {
    if (cachedClient == null) {
      cachedClient = new KeyVaultADALAuthenticator(
        uri,
        clientID,
        clientPassword,
        authority).getAuthenticatedClient
    }
    cachedClient
  }

  @throws[IOException]
  def getStorageParamsFromKeyVault(
      keyVaultAuthentication: KeyVaultAuthentication): TransientStorageCredentials = {
    keyVaultAuthentication match {
      case app: KeyVaultAppAuthentication =>
        val client = getClient(app.uri, app.keyVaultAppID, app.keyVaultAppKey, app.authority)
        getStorageParamsFromKeyVaultImpl(client)
      case certificate: KeyVaultCertificateAuthentication =>
        throw new UnsupportedOperationException("certificates are not yet supported")
    }
  }

  @throws[IOException]
  def getAadAppParametersFromKeyVault(
      keyVaultAuthentication: KeyVaultAuthentication): AadApplicationAuthentication = {
    keyVaultAuthentication match {
      case app: KeyVaultAppAuthentication =>
        val client = getClient(app.uri, app.keyVaultAppID, app.keyVaultAppKey, app.authority)
        getAadAppParamsFromKeyVaultImpl(client, app.uri)
      case _: KeyVaultCertificateAuthentication =>
        throw new UnsupportedOperationException("certificates are not yet supported")
    }
  }

  private def getAadAppParamsFromKeyVaultImpl(
      secretClient: SecretClient,
      uri: String): AadApplicationAuthentication = {
    val id = secretClient.getSecret(appId)
    val key = secretClient.getSecret(appKey)
    val authority = secretClient.getSecret(appAuthority)

    AadApplicationAuthentication(
      ID = if (Objects.isNull(id)) {
        ""
      } else {
        id.getValue
      },
      password = if (Objects.isNull(key)) {
        ""
      } else {
        key.getValue
      },
      authority = if (Objects.isNull(authority)) {
        KustoDataSourceUtils.DefaultMicrosoftTenant
      } else {
        authority.getValue
      })
  }

  private def getStorageParamsFromKeyVaultImpl(
      client: SecretClient): TransientStorageCredentials = {
    val sasUrlValue = Try(client.getSecret(sasUrl))
    val accountName = Try(client.getSecret(storageAccountName))
    val accountKey = Try(client.getSecret(storageAccountKey))
    val containerValue = Try(client.getSecret(container))

    val tsc = getValueOrEmpty(sasUrlValue)

    if (StringUtils.isEmpty(tsc)) {
      new TransientStorageCredentials(
        getValueOrEmpty(accountName),
        getValueOrEmpty(accountKey),
        getValueOrEmpty(containerValue))

    } else {
      new TransientStorageCredentials(tsc)
    }
  }

  private def getValueOrEmpty(secret: Try[KeyVaultSecret]): String = {
    secret match {
      case Success(s) => s.getValue
      case Failure(_) => ""
    }
  }
}

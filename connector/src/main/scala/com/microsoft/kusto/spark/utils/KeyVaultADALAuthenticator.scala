//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark.utils

import com.azure.core.http.policy.{HttpLogDetailLevel, HttpLogOptions}
import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.security.keyvault.secrets.{SecretClient, SecretClientBuilder}
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils.DefaultMicrosoftTenant

/**
 * Authenticates to Azure Key Vault by providing a callback to authenticate using ADAL.
 */
class KeyVaultADALAuthenticator(
    uri: String,
    clientId: String,
    clientKey: String,
    authority: String) {
  val authorityId: String = if (authority == null) DefaultMicrosoftTenant else authority

  def getAuthenticatedClient: SecretClient = {
    new SecretClientBuilder()
      .credential(
        new ClientSecretCredentialBuilder()
          .clientId(clientId)
          .clientSecret(clientKey)
          .tenantId(authorityId)
          .build())
      .vaultUrl(uri)
      .httpLogOptions(new HttpLogOptions().setLogLevel(HttpLogDetailLevel.BODY_AND_HEADERS))
      .buildClient
  }
}

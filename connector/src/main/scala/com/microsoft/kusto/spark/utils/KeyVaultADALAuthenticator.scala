package com.microsoft.kusto.spark.utils

import java.net.MalformedURLException
import java.util.concurrent.{ExecutionException, ExecutorService, Executors, Future}

import com.microsoft.aad.adal4j.{AuthenticationContext, AuthenticationResult, ClientCredential}
import com.microsoft.azure.keyvault.KeyVaultClient
import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials
import com.microsoft.rest.credentials.ServiceClientCredentials

/**
  * Authenticates to Azure Key Vault by providing a callback to authenticate
  * using ADAL.
  */
class KeyVaultADALAuthenticator(clientId: String, clientKey: String) {

  def getAuthenticatedClient: KeyVaultClient = {
    // Creates the KeyVaultClient using the created credentials.
    new KeyVaultClient(createCredentials)
  }

  private def createCredentials: ServiceClientCredentials = {
    new KeyVaultCredentials() { //Callback that supplies the token type and access token on request.
      override def doAuthenticate(authorization: String, resource: String, scope: String): String = {
        try {
          val authResult = getAccessToken(authorization, resource)
          authResult.getAccessToken
        } catch {
          case e: Exception =>
            KustoDataSourceUtils.logError("KeyVaultADALAuthenticator", "Exception trying to access Key Vault:" + e.getMessage)
            ""
        }
      }
    }
  }

  @throws[InterruptedException]
  @throws[ExecutionException]
  @throws[MalformedURLException]
  private def getAccessToken(authorization: String, resource: String): AuthenticationResult  = {
    var result: AuthenticationResult = null
    var service: ExecutorService = null

    //Starts a service to fetch access token.
    try {
      service = Executors.newFixedThreadPool(1)
      val context = new AuthenticationContext(authorization, false, service)

      //Acquires token based on client ID and client secret.
      var future: Future[AuthenticationResult] = null
      if (clientId != null && clientKey != null) {
        val credentials = new ClientCredential(clientId, clientKey)
        future = context.acquireToken(resource, credentials, null)
      }

      result = future.get
    } finally service.shutdown()
    if (result == null) throw new RuntimeException("Authentication results were null.")
    result
  }
}

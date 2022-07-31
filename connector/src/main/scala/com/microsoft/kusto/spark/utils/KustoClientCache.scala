package com.microsoft.kusto.spark.utils

import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.function

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.authentication._
import com.microsoft.kusto.spark.utils.{KustoConstants => KCONST}
import org.apache.http.client.utils.URIBuilder

object KustoClientCache {
  var clientCache = new ConcurrentHashMap[ClusterAndAuth, KustoClient]

  def getClient(clusterUrl: String, authentication: KustoAuthentication, ingestionUrl: Option[String], clusterAlias: String): KustoClient = {
    val clusterAndAuth = ClusterAndAuth(clusterUrl, authentication, ingestionUrl, clusterAlias)
    clientCache.computeIfAbsent(clusterAndAuth, adderSupplier)
  }

  val adderSupplier: function.Function[ClusterAndAuth, KustoClient] = new java.util.function.Function[ClusterAndAuth, KustoClient]() {
    override def apply(aa: ClusterAndAuth): KustoClient = createClient(aa)
  }

  private def createClient(clusterAndAuth: ClusterAndAuth): KustoClient = {
    val (engineKcsb, ingestKcsb) = clusterAndAuth.authentication match {
      case app: AadApplicationAuthentication => (
        ConnectionStringBuilder.createWithAadApplicationCredentials(clusterAndAuth.engineUri, app.ID, app.password, app.authority),
        ConnectionStringBuilder.createWithAadApplicationCredentials(clusterAndAuth.ingestUri, app.ID, app.password, app.authority)
      )
      case app: AadApplicationCertificateAuthentication =>
        val keyCert = CertUtils.readPfx(app.certFilePath, app.certPassword)
        (
          ConnectionStringBuilder.createWithAadApplicationCertificate(clusterAndAuth.engineUri, app.appId, keyCert.cert, keyCert.key, app.authority),
          ConnectionStringBuilder.createWithAadApplicationCertificate(clusterAndAuth.ingestUri, app.appId, keyCert.cert, keyCert.key, app.authority)
        )
      case keyVaultParams: KeyVaultAuthentication =>
        val app = KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultParams)
        (
          ConnectionStringBuilder.createWithAadApplicationCredentials(clusterAndAuth.engineUri, app.ID, app.password, app.authority),
          ConnectionStringBuilder.createWithAadApplicationCredentials(clusterAndAuth.ingestUri, app.ID, app.password, app.authority)
        )
      case userPrompt: KustoUserPromptAuthentication => (
        ConnectionStringBuilder.createWithUserPrompt(clusterAndAuth.engineUri, userPrompt.authority, null),
        ConnectionStringBuilder.createWithUserPrompt(clusterAndAuth.ingestUri, userPrompt.authority, null)
      )
      case userToken: KustoAccessTokenAuthentication => (
        ConnectionStringBuilder.createWithAadAccessTokenAuthentication(clusterAndAuth.engineUri, userToken.token),
        ConnectionStringBuilder.createWithAadAccessTokenAuthentication(clusterAndAuth.ingestUri, userToken.token)
      )
      case tokenProvider: KustoTokenProviderAuthentication => (
        ConnectionStringBuilder.createWithAadTokenProviderAuthentication(clusterAndAuth.engineUri, tokenProvider.tokenProviderCallback),
        ConnectionStringBuilder.createWithAadTokenProviderAuthentication(clusterAndAuth.ingestUri, tokenProvider.tokenProviderCallback)
      )
    }

    engineKcsb.setClientVersionForTracing(KCONST.ClientName)
    ingestKcsb.setClientVersionForTracing(KCONST.ClientName)

    new KustoClient(engineKcsb, ingestKcsb, clusterAndAuth.clusterAlias)
  }

  private[kusto] case class ClusterAndAuth(engineUrl: String, authentication: KustoAuthentication, ingestionUri: Option[String], clusterAlias:String) {
    val engineUri: String = engineUrl
    val ingestUri: String = ingestionUri.getOrElse(new URIBuilder().setScheme("https")
      .setHost(KustoDataSourceUtils.IngestPrefix + new URI(engineUrl).getHost)
      .toString)

    override def equals(that: Any): Boolean = that match {
      case aa: ClusterAndAuth => engineUrl == aa.engineUrl && authentication == aa.authentication && ingestUri == aa.ingestUri
      case _ => false
    }

    override def hashCode(): Int = engineUri.hashCode + authentication.hashCode + ingestUri.hashCode
  }
}

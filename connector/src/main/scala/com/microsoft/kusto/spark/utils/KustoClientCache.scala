package com.microsoft.kusto.spark.utils

import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.function

import com.microsoft.azure.kusto.data.ConnectionStringBuilder
import com.microsoft.kusto.spark.authentication._
import com.microsoft.kusto.spark.utils.{KustoConstants => KCONST}
import org.apache.http.client.utils.URIBuilder

object KustoClientCache {
  var clientCache = new ConcurrentHashMap[AliasAndAuth, KustoClient]

  def getClient(clusterAlias: String, clusterUrl: String, authentication: KustoAuthentication): KustoClient = {
    val clusterAndAuth = AliasAndAuth(clusterAlias, clusterUrl, authentication)
    clientCache.computeIfAbsent(clusterAndAuth, adderSupplier)
  }

  val adderSupplier: function.Function[AliasAndAuth, KustoClient] = new java.util.function.Function[AliasAndAuth, KustoClient]() {
    override def apply(aa: AliasAndAuth): KustoClient = createClient(aa)
  }

  private def createClient(aliasAndAuth: AliasAndAuth): KustoClient = {
    val (engineKcsb, ingestKcsb) = aliasAndAuth.authentication match {
      case null => throw new MatchError("Can't create ConnectionStringBuilder with null authentication params")
      case app: AadApplicationAuthentication => (
        ConnectionStringBuilder.createWithAadApplicationCredentials(aliasAndAuth.engineUri, app.ID, app.password, app.authority),
        ConnectionStringBuilder.createWithAadApplicationCredentials(aliasAndAuth.ingestUri, app.ID, app.password, app.authority)
      )
      case app: AadApplicationCertificateAuthentication =>
        val keyCert = CertUtils.readPfx(app.certFilePath, app.certPassword)
        (
          ConnectionStringBuilder.createWithAadApplicationCertificate(aliasAndAuth.engineUri, app.appId, keyCert.cert, keyCert.key),
          ConnectionStringBuilder.createWithAadApplicationCertificate(aliasAndAuth.ingestUri, app.appId, keyCert.cert, keyCert.key)
        )
      case keyVaultParams: KeyVaultAuthentication =>
        val app = KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultParams)
        (
          ConnectionStringBuilder.createWithAadApplicationCredentials(aliasAndAuth.engineUri, app.ID, app.password, app.authority),
          ConnectionStringBuilder.createWithAadApplicationCredentials(aliasAndAuth.ingestUri, app.ID, app.password, app.authority)
        )
      case userToken: KustoAccessTokenAuthentication => (
        ConnectionStringBuilder.createWithAadAccessTokenAuthentication(aliasAndAuth.engineUri, userToken.token),
        ConnectionStringBuilder.createWithAadAccessTokenAuthentication(aliasAndAuth.ingestUri, userToken.token)
      )
      case tokenProvider: KustoTokenProviderAuthentication => (
        ConnectionStringBuilder.createWithAadTokenProviderAuthentication(aliasAndAuth.engineUri, tokenProvider.tokenProviderCallback),
        ConnectionStringBuilder.createWithAadTokenProviderAuthentication(aliasAndAuth.ingestUri, tokenProvider.tokenProviderCallback)
      )
    }

    engineKcsb.setClientVersionForTracing(KCONST.clientName)
    ingestKcsb.setClientVersionForTracing(KCONST.clientName)

    new KustoClient(aliasAndAuth.clusterAlias, engineKcsb, ingestKcsb)
  }

  private[kusto] case class AliasAndAuth(clusterAlias: String, engineUrl: String, authentication: KustoAuthentication) {
    val engineUri: String = engineUrl
    val ingestUri: String = new URIBuilder().setScheme("https")
      .setHost(KustoDataSourceUtils.ingestPrefix + new URI(engineUrl).getHost)
      .toString

    override def equals(that: Any): Boolean = that match {
      case aa: AliasAndAuth => clusterAlias == aa.clusterAlias && authentication == aa.authentication
      case _ => false
    }

    override def hashCode(): Int = clusterAlias.hashCode + authentication.hashCode
  }
}



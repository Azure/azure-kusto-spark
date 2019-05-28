package com.microsoft.kusto.spark.utils

import java.util.concurrent.ConcurrentHashMap

import com.microsoft.azure.kusto.data.{ClientFactory, ConnectionStringBuilder}
import com.microsoft.azure.kusto.ingest.IngestClientFactory
import com.microsoft.kusto.spark.authentication.{AadApplicationAuthentication, KeyVaultAuthentication, KustoAccessTokenAuthentication, KustoAuthentication}
import com.microsoft.kusto.spark.utils.{KustoConstants => KCONST}

object KustoClientCache {
  var clientCache = new ConcurrentHashMap[AliasAndAuth, KustoClient]

  def getClient(clusterAlias: String, authentication: KustoAuthentication): KustoClient = {
    val aliasAndAuth = AliasAndAuth(clusterAlias, authentication)
    clientCache.computeIfAbsent(aliasAndAuth, adderSupplier)
  }

  val adderSupplier = new java.util.function.Function[AliasAndAuth, KustoClient]() {
    override def apply(aa: AliasAndAuth): KustoClient = createClient(aa)
  }

  private def createClient(aliasAndAuth: AliasAndAuth): KustoClient = {
    val (engineKcsb, ingestKcsb) = aliasAndAuth.authentication match {
      case null => throw new MatchError("Can't create ConnectionStringBuilder with null authentication params")
      case app: AadApplicationAuthentication => (
        ConnectionStringBuilder.createWithAadApplicationCredentials(aliasAndAuth.engineUri, app.ID, app.password, app.authority),
        ConnectionStringBuilder.createWithAadApplicationCredentials(aliasAndAuth.ingestUri, app.ID, app.password, app.authority)
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
    }

    engineKcsb.setClientVersionForTracing(KCONST.clientName)
    ingestKcsb.setClientVersionForTracing(KCONST.clientName)

    new KustoClient(aliasAndAuth.clusterAlias,
      ClientFactory.createClient(engineKcsb),
      ClientFactory.createClient(ingestKcsb),
      IngestClientFactory.createClient(ingestKcsb))
  }

  private[KustoClientCache] case class AliasAndAuth(clusterAlias: String, authentication: KustoAuthentication) {
    private[AliasAndAuth] val clusterUri = "https://%s.kusto.windows.net"
    val ingestClusterAlias = s"ingest-${clusterAlias}"
    val engineUri = clusterUri.format(clusterAlias)
    val ingestUri = clusterUri.format(ingestClusterAlias)

    override def equals(that: Any) : Boolean = that match {
      case aa: AliasAndAuth => clusterAlias == aa.clusterAlias && authentication == aa.authentication
      case _ => false
    }

    override def hashCode(): Int = clusterAlias.hashCode + authentication.hashCode
  }
}



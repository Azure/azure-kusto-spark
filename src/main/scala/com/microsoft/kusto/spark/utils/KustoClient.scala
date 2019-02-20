package com.microsoft.kusto.spark.utils
import com.microsoft.azure.kusto.data.{Client, ClientFactory, ConnectionStringBuilder}
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestClientFactory}
import com.microsoft.kusto.spark.datasource._
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

object KustoClient {
  private[kusto] def getAdmin(authentication: KustoAuthentication, clusterAlias: String, isAdminCluster: Boolean = false): Client = {
    val clusterUri = s"https://${if(isAdminCluster) "ingest"}$clusterAlias.kusto.windows.net"
    val kcsb = getKcsbHelper(authentication,clusterUri)
    kcsb.setClientVersionForTracing(KDSU.ClientName)
    ClientFactory.createClient(kcsb)
  }

  private[kusto] def getIngest(authentication: KustoAuthentication, clusterAlias: String): IngestClient = {
    val ingestKcsb = getKcsbHelper(authentication, s"https://ingest-$clusterAlias.kusto.windows.net")
    ingestKcsb.setClientVersionForTracing(KDSU.ClientName)
    IngestClientFactory.createClient(ingestKcsb)
  }

  private def getKcsbHelper(authentication: KustoAuthentication, clusterUri: String) ={
    var kcsb: ConnectionStringBuilder = null
    authentication match {
      case app: AadApplicationAuthentication =>
        kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(clusterUri, app.ID, app.password, app.authority)
      case keyVaultParams: KeyVaultAuthentication =>
        val app = keyVaultParams match {
          case app: KeyVaultAppAuthentication => KeyVaultUtils.getAadParamsFromKeyVaultAppAuth(app.keyVaultAppID, app.keyVaultAppKey, app.uri)
          case cert: KeyVaultCertificateAuthentication => KeyVaultUtils.getAadParamsFromKeyVaultCertAuth
        }
        kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(clusterUri, app.ID, app.password, app.authority)
      case userTokne: KustoUserTokenAuthentication =>
        kcsb = ConnectionStringBuilder.createWithAadUserTokenAuthentication(clusterUri, userTokne.token)
    }
    kcsb
  }
}

package com.microsoft.kusto.spark.utils
import com.microsoft.azure.kusto.data.{Client, ClientFactory, ConnectionStringBuilder}
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestClientFactory}
import com.microsoft.kusto.spark.datasource._
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

object KustoClient {
  def getAdmin(authentication: KustoAuthentication, clusterAlias: String, isIngestCluster: Boolean = false): Client = {
    val clusterUri = s"https://${if(isIngestCluster) "ingest" else ""}$clusterAlias.kusto.windows.net"
    val kcsb = getKcsb(authentication,clusterUri)
    kcsb.setClientVersionForTracing(KDSU.ClientName)
    ClientFactory.createClient(kcsb)
  }

  def getAdmin(kcsb: ConnectionStringBuilder): Client = {
    ClientFactory.createClient(kcsb)
  }

  def getIngest(authentication: KustoAuthentication, clusterAlias: String): IngestClient = {
    val ingestKcsb = getKcsb(authentication, s"https://ingest-$clusterAlias.kusto.windows.net")
    ingestKcsb.setClientVersionForTracing(KDSU.ClientName)
    IngestClientFactory.createClient(ingestKcsb)
  }

  def getIngest(ingestKcsb: ConnectionStringBuilder): IngestClient = {
    IngestClientFactory.createClient(ingestKcsb)
  }

  def getKcsb(authentication: KustoAuthentication, clusterUri: String): ConnectionStringBuilder = {
    authentication match {
      case null => throw new MatchError("Can't create ConnectionStringBuilder with null authentication params")
      case app: AadApplicationAuthentication =>
        ConnectionStringBuilder.createWithAadApplicationCredentials(clusterUri, app.ID, app.password, app.authority)
      case keyVaultParams: KeyVaultAuthentication =>
        var app = KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultParams)
        ConnectionStringBuilder.createWithAadApplicationCredentials(clusterUri, app.ID, app.password, app.authority)
      case userTokne: KustoAccessTokenAuthentication =>
        ConnectionStringBuilder.createWithAadAccessTokenAuthentication(clusterUri, userTokne.token)
    }
  }
}

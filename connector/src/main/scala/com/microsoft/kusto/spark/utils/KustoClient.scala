package com.microsoft.kusto.spark.utils

import com.microsoft.azure.kusto.data.{Client, ClientFactory, ConnectionStringBuilder}
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestClientFactory}
import com.microsoft.kusto.spark.datasource._
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU, KustoConstants => KCONST}

import scala.collection.immutable.HashMap

object KustoClient {
  var ingestClientCache = new HashMap[String, IngestClient]
  var adminClientCache = new HashMap[String, Client]
  org.apache.spark.sql.sources.In
  def getAdmin(authentication: KustoAuthentication, clusterAlias: String, isIngestCluster: Boolean = false): Client = {
    val clusterUri = s"https://${if(isIngestCluster) "ingest" else ""}$clusterAlias.kusto.windows.net"
    val kcsb = getKcsb(authentication,clusterUri)
    kcsb.setClientVersionForTracing(KCONST.ClientName)
    getAdmin(kcsb, clusterAlias, isIngestCluster)
  }

  def getAdmin(kcsb: ConnectionStringBuilder, clusterAlias: String, isIngestCluster: Boolean): Client = {
    var cachedClient: Option[Client] = adminClientCache.get(clusterAlias + isIngestCluster)
    if(cachedClient.isEmpty){
      cachedClient = Some(ClientFactory.createClient(kcsb))
      adminClientCache = adminClientCache + (clusterAlias + isIngestCluster -> cachedClient.get)
    }

    cachedClient.get
  }

  def getIngest(authentication: KustoAuthentication, clusterAlias: String): IngestClient = {
    val ingestKcsb = getKcsb(authentication, s"https://ingest-$clusterAlias.kusto.windows.net")
    ingestKcsb.setClientVersionForTracing(KCONST.ClientName)
    getIngest(ingestKcsb, clusterAlias)
  }

  def getIngest(ingestKcsb: ConnectionStringBuilder, clusterAlias: String): IngestClient = {
    var cachedClient = ingestClientCache.get(clusterAlias)
    if(cachedClient.isEmpty) {
      cachedClient = Some(IngestClientFactory.createClient(ingestKcsb))
      ingestClientCache = ingestClientCache + (clusterAlias -> cachedClient.get)
    }

    cachedClient.get
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

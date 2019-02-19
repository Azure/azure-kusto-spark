package com.microsoft.kusto.spark.utils
import com.microsoft.azure.kusto.data.{Client, ClientFactory, ConnectionStringBuilder}
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestClientFactory}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

object KustoClient {
  private[kusto] def getAdmin(
                               cluster: String,
                               appId: String,
                               appKey: String,
                               aadAuthority: String
                             ): Client = {
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, aadAuthority)
    engineKcsb.setClientVersionForTracing(KDSU.ClientName)
    ClientFactory.createClient(engineKcsb)
  }

  private[kusto] def getIngest(
                               cluster: String,
                               appId: String,
                               appKey: String,
                               aadAuthority: String
                             ): IngestClient = {
    val ingestKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://ingest-$cluster.kusto.windows.net", appId, appKey, aadAuthority)
    ingestKcsb.setClientVersionForTracing(KDSU.ClientName)
    IngestClientFactory.createClient(ingestKcsb)
  }
}

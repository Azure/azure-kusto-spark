package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestClientFactory, IngestionProperties}

class KustoParquetIngestor(ingestionProperties: IngestionProperties) extends Serializable {
  def ingest(blobPath: String): Unit = {
    val csb: ConnectionStringBuilder = ConnectionStringBuilder.createWithAadApplicationCredentials(
      "https://ingest-sdke2etestcluster.eastus.dev.kusto.windows.net",
      "",
      "")
    val ingestClient: IngestClient = IngestClientFactory.createClient(csb)
    println(s" ==========>>>>>>>>>>>>>>>> IN INGESTOR : >>>>>>>>>>  ${blobPath} ")
    val blobSourceInfo = new BlobSourceInfo(blobPath)
    ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties)
  }
}

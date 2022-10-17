package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestionProperties}

class KustoParquetIngestor(ingestClient: IngestClient) extends Serializable {
  def ingest(blobPath: String, ingestionProperties: IngestionProperties): Unit = {
    val blobSourceInfo = new BlobSourceInfo(blobPath)
    ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties)
  }
}

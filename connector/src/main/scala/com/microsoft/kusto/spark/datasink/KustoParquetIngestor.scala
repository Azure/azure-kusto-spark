package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.ingest.result.IngestionResult
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestionProperties}

class KustoParquetIngestor(ingestClient: IngestClient) extends Serializable {
  def ingest(blobPath: String, ingestionProperties: IngestionProperties): IngestionResult  = {
    val blobSourceInfo = new BlobSourceInfo(blobPath)
    ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties)
  }
}

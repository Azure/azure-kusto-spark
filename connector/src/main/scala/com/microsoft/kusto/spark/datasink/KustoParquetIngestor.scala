// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.ingest.result.IngestionResult
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestionProperties}

class KustoParquetIngestor(ingestClient: IngestClient) extends Serializable {
  def ingest(blobPath: String, ingestionProperties: IngestionProperties): IngestionResult = {
    val blobSourceInfo = new BlobSourceInfo(blobPath)
    ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties)
  }
}

package com.microsoft.kusto.spark.utils


import com.azure.storage.blob.BlobContainerClientBuilder
import com.azure.storage.blob.models.BlobItem
import com.microsoft.kusto.spark.datasource.TransientStorageCredentials



object KustoBlobStorageUtils {
  def deleteFromBlob(account: String, directory: String, container: String, secret: String, keyIsSas: Boolean = false): Unit = {
    val storageConnectionString = if (keyIsSas) {
      "DefaultEndpointsProtocol=https;" +
        s"AccountName=$account;" +
        s"SharedAccessSignature=$secret"
    }
    else {
      "DefaultEndpointsProtocol=https;" +
      s"AccountName=$account;" +
      s"AccountKey=$secret"
    }

    performBlobDelete(directory, container, storageConnectionString)
  }

  def deleteFromBlob(directory: String, sasKeyFullUrl: String): Unit = {
    val storageParams = new TransientStorageCredentials(sasKeyFullUrl)
    val storageConnectionString =
      "DefaultEndpointsProtocol=https;" +
      s"AccountName=${storageParams.storageAccountName};" +
      s"SharedAccessSignature=${storageParams.sasKey}"

    performBlobDelete(directory, storageParams.blobContainer, storageConnectionString)
  }

  private[kusto] def performBlobDelete(directory: String, container: String, storageConnectionString: String): Unit = {
    val blobClient = new BlobContainerClientBuilder()
      .connectionString(storageConnectionString)
      .containerName(container)
      .buildClient()
    val blobsToDelete = blobClient.listBlobsByHierarchy(directory).iterator()
    while (blobsToDelete.hasNext) {
      val blob: BlobItem = blobsToDelete.next()
      blobClient.getBlobClient(blob.getName).delete()
    }
  }
}

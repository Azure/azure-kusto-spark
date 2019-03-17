package com.microsoft.kusto.spark.utils

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlockBlob
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import scala.collection.JavaConversions._

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
    val storageParams = KDSU.parseSas(sasKeyFullUrl)
    val storageConnectionString =
      "DefaultEndpointsProtocol=https;" +
      s"AccountName=${storageParams.account};" +
      s"SharedAccessSignature=${storageParams.secret}"

    performBlobDelete(directory, storageParams.container, storageConnectionString)
  }

  private[kusto] def performBlobDelete(directory: String, container: String, storageConnectionString: String): Unit = {
    val cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString)

    val blobClient = cloudStorageAccount.createCloudBlobClient()
    val blobContainer = blobClient.getContainerReference(container)
    val blobsWithPrefix = blobContainer.listBlobs(directory)

    blobsWithPrefix.foreach(blob => {
      val cloudBlob = blobContainer.getBlockBlobReference(new CloudBlockBlob(blob.getUri).getName)
      cloudBlob.deleteIfExists()
    })
  }
}

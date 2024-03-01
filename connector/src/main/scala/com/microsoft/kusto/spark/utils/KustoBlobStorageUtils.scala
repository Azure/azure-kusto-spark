//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark.utils

import com.azure.storage.blob.BlobContainerClientBuilder
import com.azure.storage.blob.models.BlobItem
import com.microsoft.kusto.spark.datasource.TransientStorageCredentials

object KustoBlobStorageUtils {
  def deleteFromBlob(
      account: String,
      directory: String,
      container: String,
      secret: String,
      keyIsSas: Boolean = false): Unit = {
    val storageConnectionString = if (keyIsSas) {
      "DefaultEndpointsProtocol=https;" +
        s"AccountName=$account;" +
        s"SharedAccessSignature=$secret"
    } else {
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

  private[kusto] def performBlobDelete(
      directory: String,
      container: String,
      storageConnectionString: String): Unit = {
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

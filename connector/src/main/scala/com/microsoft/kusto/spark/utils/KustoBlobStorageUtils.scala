// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

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
      keyIsSas: Boolean = false): Unit =
    deleteFromBlob(
      account,
      directory,
      container,
      secret,
      keyIsSas,
      KustoDataSourceUtils.DefaultDomainPostfix)

  def deleteFromBlob(
      account: String,
      directory: String,
      container: String,
      secret: String,
      keyIsSas: Boolean,
      endpointSuffix: String): Unit =
    performBlobDelete(
      directory,
      container,
      storageConnectionString(account, secret, keyIsSas, endpointSuffix))

  private[kusto] def storageConnectionString(
      account: String,
      secret: String,
      keyIsSas: Boolean,
      endpointSuffix: String): String = {
    val credential =
      if (keyIsSas) s"SharedAccessSignature=${secret.stripPrefix("?")}"
      else s"AccountKey=$secret"
    s"DefaultEndpointsProtocol=https;AccountName=$account;EndpointSuffix=$endpointSuffix;$credential"
  }

  def deleteFromBlob(directory: String, sasKeyFullUrl: String): Unit = {
    val storageParams = new TransientStorageCredentials(sasKeyFullUrl)
    deleteFromBlob(
      storageParams.storageAccountName,
      directory,
      storageParams.blobContainer,
      storageParams.sasKey,
      keyIsSas = true,
      storageParams.domainSuffix)
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

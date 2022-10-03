package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import com.microsoft.kusto.spark.utils.{KustoAzureFsSetupCache, KustoQueryUtils}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkContext, TaskContext}
import org.joda.time.{DateTime, DateTimeZone}

import java.util.UUID

/*
  Serializes and writes data as a parquet blob
*/
class KustoParquetWriter(sparkContext: SparkContext, storageCredentials: TransientStorageCredentials) {
  def write(inputDataFrame: DataFrame,
            databaseName: String,
            tmpTableName: String): Unit = {
    // Create the blob file path
    val blobName = s"${KustoQueryUtils.simplifyName(databaseName)}_${tmpTableName}_" +
      s"${UUID.randomUUID.toString}_${TaskContext.getPartitionId()}_spark.parquet"
    val blobPath = s"wasbs://${storageCredentials.blobContainer}@${storageCredentials.storageAccountName}." +
      s"blob.${storageCredentials.domainSuffix}/$blobName"
    // Set up the access in spark config
    setUpAccessForAzureFS()
    // Write the file
    inputDataFrame.write.parquet(blobPath)
  }

  private[kusto] def setUpAccessForAzureFS(): Unit = {
    val hadoopConfiguration = sparkContext.hadoopConfiguration
    val now = DateTime.now(DateTimeZone.UTC)
    if (!storageCredentials.sasDefined) {
      if (!KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(storageCredentials.storageAccountName
        , storageCredentials.storageAccountKey, now)) {
        hadoopConfiguration.set(s"fs.azure.account.key.${storageCredentials.storageAccountName}." +
          s"blob.${storageCredentials.domainSuffix}",
          s"${storageCredentials.storageAccountKey}")
      }
    }
    else {
      if (!KustoAzureFsSetupCache.updateAndGetPrevSas(storageCredentials.blobContainer,
        storageCredentials.storageAccountName, storageCredentials.sasKey, now)) {
        hadoopConfiguration.set(s"fs.azure.sas.${storageCredentials.blobContainer}.${storageCredentials.storageAccountName}." +
          s"blob.${storageCredentials.domainSuffix}",
          s"${storageCredentials.sasKey}")
      }
    }

    hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfiguration.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfiguration.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
  }
}

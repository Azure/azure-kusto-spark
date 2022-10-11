package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import com.microsoft.kusto.spark.utils.{KustoAzureFsSetupCache, KustoQueryUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkContext, TaskContext}
import org.joda.time.{DateTime, DateTimeZone}

import java.util.UUID
import scala.util.Try

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
    val blobDestinationPath = s"wasbs://${storageCredentials.blobContainer}@${storageCredentials.storageAccountName}." +
      s"blob.${storageCredentials.domainSuffix}/$blobName-merged-parquet"
    // Set up the access in spark config
    setUpAccessForAzureFS()
    // Write the file
    // inputDataFrame.write.format("com.microsoft.kusto.spark.datasink.parquet.KustoParquetSinkProvider").option("create_table", "true").save(blobPath)
    inputDataFrame.write.parquet(blobPath)

    val hadoopConfig = sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConfig)
    val s = new Path(blobPath)
    val d = new Path(blobDestinationPath)
    // Source path is expected to be a directory:
    Try {
      hdfs
        .listStatus(s)
        .sortBy(_.getPath.getName)
        .collect {
          case status if status.isFile =>
            val inputFile = hdfs.open(status.getPath)
            println(s"Files ==> $inputFile")
        }
    }
    sparkContext.binaryFiles(blobPath).foreachPartition(partition=>{
      partition.foreach(part=>{
        println(s"Partitioned Files ==> ${part._1}")
      })
    })
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

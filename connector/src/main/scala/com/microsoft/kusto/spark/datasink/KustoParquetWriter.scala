package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import com.microsoft.kusto.spark.utils.{KustoAzureFsSetupCache, KustoQueryUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkContext, TaskContext}
import org.joda.time.{DateTime, DateTimeZone}

import java.net.URI
import java.time.Instant
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/*
  Serializes and writes data as a parquet blob
*/
class KustoParquetWriter(sparkSession: SparkSession, storageCredentials: TransientStorageCredentials) {
  def write(inputDataFrame: DataFrame,
            databaseName: String,
            tmpTableName: String): Unit = {
    // Create the blob file path
    val blobName = s"${KustoQueryUtils.simplifyName(databaseName)}_${tmpTableName}_" +
      s"${UUID.randomUUID.toString}_${TaskContext.getPartitionId()}_spark.parquet"
    val blobRoot = s"wasbs://${storageCredentials.blobContainer}@${storageCredentials.storageAccountName}.blob.${storageCredentials.domainSuffix}"
    val blobPath = s"/$blobName"
    // Set up the access in spark config
    val hadoopConfig = getWasbHadoopConfig(sparkSession.sparkContext.hadoopConfiguration)
    setUpAccessForAzureFS(hadoopConfig)
    // Write the file
    println(s"Starting ${Instant.now()}")
    inputDataFrame.write.parquet(s"$blobRoot$blobPath")
    println(s"Processing next at ${Instant.now()}")
    val hdfs = FileSystem.get(new URI(blobRoot),hadoopConfig)
    val sourcePath = new Path(blobPath)
    val listOfFiles = new ListBuffer[String]()
    Try(hdfs.listFiles(sourcePath, true)) match {
      case Success(remoteFileIterator) => {
        while (remoteFileIterator.hasNext){
          val remoteFile = remoteFileIterator.next()
          listOfFiles.append(remoteFile.getPath.getName)
        }
      }
      case Failure(exception) => exception.printStackTrace()
    }
    println(s"List of files ${listOfFiles.mkString(",")}")
  }

  private[kusto] def getWasbHadoopConfig(hadoopConfig:Configuration): Configuration = {
    hadoopConfig.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig
  }

  private[kusto] def setUpAccessForAzureFS(hadoopConfig:Configuration): Unit = {
    val now = DateTime.now(DateTimeZone.UTC)
    if (!storageCredentials.sasDefined) {
      if (!KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(storageCredentials.storageAccountName
        , storageCredentials.storageAccountKey, now)) {
        hadoopConfig.set(s"fs.azure.account.key.${storageCredentials.storageAccountName}." +
          s"blob.${storageCredentials.domainSuffix}",
          s"${storageCredentials.storageAccountKey}")
      }
    }
    else {
      if (!KustoAzureFsSetupCache.updateAndGetPrevSas(storageCredentials.blobContainer,
        storageCredentials.storageAccountName, storageCredentials.sasKey, now)) {
        hadoopConfig.set(s"fs.azure.sas.${storageCredentials.blobContainer}.${storageCredentials.storageAccountName}." +
          s"blob.${storageCredentials.domainSuffix}",
          s"${storageCredentials.sasKey}")
      }
    }
  }
}

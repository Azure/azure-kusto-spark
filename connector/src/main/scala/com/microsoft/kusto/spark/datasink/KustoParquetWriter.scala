package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{ClientImpl, ClientRequestProperties}
import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableGetSchemaAsRowsCommand
import com.microsoft.kusto.spark.utils.KustoIngestionUtils.{setCsvMapping, setParquetMapping}
import com.microsoft.kusto.spark.utils.{ExtendedKustoClient, KustoAzureFsSetupCache, KustoDataSourceUtils, KustoQueryUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.{DateTime, DateTimeZone}
import org.json.JSONObject

import java.net.URI
import java.util.UUID
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/*
  Serializes and writes data as a parquet blob
*/
class KustoParquetWriter(sparkSession: SparkSession, storageCredentials: TransientStorageCredentials) {
  private val className = this.getClass.getName

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
    inputDataFrame.write.parquet(s"$blobRoot$blobPath")
    // Get the list of files created. This will be used for ingestion
    val hdfs = FileSystem.get(new URI(blobRoot), hadoopConfig)
    val sourcePath = new Path(blobPath)
    val listOfFiles = new ListBuffer[String]()
    // List these files and add them to the list for processing
    Try(hdfs.listFiles(sourcePath, true)) match {
      case Success(remoteFileIterator) =>
        while (remoteFileIterator.hasNext) {
          val mayBeRemoteFile = Option(remoteFileIterator.next())
          mayBeRemoteFile match {
            case Some(remoteFile) => if (!"_SUCCESS".equals(remoteFile.getPath.getName)) listOfFiles.append(remoteFile.getPath.getName)
            case None => KustoDataSourceUtils.logInfo(className, s"Empty paths while parsing $sourcePath. " +
              "Will not be added for processing")
          }
        }
      // Cannot list these files. Error accessing the file system needs to be halted processing
      case Failure(exception) => throw exception
    }
    import sparkSession.implicits._
    val listOfFilesToProcess = listOfFiles.toDF()
    val blobFileBase = s"https://${storageCredentials.storageAccountName}.blob.${storageCredentials.domainSuffix}/${storageCredentials.blobContainer}"
    val sourceSchema = inputDataFrame.schema

    listOfFilesToProcess.rdd.foreachPartition(partitionResult => {
      val database: String = "" //System.getProperty(KustoSinkOptions.KUSTO_DATABASE)
      val tableName = ""
      //val tableName = "stormsspark"

      /*val ingestionProperties = new IngestionProperties(database, tableName)
      ingestionProperties.getIngestionMapping().setIngestionMappingReference("spark_data_ref2", IngestionMappingKind.PARQUET)
      ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PARQUET)
      ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)*/


      //build the table schema if not exists
      val kustoClient = new ExtendedKustoClient(ConnectionStringBuilder.createWithAadApplicationCredentials(
        "",
        "",
        "",
        ""), null,"")
      val schemaShowCommandResult = kustoClient.executeEngine(database,
        generateTableGetSchemaAsRowsCommand(tableName), new ClientRequestProperties).getPrimaryResults
      val targetSchema = schemaShowCommandResult.getData.asScala.map(c => c.get(0).asInstanceOf[JSONObject]).toArray
      val sparkIngestionProperties = new SparkIngestionProperties()

      setParquetMapping(sourceSchema, targetSchema, sparkIngestionProperties)
      val parquetIngestor = new KustoParquetIngestor(sparkIngestionProperties.toIngestionProperties(database, tableName))
      //val parquetIngestor = new KustoParquetIngestor(ingestionProperties)
      partitionResult.foreach(rowIterator => {
        val fileName = rowIterator.getString(0)
        val fullPath = s"$blobFileBase$blobPath/$fileName"
        parquetIngestor.ingest(fullPath)
      })
    })
  }

  private[kusto] def getWasbHadoopConfig(hadoopConfig: Configuration): Configuration = {
    hadoopConfig.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig
  }

  private[kusto] def setUpAccessForAzureFS(hadoopConfig: Configuration): Unit = {
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

package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.ingest.{IngestionMapping, IngestionProperties}
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.azure.kusto.ingest.result.IngestionResult
import com.microsoft.azure.storage.blob.CloudBlockBlob
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.FinalizeHelper.finalizeIngestionWhenWorkersSucceeded
import com.microsoft.kusto.spark.datasink.SparkIngestionProperties.{ingestionPropertiesFromString, ingestionPropertiesToString, toIngestionProperties}
import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableGetSchemaAsRowsCommand
import com.microsoft.kusto.spark.utils.KustoConstants.{IngestSkippedTrace, MaxIngestRetryAttempts}
import com.microsoft.kusto.spark.utils.KustoIngestionUtils.{adjustSchema, stringToMapping}
import com.microsoft.kusto.spark.utils._
import io.github.resilience4j.retry.RetryConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.{DateTime, DateTimeZone}
import org.json.JSONObject

import java.io.BufferedWriter
import java.net.URI
import java.security.InvalidParameterException
import java.util.UUID
import java.util.zip.GZIPOutputStream
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/*
  Serializes and writes data as a parquet blob
*/
class KustoParquetWriter {
  private val className = this.getClass.getName

  def write(batchId: Option[Long],
            data: DataFrame,
            tableCoordinates: KustoCoordinates,
            authentication: KustoAuthentication,
            writeOptions: WriteOptions,
            crp: ClientRequestProperties): Unit = {
    val sparkContext = data.sparkSession.sparkContext
    val batchIdIfExists = batchId.map(b => s",batch: ${b.toString}").getOrElse("")
    val kustoClient = KustoClientCache.getClient(tableCoordinates.clusterUrl, authentication,
      tableCoordinates.ingestionUrl, tableCoordinates.clusterAlias)
    val table = tableCoordinates.table.getOrElse("")
    val tmpTableName: String = KustoDataSourceUtils.generateTempTableName(sparkContext.appName, table,
      writeOptions.requestId, batchIdIfExists, writeOptions.maybeUserTempTableName)
    // Get and create/adjust schema if the final table exists
    val schemaShowCommandResult = kustoClient.executeEngine(tableCoordinates.database,
      generateTableGetSchemaAsRowsCommand(tableCoordinates.table.get), crp).getPrimaryResults
    val targetSchema = schemaShowCommandResult.getData.asScala.map(c => c.get(0).asInstanceOf[JSONObject]).toArray
    val stagingTableIngestionProperties = adjustSchema(writeOptions.adjustSchema, data.schema, targetSchema,
      sparkIngestionPropertiesFromWriteOptions(writeOptions))
    val tableExists = schemaShowCommandResult.count() > 0
    val shouldIngest = kustoClient.shouldIngestData(tableCoordinates, writeOptions.maybeSparkIngestionProperties, tableExists, crp)
    if (shouldIngest) {
      // If the temp table already exists , then creation of the temp table will fail (and also cause schema mismatches)
      // Throw an exception back and report that this table needs to be dropped.
      val updatedStagingTableIngestionProperties = writeOptions.maybeUserTempTableName match {
        case Some(userTempTable) => if (kustoClient.executeEngine(tableCoordinates.database,
          generateTableGetSchemaAsRowsCommand(userTempTable), crp).getPrimaryResults.count() <= 0 ||
          !tableExists) {
          throw new InvalidParameterException("Temp table name provided but the table does not exist. Either drop this " +
            "option or create the table beforehand.")
        }
        stagingTableIngestionProperties
        case None =>
          // KustoWriter will create a temporary table ingesting the data to it.
          // Only if all executors succeeded the table will be appended to the original destination table.
          kustoClient.initializeTablesBySchema(tableCoordinates, tmpTableName, data.schema, targetSchema, writeOptions,
            crp, stagingTableIngestionProperties.maybeCreationTime.isEmpty)
          // Get the schema if the table is newly created
          if (targetSchema.isEmpty && writeOptions.tableCreateOptions != SinkTableCreationMode.FailIfNotExist) {
            val schemaShowCommandResult = kustoClient.executeEngine(tableCoordinates.database,
              generateTableGetSchemaAsRowsCommand(tableCoordinates.table.get), crp).getPrimaryResults
            val newTargetSchema = schemaShowCommandResult.getData.asScala.
              map(c => c.get(0).asInstanceOf[JSONObject]).toArray
            adjustSchema(SchemaAdjustmentMode.GenerateDynamicParquetMapping, data.schema,
              newTargetSchema, stagingTableIngestionProperties)
          }else {
            stagingTableIngestionProperties
          }
      }
      if (stagingTableIngestionProperties.flushImmediately) {
        KustoDataSourceUtils.logWarn(className, "It's not recommended to set flushImmediately to true")
      }
      // Rebuild writeOptions with modified ingest properties
      val updatedSparkIngestionProperties = ingestionPropertiesToString(updatedStagingTableIngestionProperties)
      val rebuiltOptions = writeOptions.copy(maybeSparkIngestionProperties = Some(updatedSparkIngestionProperties))
/* TODO
      kustoClient.setMappingOnStagingTable(updatedStagingTableIngestionProperties,
        tableCoordinates.database, tmpTableName, table, crp)
*/
      if (stagingTableIngestionProperties.flushImmediately) {
        KustoDataSourceUtils.logWarn(className, "It's not recommended to set flushImmediately to true")
      }
      val transactionWriteParams = TransactionWriteParams(tableCoordinates, authentication, rebuiltOptions, crp,
        batchIdIfExists,tmpTableName, tableExists)
      performWrite(data, transactionWriteParams)
    }
    else {
      KustoDataSourceUtils.logInfo(className, s"$IngestSkippedTrace '$table'")
    }
  }

  private def performWrite(data: DataFrame, transactionWriteParams: TransactionWriteParams): Unit = {
    if (transactionWriteParams.writeOptions.isTransactionalMode) {
      performTransactionalWrite(data, transactionWriteParams)
    } else {
      // call queued ingestion
      val rdd = data.queryExecution.toRdd
      rdd.foreachPartitionAsync { rows =>
        rows.foreach(_ => {
        })
      }
    }
  }

  private def performTransactionalWrite(data: DataFrame, transactionWriteParams: TransactionWriteParams): Unit = {
    // get container details - TODO don't use magic numbers like (1)
    val kustoClient = KustoClientCache.getClient(transactionWriteParams.tableCoordinates.clusterUrl,
      transactionWriteParams.authentication, transactionWriteParams.tableCoordinates.ingestionUrl,
      transactionWriteParams.tableCoordinates.clusterAlias)
    val storageCredentials = kustoClient.getTempBlobsForExport.storageCredentials(1)

    val blobName = s"${KustoQueryUtils.simplifyName(transactionWriteParams.tableCoordinates.database)}_${transactionWriteParams.tmpTableName}_" +
      s"${UUID.randomUUID.toString}_${TaskContext.getPartitionId}_spark.parquet"
    // set up the access in spark config
    val hadoopConfig = setUpAccessForAzureFS(getWasbHadoopConfig(data.sparkSession.sparkContext.hadoopConfiguration), storageCredentials)
    // create the blob file path
    val blobRoot = s"wasbs://${storageCredentials.blobContainer}@${storageCredentials.storageAccountName}.blob.${storageCredentials.domainSuffix}"
    val blobNamePath = s"/$blobName"
    // write data to blob
    data.write.parquet(s"$blobRoot$blobNamePath")
    // Enumerate the list of files
    val listOfFiles = getListOfFileFromDirectory(blobRoot, blobNamePath, hadoopConfig)
    // ingest blobs to kusto table
    if (listOfFiles.nonEmpty) {
      val spark = SparkSession.builder.config(data.rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val  listOfFilesToProcess = listOfFiles.toDF()
      // TODO use of magic numbers (0)
      val blobFileBase = s"https://${storageCredentials.storageAccountName}.blob." +
        s"${storageCredentials.domainSuffix}/${storageCredentials.blobContainer.split("/")(0)}"
      Try(
        ingestAndFinalizeData(listOfFilesToProcess.rdd, s"$blobFileBase$blobNamePath", transactionWriteParams)
      ) match {
        case Success(_) => KustoDataSourceUtils.logDebug(className, s"Ingestion completed for blob $blobFileBase")
        case Failure(exception) => if (transactionWriteParams.writeOptions.isTransactionalMode) {
          if (transactionWriteParams.writeOptions.maybeUserTempTableName.isEmpty) {
            kustoClient.cleanupIngestionByProducts(
              transactionWriteParams.tableCoordinates.database, transactionWriteParams.tmpTableName,
              transactionWriteParams.crp)
          }
          throw exception
        }
      }
    }
  }

  private def ingestAndFinalizeData(rdd: RDD[Row], blobDirPath: String,
                                    transactionWriteParams: TransactionWriteParams): Unit = {
    val partitionId = TaskContext.getPartitionId
    rdd.foreachPartition(partition => {
      // Create the client
      val sparkContext = SparkContext.getOrCreate()
      val partitionResult = sparkContext.collectionAccumulator[PartitionResult]
      val tableName = if (transactionWriteParams.writeOptions.isTransactionalMode) {
        transactionWriteParams.tmpTableName
      } else {
        transactionWriteParams.tableCoordinates.table.getOrElse(transactionWriteParams.tmpTableName)
      }
      // Get the ingestion properties
      val ingestionProperties = transactionWriteParams.writeOptions.maybeSparkIngestionProperties match {
        case Some(ip) => toIngestionProperties(transactionWriteParams.tableCoordinates.database, tableName,
          ingestionPropertiesFromString(ip))
        case None => new IngestionProperties(transactionWriteParams.tableCoordinates.database, tableName)
      }
      ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE)
      ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
      ingestionProperties.setDataFormat(DataFormat.PARQUET.name)
      // Clone the properties and set flush immediately
      val clonedIngestionProperties = if (!ingestionProperties.getFlushImmediately) {
        // Need to copy the ingestionProperties so that only this one will be flushed immediately
        val copiedProperties = new IngestionProperties(ingestionProperties)
        copiedProperties.setFlushImmediately(true)
        copiedProperties
      } else {
        ingestionProperties
      }
      partition.foreach(rowIterator => {
        val fileName = rowIterator.getString(0)
        val fullPath = s"$blobDirPath/$fileName"
        val ingestClient = KustoClientCache.getClient(transactionWriteParams.tableCoordinates.clusterUrl,
          transactionWriteParams.authentication,
          transactionWriteParams.tableCoordinates.ingestionUrl,
          transactionWriteParams.tableCoordinates.clusterAlias).ingestClient
        val parquetIngestor = new KustoParquetIngestor(ingestClient)
        val retryConfig = RetryConfig.custom.maxAttempts(MaxIngestRetryAttempts).
          retryExceptions(classOf[IngestionServiceException]).build
        partitionResult.add(
          PartitionResult(
            KustoDataSourceUtils.retryFunction(() => {
              parquetIngestor.ingest(fullPath, clonedIngestionProperties)
            }, retryConfig, "Ingest into Kusto"),
            partitionId)
        )
      })
      finalizeIngestionWhenWorkersSucceeded(partitionResult, sparkContext, transactionWriteParams)
    })
  }

  private def getListOfFileFromDirectory(blobRoot: String, blobNamePath: String, hadoopConfig: Configuration): ListBuffer[String] = {
    // Get the list of files created. This will be used for ingestion
    val hdfs = FileSystem.get(new URI(blobRoot), hadoopConfig)
    val sourcePath = new Path(blobNamePath)
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
    listOfFiles
  }


  private[kusto] def getWasbHadoopConfig(hadoopConfig: Configuration): Configuration = {
    hadoopConfig.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    hadoopConfig
  }

  private[kusto] def setUpAccessForAzureFS(hadoopConfig: Configuration, storageCredentials: TransientStorageCredentials): Configuration = {
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
    hadoopConfig
  }

  private def getIngestionProperties(writeOptions: WriteOptions, database: String, tableName: String): IngestionProperties = {
    writeOptions.maybeSparkIngestionProperties match {
      case Some(value) => toIngestionProperties(database, tableName, ingestionPropertiesFromString(value))
      case None => new IngestionProperties(database, tableName)
    }
  }
  private def sparkIngestionPropertiesFromWriteOptions(writeOptions: WriteOptions): SparkIngestionProperties = {
    writeOptions.maybeSparkIngestionProperties match {
      case Some(sip) => ingestionPropertiesFromString(sip)
      case None => new SparkIngestionProperties()
    }
  }
}
final case class PartitionResult(ingestionResult: IngestionResult, partitionId: Int)

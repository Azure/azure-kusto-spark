package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.ingest.IngestionProperties
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.FinalizeHelper.finalizeIngestionWhenWorkersSucceeded
import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableGetSchemaAsRowsCommand
import com.microsoft.kusto.spark.utils.KustoConstants.{IngestSkippedTrace, MaxIngestRetryAttempts}
import com.microsoft.kusto.spark.utils._
import io.github.resilience4j.retry.RetryConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{TaskContext, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import org.joda.time.{DateTime, DateTimeZone}
import org.json.JSONObject

import java.net.URI
import java.security.InvalidParameterException
import java.util
import java.util.UUID
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/*
  Serializes and writes data as a parquet blob
*/
class KustoParquetWriter() {
  val LegacyTempIngestionTablePrefix = "_tmpTable"
  val TempIngestionTablePrefix = "sparkTempTable_"
  private val className = this.getClass.getName
  private val retryConfig = RetryConfig.custom.maxAttempts(MaxIngestRetryAttempts).retryExceptions(classOf[IngestionServiceException]).build
  private val myName = this.getClass.getSimpleName

  def write(batchId: Option[Long],
            data: DataFrame,
            tableCoordinates: KustoCoordinates,
            authentication: KustoAuthentication,
            writeOptions: WriteOptions,
            crp: ClientRequestProperties): Unit = {
    val sparkContext = data.sparkSession.sparkContext
    val batchIdIfExists = batchId.map(b => s",batch: ${b.toString}").getOrElse("")
    val kustoClient = KustoClientCache.getClient(tableCoordinates.clusterUrl, authentication, tableCoordinates.ingestionUrl, tableCoordinates.clusterAlias)
    val table = tableCoordinates.table.get

    val tmpTableName: String = KustoDataSourceUtils.generateTempTableName(sparkContext.appName, table,
      writeOptions.requestId, batchIdIfExists, writeOptions.userTempTableName)

    val stagingTableIngestionProperties = getSparkIngestionProperties(writeOptions)
    val schemaShowCommandResult = kustoClient.executeEngine(tableCoordinates.database,
      generateTableGetSchemaAsRowsCommand(tableCoordinates.table.get), crp).getPrimaryResults

    val targetSchema = schemaShowCommandResult.getData.asScala.map(c => c.get(0).asInstanceOf[JSONObject]).toArray
    KustoIngestionUtils.adjustSchema(writeOptions.adjustSchema, data.schema, targetSchema, stagingTableIngestionProperties)
    val rebuiltOptions = writeOptions.copy(ingestionProperties = Some(stagingTableIngestionProperties.toString()))
    val tableExists = schemaShowCommandResult.count() > 0
    val shouldIngest = kustoClient.shouldIngestData(tableCoordinates, writeOptions.ingestionProperties, tableExists, crp)

    if (!shouldIngest) {
      KustoDataSourceUtils.logInfo(myName, s"$IngestSkippedTrace '$table'")
      return
    }
    if (writeOptions.userTempTableName.isDefined) {
      if (kustoClient.executeEngine(tableCoordinates.database,
        generateTableGetSchemaAsRowsCommand(writeOptions.userTempTableName.get), crp).getPrimaryResults.count() <= 0 ||
        !tableExists) {
        throw new InvalidParameterException("Temp table name provided but the table does not exist. Either drop this " +
          "option or create the table beforehand.")
      }
    } else {
      // KustoWriter will create a temporary table ingesting the data to it.
      // Only if all executors succeeded the table will be appended to the original destination table.
      kustoClient.initializeTablesBySchema(tableCoordinates, tmpTableName, data.schema, targetSchema, writeOptions,
        crp, stagingTableIngestionProperties.creationTime == null)
    }

    kustoClient.setMappingOnStagingTableIfNeeded(stagingTableIngestionProperties, tableCoordinates.database,
      tmpTableName, table, crp)

    if (stagingTableIngestionProperties.flushImmediately) {
      KustoDataSourceUtils.logWarn(myName, "It's not recommended to set flushImmediately to true")
    }

    val ingestionProperties = getIngestionProperties(writeOptions,
      tableCoordinates.database,
      if (writeOptions.isTransactionalMode) tmpTableName else tableCoordinates.table.getOrElse(tmpTableName))

    if (writeOptions.isTransactionalMode) {
      performTransactionalWrite(data, tableCoordinates, authentication, rebuiltOptions, crp,
        batchIdIfExists, kustoClient, tmpTableName, tableExists, ingestionProperties)
    } else {
      //call queued ingestion
      val ingestClient = KustoClientCache.getClient(tableCoordinates.clusterUrl, authentication,
        tableCoordinates.ingestionUrl,tableCoordinates.clusterAlias).ingestClient
      ingestionProperties.setDataFormat(DataFormat.PARQUET.name)
      //ingestClient.in
      val rdd = data.queryExecution.toRdd
      rdd.foreachPartitionAsync { rows =>
        (
          rows.foreach(row => {
          })
          )
      }
    }
  }

  private def performTransactionalWrite(data: DataFrame, tableCoordinates: KustoCoordinates,
                                        authentication: KustoAuthentication, writeOptions: WriteOptions,
                                        crp: ClientRequestProperties,
                                        batchIdIfExists: String, kustoClient: ExtendedKustoClient,
                                        tmpTableName: String, tableExists: Boolean,
                                        ingestionProperties: IngestionProperties): Unit = {
    val sparkContext = data.sparkSession.sparkContext
    // get container details
    val storageCredentials =  KustoClientCache.getClient(tableCoordinates.clusterUrl, authentication, tableCoordinates.ingestionUrl, tableCoordinates.clusterAlias).getTempBlobsForExport.storageCredentials(1)

    val blobName = s"${KustoQueryUtils.simplifyName(tableCoordinates.database)}_${tmpTableName}_" +
      s"${UUID.randomUUID.toString}_${TaskContext.getPartitionId}_spark.parquet"
    // set up the access in spark config
    val hadoopConfig = setUpAccessForAzureFS(getWasbHadoopConfig(sparkContext.hadoopConfiguration),storageCredentials)
    // create the blob file path
    val blobRoot = s"wasbs://${storageCredentials.blobContainer}@${storageCredentials.storageAccountName}.blob.${storageCredentials.domainSuffix}"
    val blobNamePath = s"/$blobName"
    // write data to blob
    writeToBlob(data, s"$blobRoot$blobNamePath")
    // Enumerate the list of files
    val listOfFiles = getListOfFileFromDirectory(blobRoot, blobNamePath, hadoopConfig)
    // ingest blobs to kusto table
    if (listOfFiles.nonEmpty) {
      import data.sparkSession.implicits._
      // TODO we need to iterate the dataFrame below ??
      val listOfFilesToProcess = listOfFiles.toDF()
      val blobFileBase = s"https://${storageCredentials.storageAccountName}.blob.${storageCredentials.domainSuffix}/${storageCredentials.blobContainer.split("/")(0)}"
      ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE)
      ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
      ingestionProperties.setDataFormat(DataFormat.PARQUET.name)

      try {
        getPartitionResultsPostIngestion(listOfFilesToProcess.rdd,
          tableCoordinates, authentication, s"$blobFileBase$blobNamePath",writeOptions, tmpTableName, batchIdIfExists, crp, tableExists, sparkContext )
      }
      catch {
        case exception: Exception => if (writeOptions.isTransactionalMode) {
          if (writeOptions.userTempTableName.isEmpty) {
            kustoClient.cleanupIngestionByProducts(
              tableCoordinates.database, tmpTableName, crp)
          }
          throw exception
        }
      }
    }
  }

  private def getPartitionResultsPostIngestion(rdd: RDD[Row],
                                               tableCoordinates: KustoCoordinates, authentication: KustoAuthentication,
                                               blobDirPath: String, writeOptions: WriteOptions, tmpTableName : String,
                                               batchIdIfExists : String, crp : ClientRequestProperties, tableExists: Boolean , sparkContext: SparkContext ) = {
    val partitionId = TaskContext.getPartitionId
    val partitionIdString = TaskContext.getPartitionId.toString

    val database = tableCoordinates.database
    val clusterUrl = tableCoordinates.clusterUrl
    val ingestionUrl = tableCoordinates.ingestionUrl
    val clusterAlias = tableCoordinates.clusterAlias
    val isTransactionalMode = writeOptions.isTransactionalMode

    rdd.foreachPartition(partition => {
      val sparkContext = SparkContext.getOrCreate()
      val kustoClient = KustoClientCache.getClient(tableCoordinates.clusterUrl, authentication, tableCoordinates.ingestionUrl, tableCoordinates.clusterAlias)
      val partitionResult = sparkContext.collectionAccumulator[PartitionResult]
      val tableName = if (writeOptions.isTransactionalMode) tmpTableName else tableCoordinates.table.getOrElse(tmpTableName)
      val ingestionProperties = SparkIngestionProperties.fromString(writeOptions.ingestionProperties.get).toIngestionProperties(database, tableName)

      //val ingestionProperties = new IngestionProperties(database,table)
      var props = ingestionProperties
      if (!ingestionProperties.getFlushImmediately) {
        // Need to copy the ingestionProperties so that only this one will be flushed immediately
        props = SparkIngestionProperties.cloneIngestionProperties(ingestionProperties)
        props.setFlushImmediately(true)
      }
      partition.foreach(rowIterator => {
        val fileName = rowIterator.getString(0)
        val fullPath = s"$blobDirPath/$fileName"
        val ingestClient = KustoClientCache.getClient(clusterUrl,
          authentication,
          ingestionUrl,
          clusterAlias).ingestClient
        val parquetIngestor = new KustoParquetIngestor(ingestClient)
        val retryConfig = RetryConfig.custom.maxAttempts(MaxIngestRetryAttempts).retryExceptions(classOf[IngestionServiceException]).build
        partitionResult.add(
          PartitionResult(
            KustoDataSourceUtils.retryFunction(() => {
              //KustoDataSourceUtils.logInfo(myName, s"Queued blob for ingestion in partition $partitionIdString for requestId: '$requestId}")
              parquetIngestor.ingest(fullPath, props)
            }, retryConfig, "Ingest into Kusto"),
            partitionId)
        )
      })
      /*finalizeIngestionWhenWorkersSucceeded(
        tableCoordinates, batchIdIfExists, tmpTableName,partitionResult , writeOptions,
        crp, tableExists, sparkContext , authentication, kustoClient)*/
    })
  }

  private def getIngestionProperties(writeOptions: WriteOptions, database: String, tableName: String): IngestionProperties = {
    if (writeOptions.ingestionProperties.isDefined) {
      val ingestionProperties = SparkIngestionProperties.fromString(writeOptions.ingestionProperties.get)
        .toIngestionProperties(database, tableName)

      ingestionProperties
    } else {
      new IngestionProperties(database, tableName)
    }
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

  private def getContainerDetails(clusterUrl: String, authentication: KustoAuthentication, maybeIngestionUrl: Option[String]
                                  , clusterAlias: String): ContainerAndSas = {
    KustoClientCache.getClient(clusterUrl, authentication, maybeIngestionUrl, clusterAlias).getTempBlobForIngestion
  }

  private def writeToBlob(data: DataFrame, blobPath: String): Unit = {
    // Write the file
    data.write.parquet(blobPath)
  }

  private def getSparkIngestionProperties(writeOptions: WriteOptions): SparkIngestionProperties = {
    val ingestionProperties = if (writeOptions.ingestionProperties.isDefined)
      SparkIngestionProperties.fromString(writeOptions.ingestionProperties.get)
    else
      new SparkIngestionProperties()
    ingestionProperties.ingestIfNotExists = new util.ArrayList()

    ingestionProperties
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
}

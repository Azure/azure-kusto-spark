package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.ingest.IngestionProperties
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.azure.storage.blob.CloudBlockBlob
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.FinalizeHelper.finalizeIngestionWhenWorkersSucceeded
import com.microsoft.kusto.spark.datasource.TransientStorageCredentials
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableGetSchemaAsRowsCommand
import com.microsoft.kusto.spark.utils.KustoConstants.{IngestSkippedTrace, MaxIngestRetryAttempts}
import com.microsoft.kusto.spark.utils.{ContainerAndSas, KustoAzureFsSetupCache, KustoClientCache, KustoDataSourceUtils, KustoIngestionUtils, KustoQueryUtils}
import io.github.resilience4j.retry.RetryConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
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
  private val className = this.getClass.getName

  private val retryConfig = RetryConfig.custom.maxAttempts(MaxIngestRetryAttempts).retryExceptions(classOf[IngestionServiceException]).build
  private val myName = this.getClass.getSimpleName
  val LegacyTempIngestionTablePrefix = "_tmpTable"
  val TempIngestionTablePrefix = "sparkTempTable_"

  def write(batchId: Option[Long],
            data: DataFrame,
            tableCoordinates: KustoCoordinates,
            authentication: KustoAuthentication,
            writeOptions: WriteOptions,
            crp: ClientRequestProperties ): Unit = {
    val sparkSession = data.sparkSession
    val batchIdIfExists = batchId.map(b => s",batch: ${b.toString}").getOrElse("")
    val kustoClient = KustoClientCache.getClient(tableCoordinates.clusterUrl, authentication, tableCoordinates.ingestionUrl, tableCoordinates.clusterAlias)
    val table = tableCoordinates.table.get
    val tmpTableName: String = KustoDataSourceUtils.generateTempTableName(sparkSession.sparkContext.appName, table,
      writeOptions.requestId, batchIdIfExists, writeOptions.userTempTableName)

    val stagingTableIngestionProperties = getSparkIngestionProperties(writeOptions)
    val schemaShowCommandResult = kustoClient.executeEngine(tableCoordinates.database,
      generateTableGetSchemaAsRowsCommand(tableCoordinates.table.get), crp).getPrimaryResults

    val targetSchema = schemaShowCommandResult.getData.asScala.map(c => c.get(0).asInstanceOf[JSONObject]).toArray
    KustoIngestionUtils.adjustSchema(writeOptions.adjustSchema, data.schema, targetSchema, stagingTableIngestionProperties)

    val rebuiltOptions = writeOptions.copy(ingestionProperties = Some(stagingTableIngestionProperties.toString()))

    implicit val parameters: KustoWriteResource = KustoWriteResource(authentication, tableCoordinates, data.schema,
      rebuiltOptions, tmpTableName)

    val tableExists = schemaShowCommandResult.count() > 0
    val shouldIngest = kustoClient.shouldIngestData(tableCoordinates, writeOptions.ingestionProperties, tableExists,
      crp)

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

    kustoClient.setMappingOnStagingTableIfNeeded(stagingTableIngestionProperties, tableCoordinates.database, tmpTableName, table, crp)
    if (stagingTableIngestionProperties.flushImmediately) {
      KustoDataSourceUtils.logWarn(myName, "It's not recommended to set flushImmediately to true")
    }

    val ingestionProperties = getIngestionProperties(writeOptions,
      parameters.coordinates.database,
      if (writeOptions.isTransactionalMode) parameters.tmpTableName else parameters.coordinates.table.get)

    if(writeOptions.isTransactionalMode) {

      // get container details
      val containerAndSas = getContainerDetails()

      val blobName = s"${KustoQueryUtils.simplifyName(tableCoordinates.database)}_${tmpTableName}_${UUID.randomUUID.toString}_${TaskContext.getPartitionId}_spark.parquet"
      val currentBlobURI = new URI(s"${containerAndSas.containerUrl}/$blobName${containerAndSas.sas}")

      val storageCredentials = new TransientStorageCredentials(currentBlobURI.toString)
      //storageCredentials.parseSas(currentBlobURI.toString)

      // set up the access in spark config
      val hadoopConfig = getWasbHadoopConfig(sparkSession.sparkContext.hadoopConfiguration)
      println(s"------------=========>>>>>>>>>>> HERE IS HCONF: ${hadoopConfig.toString} ")
      setUpAccessForAzureFS(hadoopConfig, storageCredentials)
      // create the blob file path
      val blobRoot = s"wasbs://${storageCredentials.blobContainer.split("/")(0)}@${storageCredentials.storageAccountName}.blob.${storageCredentials.domainSuffix}"
      val blobNamePath = s"/$blobName"

      println(s"------------=========>>>>>>>>>>> HERE IS BLOB: $blobRoot ||  ${containerAndSas.sas} || sas: ${storageCredentials.sasKey} | Hconf : ${hadoopConfig.get(s"fs.azure.sas.${storageCredentials.blobContainer.split("/")(0)}.${storageCredentials.storageAccountName}." +
        s"blob.${storageCredentials.domainSuffix}")}" )
      /*
      *  wasbs://20221019-ingestdata-e5c334ee145d4b4-0/sdktestsdb_sparkTempTable_KustoSink_KustoSparkReadWriteTest_01652358_871e_47c0_b95c_ca3aa2d3895e_04ec0408_3cc3__asd_f5aa396f-7009-429f-bd6d-bd6d3e2b1b03_0_spark.parquet@kxpldsdke2etestcluster00.blob.core.windows.net and /sdktestsdb_sparkTempTable_KustoSink_KustoSparkReadWriteTest_01652358_871e_47c0_b95c_ca3aa2d3895e_04ec0408_3cc3__asd_f5aa396f-7009-429f-bd6d-bd6d3e2b1b03_0_spark.parquet
      *  wasbs://20221019-ingestdata-e5c334ee145d4b4-0/sdktestsdb_sparkTempTable_KustoSink_KustoSparkReadWriteTest_a9eb4bb4_0b1b_495e_b2fb_f1c6cb52310d_04ec0408_3cc3__asd_7a7ed6cb-99b2-4e1e-88af-a9c829dd6df4_0_spark.parquet@kxpldsdke2etestcluster00.blob.core.windows.net and /sdktestsdb_sparkTempTable_KustoSink_KustoSparkReadWriteTest_a9eb4bb4_0b1b_495e_b2fb_f1c6cb52310d_04ec0408_3cc3__asd_7a7ed6cb-99b2-4e1e-88af-a9c829dd6df4_0_spark.parquet and | https://kxpldsdke2etestcluster00.blob.core.windows.net/20221019-ingestdata-e5c334ee145d4b4-0/sdktestsdb_sparkTempTable_KustoSink_KustoSparkReadWriteTest_a9eb4bb4_0b1b_495e_b2fb_f1c6cb52310d_04ec0408_3cc3__asd_7a7ed6cb-99b2-4e1e-88af-a9c829dd6df4_0_spark.parquet?sv=2018-03-28&sr=c&sig=eEhOts2fRiXIMY3F0ohKsxVCl%2Ft78dp3mS3yCTmCvOk%3D&st=2022-10-19T09%3A42%3A03Z&se=2022-10-23T10%3A42%3A03Z&sp=rw
      * */

      // write data to blob
      writeToBlob(data, s"$blobRoot$blobNamePath")
      val listOfFiles = getListOfFileFromDirectory(blobRoot, blobNamePath, hadoopConfig)

      // ingest blobs to kusto table
      if (listOfFiles.nonEmpty) {
        import sparkSession.implicits._
        val listOfFilesToProcess = listOfFiles.toDF()
        val blobFileBase = s"https://${storageCredentials.storageAccountName}.blob.${storageCredentials.domainSuffix}/${storageCredentials.blobContainer.split("/")(0)}"
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE)
        ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
        ingestionProperties.setDataFormat(DataFormat.PARQUET.name)

        val rdd = listOfFilesToProcess.rdd

        try {
          val partitionsResults = getPartitionResultsPostIngestion(rdd, ingestionProperties, s"$blobFileBase$blobNamePath")
          finalizeIngestionWhenWorkersSucceeded(
            tableCoordinates, batchIdIfExists, tmpTableName, partitionsResults, writeOptions,
            crp, tableExists, rdd.sparkContext, authentication, kustoClient)
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

    }else{
      //call queued ingestion
      val ingestClient = KustoClientCache.getClient(parameters.coordinates.clusterUrl,
        parameters.authentication,
        parameters.coordinates.ingestionUrl,
        parameters.coordinates.clusterAlias).ingestClient
      ingestionProperties.setDataFormat(DataFormat.PARQUET.name)
      //ingestClient.in
      val rdd = data.queryExecution.toRdd
      rdd.foreachPartitionAsync { rows => (
        rows.foreach(row => {
        })
      )}
      //ingestClient.ingestFromStream()
    }

    /*************** To be removed **************/
    /*
    // Create the blob file path
    val blobName = s"${KustoQueryUtils.simplifyName(tableCoordinates.database)}_${tmpTableName}_" +
      s"${UUID.randomUUID.toString}_${TaskContext.getPartitionId()}_spark.parquet"
    val blobRoot = s"wasbs://${storageCredentials.blobContainer}@${storageCredentials.storageAccountName}.blob.${storageCredentials.domainSuffix}"
    val blobPath = s"/$blobName"
    // Set up the access in spark config
    val hadoopConfig = getWasbHadoopConfig(sparkSession.sparkContext.hadoopConfiguration)
    setUpAccessForAzureFS(hadoopConfig)
    // Write the file
    data.write.parquet(s"$blobRoot$blobPath")
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
    val sourceSchema = data.schema

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
    })*/
    /****************************************/
  }

  private def getPartitionResultsPostIngestion(rdd: RDD[Row], ingestionProperties: IngestionProperties, blobDirPath: String)
                                              (implicit parameters: KustoWriteResource): CollectionAccumulator[PartitionResult] = {
    val partitionId = TaskContext.getPartitionId
    val partitionIdString = TaskContext.getPartitionId.toString

    val partitionResult = rdd.sparkContext.collectionAccumulator[PartitionResult]

    rdd.foreachPartition(partition => {
      var props = ingestionProperties
      if (!ingestionProperties.getFlushImmediately) {
        // Need to copy the ingestionProperties so that only this one will be flushed immediately
        props = SparkIngestionProperties.cloneIngestionProperties(ingestionProperties)
        props.setFlushImmediately(true)
      }
      partition.foreach(rowIterator => {
        val fileName = rowIterator.getString(0)
        val fullPath = s"$blobDirPath/$fileName"
        val ingestClient = KustoClientCache.getClient(parameters.coordinates.clusterUrl,
          parameters.authentication,
          parameters.coordinates.ingestionUrl,
          parameters.coordinates.clusterAlias).ingestClient
        partitionResult.add(PartitionResult(KustoDataSourceUtils.retryFunction(() => {
          KustoDataSourceUtils.logInfo(myName, s"Queued blob for ingestion in partition $partitionIdString for requestId: '${parameters.writeOptions.requestId}}")
          val parquetIngestor = new KustoParquetIngestor(ingestClient)
          parquetIngestor.ingest(fullPath, ingestionProperties)
        }, this.retryConfig, "Ingest into Kusto"),
          partitionId))
      })
    })
    partitionResult
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

  private def getListOfFileFromDirectory(blobRoot : String, blobNamePath: String , hadoopConfig: Configuration) : ListBuffer[String] = {
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

  private def getContainerDetails()(implicit parameters: KustoWriteResource): ContainerAndSas = {
    import parameters._
    KustoClientCache.getClient(coordinates.clusterUrl, authentication, coordinates.ingestionUrl, coordinates.clusterAlias).getTempBlobForIngestion
  }

  private def writeToBlob(data : DataFrame, blobPath: String) : Unit = {
    // Write the file
    println(s"------------=========>>>>>>>>>>> HERE writing : $blobPath")
    data.write.parquet(blobPath)
    println(s"------------=========>>>>>>>>>>> HERE written")

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

  private[kusto] def setUpAccessForAzureFS(hadoopConfig: Configuration, storageCredentials: TransientStorageCredentials): Unit = {
    val now = DateTime.now(DateTimeZone.UTC)
    if (!storageCredentials.sasDefined) {
      if (!KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(storageCredentials.storageAccountName
        , storageCredentials.storageAccountKey, now)) {
        hadoopConfig.set(s"fs.azure.account.key.${storageCredentials.storageAccountName}." +
          s"blob.${storageCredentials.domainSuffix}",
          s"${storageCredentials.storageAccountKey}")
        println(s"=========>>>>>>>>>>>> setUpAccessForAzureFS: if if:  ${storageCredentials.storageAccountKey} | ----->>> ${storageCredentials.blobContainer}.${storageCredentials.storageAccountName}." +
          s"blob.${storageCredentials.domainSuffix}")

      }
    }
    else {

      if (!KustoAzureFsSetupCache.updateAndGetPrevSas(storageCredentials.blobContainer.split("/")(0),
        storageCredentials.storageAccountName, storageCredentials.sasKey, now)) {
        hadoopConfig.set(s"fs.azure.sas.${storageCredentials.blobContainer.split("/")(0)}.${storageCredentials.storageAccountName}." +
          s"blob.${storageCredentials.domainSuffix}",
          s"${storageCredentials.sasKey}")
        println(s"=========>>>>>>>>>>>> setUpAccessForAzureFS: ELSE if:  ${storageCredentials.sasKey} | ----->>> ${storageCredentials.blobContainer.split("/")(0)}.${storageCredentials.storageAccountName}." +
          s"blob.${storageCredentials.domainSuffix}")
      }
    }
  }
}

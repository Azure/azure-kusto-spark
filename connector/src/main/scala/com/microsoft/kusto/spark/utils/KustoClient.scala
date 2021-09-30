package com.microsoft.kusto.spark.utils

import java.net.SocketTimeoutException
import java.time.Instant
import java.util.StringJoiner
import java.util.concurrent.TimeUnit

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.exceptions.KustoDataException
import com.microsoft.azure.kusto.data.{Client, ClientFactory, ClientRequestProperties, KustoResultSetTable}
import com.microsoft.azure.kusto.ingest.result.{IngestionStatus, OperationStatus}
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestClientFactory}
import com.microsoft.azure.storage.StorageException
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.KustoWriter.DelayPeriodBetweenCalls
import com.microsoft.kusto.spark.datasink.{PartitionResult, SinkTableCreationMode, SparkIngestionProperties, WriteOptions}
import com.microsoft.kusto.spark.datasource.KustoStorageParameters
import com.microsoft.kusto.spark.exceptions.{FailedOperationException, RetriesExhaustedException}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.KustoConstants.{IngestSkippedTrace, MaxSleepOnMoveExtentsMillis}
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils.extractSchemaFromResultTable
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator
import org.json.{JSONArray, JSONObject}
import shaded.parquet.org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, TimeoutException}

class KustoClient(val clusterAlias: String, val engineKcsb: ConnectionStringBuilder, val ingestKcsb: ConnectionStringBuilder) {
  lazy val engineClient: Client = ClientFactory.createClient(engineKcsb)

  // Reading process does not require ingest client to start working
  lazy val dmClient: Client = ClientFactory.createClient(ingestKcsb)
  lazy val ingestClient: IngestClient = IngestClientFactory.createClient(ingestKcsb)
  private val exportProviderEntryCreator = (c: ContainerAndSas) => KDSU.parseSas(c.containerUrl + c.sas)
  private val ingestProviderEntryCreator = (c: ContainerAndSas) => c
  private lazy val ingestContainersContainerProvider = new ContainerProvider(dmClient, clusterAlias, generateCreateTmpStorageCommand(), ingestProviderEntryCreator)
  private lazy val exportContainersContainerProvider = new ContainerProvider(dmClient, clusterAlias, generateGetExportContainersCommand(), exportProviderEntryCreator)

  private val myName = this.getClass.getSimpleName
  private val durationFormat = "dd:HH:mm:ss"

  def initializeTablesBySchema(tableCoordinates: KustoCoordinates,
                               tmpTableName: String,
                               sourceSchema: StructType,
                               targetSchema: Iterable[JSONObject],
                               writeOptions: WriteOptions,
                               crp: ClientRequestProperties,
                               configureRetentionPolicy: Boolean): Unit = {

    var tmpTableSchema: String = ""
    val database = tableCoordinates.database
    val table = tableCoordinates.table.get

    if (targetSchema.isEmpty) {
      // Table Does not exist
      if (writeOptions.tableCreateOptions == SinkTableCreationMode.FailIfNotExist) {
        throw new RuntimeException(s"Table '$table' doesn't exist in database '$database', cluster '${tableCoordinates.clusterAlias} and tableCreateOptions is set to FailIfNotExist.")
      } else {
        // Parse dataframe schema and create a destination table with that schema
        val tableSchemaBuilder = new StringJoiner(",")
        sourceSchema.fields.foreach { field =>
          val fieldType = DataTypeMapping.getSparkTypeToKustoTypeMap(field.dataType)
          tableSchemaBuilder.add(s"['${field.name}']:$fieldType")
        }
        tmpTableSchema = tableSchemaBuilder.toString
        engineClient.execute(database, generateTableCreateCommand(table, tmpTableSchema), crp)
      }
    } else {
      // Table exists. Parse kusto table schema and check if it matches the dataframes schema
      tmpTableSchema = extractSchemaFromResultTable(targetSchema)
    }

    // Create a temporary table with the kusto or dataframe parsed schema with retention and delete set to after the
    // write operation times out. Engine recommended keeping the retention although we use auto delete.
    engineClient.execute(database, generateTempTableCreateCommand(tmpTableName, tmpTableSchema), crp)
    if (configureRetentionPolicy) {
      engineClient.execute(database, generateTableAlterRetentionPolicy(tmpTableName,
        DurationFormatUtils.formatDuration(writeOptions.autoCleanupTime.toMillis, durationFormat, true),
        recoverable = false), crp)
    }
    val instant = Instant.now.plusSeconds(writeOptions.autoCleanupTime.toSeconds)
    engineClient.execute(database, generateTableAlterAutoDeletePolicy(tmpTableName, instant), crp)
  }

  def getTempBlobForIngestion: ContainerAndSas = {
    ingestContainersContainerProvider.getContainer
  }

  def getTempBlobsForExport: Seq[KustoStorageParameters] = {
    exportContainersContainerProvider.getAllContainers
  }

  def handleRetryFail(curBatchSize: Int, retry: Int, currentSleepTime: Int, targetTable: String, error: Object): (Int, Int)
  = {
    KDSU.logWarn(myName,
      s"""moving extents to '$targetTable' failed,
        retry number: $retry ${if (error == null) "" else s", error: ${error.asInstanceOf[String]}"}.
        Sleeping for: $currentSleepTime""")
    Thread.sleep(currentSleepTime)
    val increasedSleepTime = Math.min(MaxSleepOnMoveExtentsMillis, currentSleepTime * 2)
    if (retry % 2 == 1) {
      val reducedBatchSize = Math.max(Math.abs(curBatchSize / 2), 50)
      (reducedBatchSize, increasedSleepTime)
    } else {
      (curBatchSize, increasedSleepTime)
    }
  }

  def handleNoResults(totalAmount: Int, extentsProcessed: Int, database: String, tmpTableName: String,
                      crp: ClientRequestProperties): Boolean = {
    // Could we get here ?
    KDSU.logFatal(myName, "Some extents were not processed and we got an empty move " +
      s"result'${totalAmount - extentsProcessed}' Please open issue if you see this trace. At: https://github" +
      ".com/Azure/azure-kusto-spark/issues")
    val extentsLeftRes = engineClient.execute(database, generateExtentsCountCommand(tmpTableName), crp)
      .getPrimaryResults
    extentsLeftRes.next()

    extentsLeftRes.getInt(0) != 0
  }

  def findErrorInResult(res: KustoResultSetTable): (Boolean, Object) = {
    var error: Object = null
    var failed = false
    if (KDSU.getLoggingLevel == Level.DEBUG) {
      var i = 0
      while (res.next() && !failed) {
        val targetExtent = res.getString(1)
        error = res.getObject(2)
        if (targetExtent == "Failed" || StringUtils.isNotBlank(error.asInstanceOf[String])) {
          failed = true
          if (i > 0) {
            KDSU.logFatal(myName, "Failed extent was not reported on all extents!." +
              "Please open issue if you see this trace. At: https://github.com/Azure/azure-kusto-spark/issues")
          }
        }
        i += 1
      }
    } else {
      if (res.next()) {
        val targetExtent = res.getString(1)
        error = res.getObject(2)
        if (targetExtent == "Failed" || StringUtils.isNotBlank(error.asInstanceOf[String])) {
          failed = true
        }
        // TODO handle specific errors
      }
    }
    (failed, error)
  }

  def moveExtentsWithRetries(batchSize: Int, totalAmount: Int, database: String, tmpTableName: String, targetTable: String,
                             crp: ClientRequestProperties, writeOptions: WriteOptions): Unit = {
    var extentsProcessed = 0
    var retry = 0
    var curBatchSize = batchSize
    var delayPeriodBetweenCalls = DelayPeriodBetweenCalls
    var consecutiveSuccesses = 0
    while (extentsProcessed < totalAmount) {
      var error: Object = null
      var res: Option[KustoResultSetTable] = None
      var failed = false
      // Execute move batch and keep any transient error for handling
      try {
        val operation = engineClient.execute(database, generateTableMoveExtentsAsyncCommand(tmpTableName,
          targetTable, curBatchSize), crp).getPrimaryResults
        val operationResult = KDSU.verifyAsyncCommandCompletion(engineClient, database, operation, samplePeriod =
          KustoConstants
          .DefaultPeriodicSamplePeriod, writeOptions.timeout, s"move extents to destination table '$targetTable' ")
        res = Some(engineClient.execute(database, generateShowOperationDetails(operationResult.get.getString(0)), crp)
          .getPrimaryResults)
        if (res.get.count() == 0) {
          failed = handleNoResults(totalAmount, extentsProcessed, database, tmpTableName, crp)
          if (!failed) {
            // No more extents to move - succeeded
            extentsProcessed = totalAmount
          }
        }
      } catch {
        case ex: FailedOperationException =>
          if (ex.getResult.isDefined) {
            val failedResult: KustoResultSetTable = ex.getResult.get
            if (!failedResult.getBoolean("ShouldRetry")) {
              throw ex
            }

            error = failedResult.getString("Status")
            failed = true
          } else {
            throw ex
          }
        case ex:KustoDataException =>
          if (ex.getCause.isInstanceOf[SocketTimeoutException] || !ex.isPermanent) {
            error = ExceptionUtils.getStackTrace(ex)
            failed = true
          } else {
            throw ex
          }
      }

      // When some node fails the move - it will put "failed" as the target extent id
      if (res.isDefined && error == null) {
        val errorInResult = findErrorInResult(res.get)
        failed = errorInResult._1
        error = errorInResult._2
      }

      if (failed) {
          consecutiveSuccesses = 0
          retry += 1
          if (retry > writeOptions.maxRetriesOnMoveExtents) {
            throw RetriesExhaustedException(s"Failed to move extents after $retry tries")
          }

          // Lower batch size, increase delay
          val params = handleRetryFail(curBatchSize, retry, delayPeriodBetweenCalls, targetTable, error)

          curBatchSize = params._1
          delayPeriodBetweenCalls = params._2
        } else {
          consecutiveSuccesses += 1
          if (consecutiveSuccesses > 2) {
            curBatchSize = Math.min(curBatchSize * 2, batchSize)
          }

          extentsProcessed += res.get.count()
          KDSU.logDebug(myName, s"Moving extents batch succeeded at retry: $retry," +
            s" maxBatch: $curBatchSize, consecutive successfull batches: $consecutiveSuccesses, successes this " +
            s"batch: ${res.get.count()}," +
            s" extentsProcessed: $extentsProcessed, backoff: $delayPeriodBetweenCalls, total:$totalAmount")

          retry = 0
          delayPeriodBetweenCalls = DelayPeriodBetweenCalls
        }
    }
  }

  def moveExtents(database: String, tmpTableName: String, targetTable: String, crp: ClientRequestProperties,
                  writeOptions: WriteOptions): Unit = {
        val extentsCountQuery = engineClient.execute(database, generateExtentsCountCommand(tmpTableName), crp).getPrimaryResults
    extentsCountQuery.next()
    val extentsCount = extentsCountQuery.getInt(0)
    if (extentsCount > writeOptions.minimalExtentsCountForSplitMerge) {
      val nodeCountQuery = engineClient.execute(database, generateNodesCountCommand(), crp).getPrimaryResults
      nodeCountQuery.next()
      val nodeCount = nodeCountQuery.getInt(0)
      moveExtentsWithRetries(nodeCount * writeOptions.minimalExtentsCountForSplitMerge, extentsCount, database,
        tmpTableName, targetTable, crp, writeOptions)
    } else {
      moveExtentsWithRetries(extentsCount, extentsCount, database,
        tmpTableName, targetTable, crp, writeOptions)
    }
  }

  private[kusto] def finalizeIngestionWhenWorkersSucceeded(coordinates: KustoCoordinates,
                                                           batchIdIfExists: String,
                                                           kustoAdminClient: Client,
                                                           tmpTableName: String,
                                                           partitionsResults: CollectionAccumulator[PartitionResult],
                                                           writeOptions: WriteOptions,
                                                           crp: ClientRequestProperties,
                                                           tableExists: Boolean
                                                          ): Unit = {
    if (!shouldIngestData(coordinates, writeOptions.IngestionProperties, tableExists, crp)) {
      KDSU.logInfo(myName, s"$IngestSkippedTrace '${coordinates.table}'")
    } else {
      val mergeTask = Future {
        KDSU.logInfo(myName, s"Polling on ingestion results for requestId: ${writeOptions.requestId}, will move data to " +
          s"destination table when finished")

        try {
          partitionsResults.value.asScala.foreach {
            partitionResult => {
              var finalRes: Option[IngestionStatus] = None
              KDSU.doWhile[Option[IngestionStatus]](
                () => {
                  try {
                    finalRes = Some(partitionResult.ingestionResult.getIngestionStatusCollection.get(0))
                    finalRes
                  } catch {
                    case e: StorageException =>
                      KDSU.logWarn(myName, s"Failed to fetch operation status transiently - will keep polling. " +
                        s"RequestId: ${writeOptions.requestId}. Error: ${ExceptionUtils.getStackTrace(e)}")
                      None
                    case e: Exception => KDSU.reportExceptionAndThrow(myName, e, s"Failed to fetch operation status. RequestId: ${writeOptions.requestId}"); None
                  }
                },
                0,
                DelayPeriodBetweenCalls,
                (writeOptions.timeout.toMillis / DelayPeriodBetweenCalls + 5).toInt,
                res => res.isDefined && res.get.status == OperationStatus.Pending,
                res => finalRes = res,
                maxWaitTimeBetweenCalls = KDSU.WriteMaxWaitTime.toMillis.toInt)
                .await(writeOptions.timeout.toMillis, TimeUnit.MILLISECONDS)

              if (finalRes.isDefined) {
                finalRes.get.status match {
                  case OperationStatus.Pending =>
                    throw new RuntimeException(s"Ingestion to Kusto failed on timeout failure. Cluster: '${coordinates.clusterAlias}', " +
                      s"database: '${coordinates.database}', table: '$tmpTableName'$batchIdIfExists, partition: '${partitionResult.partitionId}'")
                  case OperationStatus.Succeeded =>
                    KDSU.logInfo(myName, s"Ingestion to Kusto succeeded. " +
                      s"Cluster: '${coordinates.clusterAlias}', " +
                      s"database: '${coordinates.database}', " +
                      s"table: '$tmpTableName'$batchIdIfExists, partition: '${partitionResult.partitionId}'', from: '${finalRes.get.ingestionSourcePath}', Operation ${finalRes.get.operationId}")
                  case otherStatus =>
                    throw new RuntimeException(s"Ingestion to Kusto failed with status '$otherStatus'." +
                      s" Cluster: '${coordinates.clusterAlias}', database: '${coordinates.database}', " +
                      s"table: '$tmpTableName'$batchIdIfExists, partition: '${partitionResult.partitionId}'. Ingestion info: '${readIngestionResult(finalRes.get)}'")
                }
              } else {
                throw new RuntimeException("Failed to poll on ingestion status.")
              }
            }
          }

          if (partitionsResults.value.size > 0) {
            // Move data to real table
            // Protect tmp table from merge/rebuild and move data to the table requested by customer. This operation is atomic.
            // We are using the ingestIfNotExists Tags here too (on top of the check at the start of the flow) so that if
            // several flows started together only one of them would ingest
            kustoAdminClient.execute(coordinates.database, generateTableAlterMergePolicyCommand(tmpTableName,
              allowMerge = false,
              allowRebuild = false), crp)
            moveExtents(coordinates.database, tmpTableName, coordinates.table.get, crp, writeOptions)

            KDSU.logInfo(myName, s"write to Kusto table '${coordinates.table.get}' finished successfully " +
              s"requestId: ${writeOptions.requestId} $batchIdIfExists")
          } else {
            KDSU.logWarn(myName, s"write to Kusto table '${coordinates.table.get}' finished with no data written " +
              s"requestId: ${writeOptions.requestId} $batchIdIfExists")
          }
        } catch {
          case ex: Exception =>
            KDSU.reportExceptionAndThrow(
              myName,
              ex,
              "Trying to poll on pending ingestions", coordinates.clusterUrl, coordinates.database, coordinates.table.getOrElse("Unspecified table name")
            )
        } finally {
          cleanupIngestionByProducts(coordinates.database, kustoAdminClient, tmpTableName, crp)
        }
      }

      if (!writeOptions.isAsync) {
        try {
          Await.result(mergeTask, writeOptions.timeout)
        } catch {
          case _: TimeoutException =>
            KDSU.reportExceptionAndThrow(
              myName,
              new TimeoutException("Timed out polling on ingestion status"),
              "polling on ingestion status", coordinates.clusterUrl, coordinates.database, coordinates.table.getOrElse
              ("Unspecified table name"))
        }
      }
    }
  }

  private[kusto] def cleanupIngestionByProducts(database: String, kustoAdminClient: Client, tmpTableName: String,
                                                crp: ClientRequestProperties): Unit = {
    try {
      kustoAdminClient.execute(database, generateTableDropCommand(tmpTableName), crp)
      KDSU.logInfo(myName, s"Temporary table '$tmpTableName' deleted successfully")
    }
    catch {
      case exception: Exception =>
        KDSU.reportExceptionAndThrow(myName, exception, s"deleting temporary table $tmpTableName", database, shouldNotThrow = false)
    }
  }

  private[kusto] def setMappingOnStagingTableIfNeeded(stagingTableSparkIngestionProperties: SparkIngestionProperties,
                                                      database: String,
                                                      tempTable: String,
                                                      originalTable: String,
                                                      crp: ClientRequestProperties): Unit = {
    val stagingTableIngestionProperties = stagingTableSparkIngestionProperties.toIngestionProperties(database, tempTable)
    val mapping = stagingTableIngestionProperties.getIngestionMapping
    val mappingReferenceName = mapping.getIngestionMappingReference
    if (StringUtils.isNotBlank(mappingReferenceName)) {
      val mappingKind = mapping.getIngestionMappingKind.toString
      val cmd = generateShowTableMappingsCommand(originalTable, mappingKind)
      val mappings = engineClient.execute(stagingTableIngestionProperties.getDatabaseName, cmd, crp).getPrimaryResults

      var found = false
      while (mappings.next && !found) {
        if (mappings.getString(0).equals(mappingReferenceName)) {
          val policyJson = mappings.getString(2).replace("\"", "'")
          val cmd = generateCreateTableMappingCommand(stagingTableIngestionProperties.getTableName, mappingKind,
            mappingReferenceName, policyJson)
          engineClient.execute(stagingTableIngestionProperties.getDatabaseName, cmd, crp)
          found = true
        }
      }
    }
  }

  def fetchTableExtentsTags(database: String, table: String, crp: ClientRequestProperties): KustoResultSetTable = {
    engineClient.execute(database, CslCommandsGenerator.generateFetchTableIngestByTagsCommand(table), crp)
      .getPrimaryResults
  }

  def shouldIngestData(tableCoordinates: KustoCoordinates, ingestionProperties: Option[String],
                       tableExists: Boolean, crp: ClientRequestProperties): Boolean = {
    var shouldIngest = true

    if (tableExists && ingestionProperties.isDefined) {
      val ingestIfNotExistsTags = SparkIngestionProperties.fromString(ingestionProperties.get).ingestIfNotExists
      if (ingestIfNotExistsTags != null && !ingestIfNotExistsTags.isEmpty) {
        val ingestIfNotExistsTagsSet = ingestIfNotExistsTags.asScala.toSet

        val res = fetchTableExtentsTags(tableCoordinates.database, tableCoordinates.table.get, crp)
        if (res.next()) {
          val tagsArray = res.getObject(0).asInstanceOf[JSONArray]
          for (i <- 0 until tagsArray.length) {
            if (ingestIfNotExistsTagsSet.contains(tagsArray.getString(i))) {
              shouldIngest = false
            }
          }
        }
      }
    }

    shouldIngest
  }

  private def readIngestionResult(statusRecord: IngestionStatus): String = {
    new ObjectMapper()
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(statusRecord)
  }
}


case class ContainerAndSas(containerUrl: String, sas: String)

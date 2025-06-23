// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.microsoft.azure.kusto.data._
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.exceptions.KustoDataExceptionBase
import com.microsoft.azure.kusto.ingest.resources.ResourceWithSas
import com.microsoft.azure.kusto.ingest.{
  IngestClientFactory,
  ManagedStreamingIngestClient,
  QueuedIngestClient
}
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.KustoWriter.DelayPeriodBetweenCalls
import com.microsoft.kusto.spark.datasink.{
  IngestionStorageParameters,
  SinkTableCreationMode,
  SparkIngestionProperties,
  WriteMode,
  WriteOptions
}
import com.microsoft.kusto.spark.datasource.{
  TransientStorageCredentials,
  TransientStorageParameters
}
import com.microsoft.kusto.spark.exceptions.{FailedOperationException, RetriesExhaustedException}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.KustoConstants.{
  MaxCommandsRetryAttempts,
  MaxSleepOnMoveExtentsMillis
}
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils.extractSchemaFromResultTable
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.retry.RetryConfig
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType

import java.time.{Instant, OffsetDateTime}
import java.util.{StringJoiner, UUID}
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

class ExtendedKustoClient(
    val engineKcsb: ConnectionStringBuilder,
    val ingestKcsb: ConnectionStringBuilder,
    val clusterAlias: String) {
  lazy val engineClient: Client = ClientFactory.createClient(engineKcsb)

  // Reading process does not require ingest client to start working
  lazy val dmClient: Client = ClientFactory.createClient(ingestKcsb)
  lazy val ingestClient: QueuedIngestClient = IngestClientFactory.createClient(ingestKcsb)
  lazy val streamingClient: ManagedStreamingIngestClient =
    IngestClientFactory.createManagedStreamingIngestClient(ingestKcsb, engineKcsb)
  private lazy val ingestContainersContainerProvider =
    new ContainerProvider(this, clusterAlias, generateCreateTmpStorageCommand())
  private lazy val exportContainersContainerProvider =
    new ContainerProvider(this, clusterAlias, generateGetExportContainersCommand())
  RetryConfig.ofDefaults()
  private val retryConfig = buildRetryConfig
  private val retryConfigAsyncOp = buildRetryConfigForAsyncOp

  private val myName = this.getClass.getSimpleName
  private val durationFormat = "dd:HH:mm:ss"

  def initializeTablesBySchema(
      tableCoordinates: KustoCoordinates,
      tmpTableName: String,
      sourceSchema: StructType,
      targetSchema: Iterable[JsonNode],
      writeOptions: WriteOptions,
      crp: ClientRequestProperties,
      configureRetentionPolicy: Boolean): Unit = {

    var tmpTableSchema: String = ""
    val database = tableCoordinates.database
    val table = tableCoordinates.table.get

    if (targetSchema.isEmpty) {
      // Table does not exist
      if (writeOptions.tableCreateOptions == SinkTableCreationMode.FailIfNotExist) {
        throw new RuntimeException(
          s"Table '$table' doesn't exist in database '$database', cluster '${tableCoordinates.clusterAlias} and tableCreateOptions is set to FailIfNotExist.")
      } else {
        // Parse dataframe schema and create a destination table with that schema
        val tableSchemaBuilder = new StringJoiner(",")
        sourceSchema.fields.foreach { field =>
          val fieldType = DataTypeMapping.getSparkTypeToKustoTypeMap(field.dataType)
          tableSchemaBuilder.add(s"['${field.name}']:$fieldType")
        }
        // Add the source transforms as well.
        if (writeOptions.kustoCustomDebugWriteOptions.addSourceLocationTransform) {
          tableSchemaBuilder.add(s"['${KustoConstants.SourceLocationColumnName}']:string")
        }
        tmpTableSchema = tableSchemaBuilder.toString
        executeEngine(
          database,
          generateTableCreateCommand(table, tmpTableSchema),
          "tableCreate",
          crp)
        if (writeOptions.writeMode == WriteMode.KustoStreaming) {
          executeEngine(
            database,
            generateTableAlterStreamIngestionCommand(table),
            "enableStreamingPolicy",
            crp)
          executeEngine(
            database,
            generateClearStreamingIngestionCacheCommand(table),
            "clearStreamingPolicyCache",
            crp)
        }
      }
    } else {
      // Table exists. Parse kusto table schema and check if it matches the dataframes schema
      val transformedTargetSchema = new ObjectMapper().createArrayNode()
      targetSchema.foreach {
        case (value) => {
          transformedTargetSchema.add(value)
        }
      }
      tmpTableSchema = extractSchemaFromResultTable(transformedTargetSchema)
    }

    if (writeOptions.writeMode == WriteMode.Transactional) {
      // Create a temporary table with the kusto or dataframe parsed schema with retention and delete set to after the
      // write operation times out. Engine recommended keeping the retention although we use auto delete.
      executeEngine(
        database,
        generateTempTableCreateCommand(tmpTableName, tmpTableSchema),
        "tableCreate",
        crp)
      val targetTableBatchingPolicyRes = executeEngine(
        database,
        generateTableShowIngestionBatchingPolicyCommand(table),
        "showBatchingPolicy",
        crp).getPrimaryResults
      targetTableBatchingPolicyRes.next()
      val targetTableBatchingPolicy =
        targetTableBatchingPolicyRes.getString(2).replace("\r\n", "").replace("\"", "\"\"")
      if (targetTableBatchingPolicy != null && targetTableBatchingPolicy != "null") {
        executeEngine(
          database,
          generateTableAlterIngestionBatchingPolicyCommand(
            tmpTableName,
            targetTableBatchingPolicy),
          "alterBatchingPolicy",
          crp)
        executeDM(
          generateRefreshBatchingPolicyCommand(database, tmpTableName),
          Some(crp),
          "refreshBatchingPolicy")
      }
      if (configureRetentionPolicy) {
        executeEngine(
          database,
          generateTableAlterRetentionPolicy(
            tmpTableName,
            DurationFormatUtils.formatDuration(
              writeOptions.autoCleanupTime.toMillis,
              durationFormat,
              true),
            recoverable = false),
          "alterRetentionPolicy",
          crp)
        val instant = Instant.now.plusSeconds(writeOptions.autoCleanupTime.toSeconds)
        executeEngine(
          database,
          generateTableAlterAutoDeletePolicy(tmpTableName, instant),
          "alterAutoDelete",
          crp)
      }
      KDSU.logInfo(
        myName,
        s"Successfully created temporary table $tmpTableName, will be deleted after completing the operation")
    }
  }

  def executeDM(
      command: String,
      maybeCrp: Option[ClientRequestProperties],
      activityName: String,
      retryConfig: Option[RetryConfig] = None): KustoOperationResult = {
    KDSU.retryApplyFunction(
      i => {
        dmClient.executeMgmt(
          ExtendedKustoClient.DefaultDb,
          command,
          newIncrementedCrp(maybeCrp, activityName, i))
      },
      retryConfig.getOrElse(this.retryConfig),
      "Execute DM command with retries")
  }

  def executeEngine(
      database: String,
      command: String,
      activityName: String,
      crp: ClientRequestProperties,
      isMgmtCommand: Boolean = true,
      retryConfig: Option[RetryConfig] = None): KustoOperationResult = {

    if (StringUtils.trim(command).startsWith(".") && !isMgmtCommand) {
      KDSU.logWarn(
        myName,
        s"Command '$command' starts with a dot but is not marked as management command. " +
          "This may lead to unexpected behavior. Please ensure the command is intended to be a management command.")
    }

    val isMgmtCommandStr = if (isMgmtCommand || StringUtils.trim(command).startsWith(".")) {
      "management"
    } else {
      "query"
    }
    KDSU.retryApplyFunction(
      retryNumber => {
        if (isMgmtCommand) {
          KDSU.logDebug(
            myName,
            s"Executing $isMgmtCommandStr command: $command, retry number: $retryNumber")
          engineClient.executeMgmt(
            database,
            command,
            newIncrementedCrp(Some(crp), activityName, retryNumber))
        } else {
          KDSU.logDebug(
            myName,
            s"Executing $isMgmtCommandStr command: $command, retry number: $retryNumber")
          engineClient.executeQuery(
            database,
            command,
            newIncrementedCrp(Some(crp), activityName, retryNumber))
        }
      },
      retryConfig.getOrElse(this.retryConfig),
      s"Execute $isMgmtCommandStr command with retries")
  }

  private def newIncrementedCrp(
      maybeCrp: Option[ClientRequestProperties],
      activityName: String,
      iteration: Int): ClientRequestProperties = {
    var prefix: Option[String] = None
    if (maybeCrp.isDefined) {
      val currentId = maybeCrp.get.getClientRequestId
      if (StringUtils.isNoneBlank(currentId)) {
        prefix = Some(currentId + ";")
      }
    }

    val id = s"${prefix.getOrElse("")}$activityName;${UUID.randomUUID()};$iteration"
    val clientRequestProperties = new ClientRequestProperties
    clientRequestProperties.setClientRequestId(id)
    clientRequestProperties
  }

  def getTempBlobForIngestion(
      maybeIngestionStorageParams: Option[Array[IngestionStorageParameters]]): ContainerAndSas = {
    ingestContainersContainerProvider.getContainer(maybeIngestionStorageParams)
  }

  def reportIngestionResult(resource: ResourceWithSas[_], success: Boolean): Unit = {
    try {
      ingestClient.getResourceManager.reportIngestionResult(resource, success)
    } catch {
      case exception: Exception =>
        KDSU.logDebug(
          myName,
          s"Exception in repoting ingestion result : ${exception.getMessage} ")
    }
  }

  def getTempBlobsForExport: TransientStorageParameters = {
    val storage = exportContainersContainerProvider.getExportContainers
    val transientStorage =
      storage.map(c => new TransientStorageCredentials(c.containerUrl + c.sas))
    val endpointSuffix = transientStorage.head.domainSuffix
    if (StringUtils.isNoneBlank(endpointSuffix)) {
      new TransientStorageParameters(transientStorage.toArray, endpointSuffix)
    } else {
      new TransientStorageParameters(transientStorage.toArray)
    }
  }

  def moveExtents(
      database: String,
      tmpTableName: String,
      targetTable: String,
      crp: ClientRequestProperties,
      writeOptions: WriteOptions,
      sinkStartTime: Instant): Unit = {
    val extentsCountQuery =
      executeEngine(
        database,
        generateExtentsCountCommand(tmpTableName),
        "countExtents",
        crp).getPrimaryResults
    extentsCountQuery.next()
    val extentsCount = extentsCountQuery.getInt(0)
    if (extentsCount > writeOptions.kustoCustomDebugWriteOptions.minimalExtentsCountForSplitMergePerNode) {
      val nodeCountQuery =
        executeEngine(database, generateNodesCountCommand(), "nodesCount", crp).getPrimaryResults
      nodeCountQuery.next()
      val nodeCount = nodeCountQuery.getInt(0)
      moveExtentsWithRetries(
        Some(
          nodeCount * writeOptions.kustoCustomDebugWriteOptions.minimalExtentsCountForSplitMergePerNode),
        extentsCount,
        database,
        tmpTableName,
        targetTable,
        sinkStartTime,
        crp,
        writeOptions)
    } else {
      moveExtentsWithRetries(
        None,
        extentsCount,
        database,
        tmpTableName,
        targetTable,
        sinkStartTime,
        crp,
        writeOptions)
    }
  }

  def moveExtentsWithRetries(
      batchSize: Option[Int],
      totalAmount: Int,
      database: String,
      tmpTableName: String,
      targetTable: String,
      ingestionStartTime: Instant,
      crp: ClientRequestProperties,
      writeOptions: WriteOptions): Unit = {
    var extentsProcessed = 0
    var retry = 0
    var curBatchSize = batchSize.getOrElse(0)
    var delayPeriodBetweenCalls = DelayPeriodBetweenCalls
    var consecutiveSuccesses = 0
    val useMaterializedViewFlag = shouldUseMaterializedViewFlag(database, targetTable, crp)
    val firstMoveRetries = writeOptions.kustoCustomDebugWriteOptions.maxRetriesOnMoveExtents
    val secondMovesRetries =
      Math.max(10, writeOptions.kustoCustomDebugWriteOptions.maxRetriesOnMoveExtents)
    while (extentsProcessed < totalAmount) {
      var error: Object = null
      var res: Option[KustoResultSetTable] = None
      var failed = false
      // Execute move batch and keep any transient error for handling
      try {
        val timeRange = Array[Instant](ingestionStartTime, Instant.now())
        val operation = executeEngine(
          database,
          generateTableMoveExtentsAsyncCommand(
            tmpTableName,
            targetTable,
            timeRange,
            if (batchSize.isEmpty) None else Some(curBatchSize),
            useMaterializedViewFlag),
          "extentsMove",
          crp).getPrimaryResults
        val operationResult = KDSU.verifyAsyncCommandCompletion(
          engineClient,
          database,
          operation,
          samplePeriod = KustoConstants.DefaultPeriodicSamplePeriod,
          writeOptions.timeout,
          s"move extents to destination table '$targetTable' ",
          myName,
          writeOptions.requestId)
        // TODO: use count over the show operations
        res = Some(
          executeEngine(
            database,
            generateShowOperationDetails(operationResult.get.getString(0)),
            "operationsDetailsShow",
            crp).getPrimaryResults)
        if (res.get.count() == 0) {
          failed = handleNoResults(totalAmount, extentsProcessed, database, tmpTableName, crp)
          if (!failed) {
            // No more extents to move - succeeded
            extentsProcessed = totalAmount
          }
        }
      } catch {
        // We don't check for the shouldRetry or permanent errors because we know
        // The issue is not with syntax or non-existing tables, it can only be transient
        // issues that might be solved in retries even if engine reports them as permanent
        case ex: FailedOperationException =>
          if (ex.getResult.isDefined) {
            error = ex.getResult.get.getString("Status")
          }
          failed = true
        case ex: KustoDataExceptionBase =>
          error = ExceptionUtils.getStackTrace(ex)
          failed = true
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
        val extentsProcessedErrorString =
          if (extentsProcessed > 0) s"and ${extentsProcessed} were moved" else ""
        if (extentsProcessed > 0) {
          // This is not the first move command
          if (retry > secondMovesRetries)
            throw RetriesExhaustedException(
              s"Failed to move extents after $retry tries$extentsProcessedErrorString.")
        } else if (retry > firstMoveRetries)
          throw RetriesExhaustedException(
            s"Failed to move extents after $retry tries$extentsProcessedErrorString.")

        // Lower batch size, increase delay
        val params =
          handleRetryFail(curBatchSize, retry, delayPeriodBetweenCalls, targetTable, error)

        curBatchSize = params._1
        delayPeriodBetweenCalls = params._2
      } else {
        consecutiveSuccesses += 1
        if (consecutiveSuccesses > 2) {
          // After curBatchSize size has decreased - we can lower it again according to original batch size
          curBatchSize = Math.min(curBatchSize * 2, batchSize.getOrElse(curBatchSize * 2))
        }

        extentsProcessed += res.get.count()
        val batchSizeString = if (batchSize.isDefined) s"maxBatch: $curBatchSize," else ""
        KDSU.logDebug(
          myName,
          s"Moving extents batch succeeded at retry: $retry," +
            s" $batchSizeString consecutive successfull batches: $consecutiveSuccesses, successes this " +
            s"batch: ${res.get.count()}," +
            s" extentsProcessed: $extentsProcessed, backoff: $delayPeriodBetweenCalls, total:$totalAmount")

        retry = 0
        delayPeriodBetweenCalls = DelayPeriodBetweenCalls
      }
    }
  }

  def handleRetryFail(
      curBatchSize: Int,
      retry: Int,
      currentSleepTime: Int,
      targetTable: String,
      error: Object): (Int, Int) = {
    KDSU.logWarn(
      myName,
      s"""moving extents to '$targetTable' failed,
        retry number: $retry ${if (error == null) ""
        else s", error: ${error.asInstanceOf[String]}"}.
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

  def handleNoResults(
      totalAmount: Int,
      extentsProcessed: Int,
      database: String,
      tmpTableName: String,
      crp: ClientRequestProperties): Boolean = {
    // Could we get here ?
    KDSU.logFatal(
      myName,
      "Some extents were not processed and we got an empty move " +
        s"result'${totalAmount - extentsProcessed}' Please open issue if you see this trace. At: https://github" +
        ".com/Azure/azure-kusto-spark/issues")
    val extentsLeftRes =
      executeEngine(
        database,
        generateExtentsCountCommand(tmpTableName),
        "extentsCount",
        crp).getPrimaryResults
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
            KDSU.logFatal(
              myName,
              "Failed extent was not reported on all extents!." +
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

  def shouldUseMaterializedViewFlag(
      database: String,
      targetTable: String,
      crp: ClientRequestProperties): Boolean = {
    val isDestinationTableMaterializedViewSourceResult =
      executeEngine(
        database,
        generateIsTableMaterializedViewSourceCommand(targetTable),
        "isTableMV",
        crp).getPrimaryResults
    isDestinationTableMaterializedViewSourceResult.next()
    val isDestinationTableMaterializedViewSource: Boolean =
      isDestinationTableMaterializedViewSourceResult.getLong(0) > 0
    if (isDestinationTableMaterializedViewSource) {
      val res =
        executeEngine(
          database,
          generateIsTableEngineV3(targetTable),
          "isTableV3",
          crp).getPrimaryResults
      res.next()
      res.getBoolean(0)
    } else {
      false
    }
  }

  def shouldIngestData(
      tableCoordinates: KustoCoordinates,
      maybeSparkIngestionProperties: Option[SparkIngestionProperties],
      tableExists: Boolean,
      crp: ClientRequestProperties): Boolean = {
    if (tableExists && maybeSparkIngestionProperties.isDefined) {
      val ingestIfNotExistsTags = maybeSparkIngestionProperties.orNull.ingestIfNotExists
      if (ingestIfNotExistsTags != null && !ingestIfNotExistsTags.isEmpty) {
        val ingestIfNotExistsTagsSet = ingestIfNotExistsTags.asScala.toSet
        val res =
          fetchTableExtentsTags(tableCoordinates.database, tableCoordinates.table.get, crp)
        if (res.next()) {
          val tagsArray = res.getObject(0).asInstanceOf[ArrayNode]
          for (i <- 0 until tagsArray.size()) {
            if (ingestIfNotExistsTagsSet.contains(tagsArray.get(i).asText())) {
              return false
            }
          }
        }
      }
    }
    true
  }

  def fetchTableExtentsTags(
      database: String,
      table: String,
      crp: ClientRequestProperties): KustoResultSetTable = {
    executeEngine(
      database,
      CslCommandsGenerator.generateFetchTableIngestByTagsCommand(table),
      "fetchTableExtentsTags",
      crp).getPrimaryResults
  }

  def retryAsyncOp(
      database: String,
      cmd: String,
      crp: ClientRequestProperties,
      timeout: FiniteDuration,
      cmdToTrace: String,
      cmdName: String,
      requestId: String): Option[KustoResultSetTable] = {
    KDSU.retryApplyFunction(
      i => {
        val operation = executeEngine(
          database,
          cmd,
          cmdToTrace,
          newIncrementedCrp(Some(crp), cmdName, i)).getPrimaryResults
        KDSU.verifyAsyncCommandCompletion(
          engineClient,
          database,
          operation,
          samplePeriod = KustoConstants.DefaultPeriodicSamplePeriod,
          timeout,
          cmdToTrace,
          myName,
          requestId)
      },
      retryConfigAsyncOp,
      cmdToTrace)
  }

  def buildRetryConfig = {
    val sleepConfig = IntervalFunction.ofExponentialRandomBackoff(
      ExtendedKustoClient.BaseIntervalMs,
      IntervalFunction.DEFAULT_MULTIPLIER,
      IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR,
      ExtendedKustoClient.MaxRetryIntervalMs)
    RetryConfig.custom
      .maxAttempts(MaxCommandsRetryAttempts)
      .intervalFunction(sleepConfig)
      .retryOnException((e: Throwable) =>
        e.isInstanceOf[KustoDataExceptionBase] && !e
          .asInstanceOf[KustoDataExceptionBase]
          .isPermanent)
      .build
  }

  private def buildRetryConfigForAsyncOp = {
    val sleepConfig = IntervalFunction.ofExponentialRandomBackoff(
      ExtendedKustoClient.BaseIntervalMs,
      IntervalFunction.DEFAULT_MULTIPLIER,
      IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR,
      ExtendedKustoClient.MaxRetryIntervalMs)
    RetryConfig.custom
      .maxAttempts(MaxCommandsRetryAttempts)
      .intervalFunction(sleepConfig)
      .retryExceptions(classOf[FailedOperationException])
      .build
  }

  private[kusto] def cleanupIngestionByProducts(
      database: String,
      tmpTableName: String,
      crp: ClientRequestProperties): Unit = {
    try {

      executeEngine(database, generateTableDropCommand(tmpTableName), "tableDrop", crp)
      KDSU.logInfo(myName, s"Temporary table '$tmpTableName' deleted successfully")
    } catch {
      case exception: Exception =>
        KDSU.reportExceptionAndThrow(
          myName,
          exception,
          s"deleting temporary table $tmpTableName",
          database,
          shouldNotThrow = false)
    }
  }

  private[kusto] def setMappingOnStagingTableIfNeeded(
      stagingTableSparkIngestionProperties: SparkIngestionProperties,
      database: String,
      tempTable: String,
      originalTable: String,
      crp: ClientRequestProperties): Unit = {
    val stagingTableIngestionProperties =
      stagingTableSparkIngestionProperties.toIngestionProperties(database, tempTable)
    val mapping = stagingTableIngestionProperties.getIngestionMapping
    val mappingReferenceName = mapping.getIngestionMappingReference
    if (StringUtils.isNotBlank(mappingReferenceName)) {
      val mappingKind = mapping.getIngestionMappingKind.toString
      val cmd = generateShowTableMappingsCommand(originalTable, mappingKind)
      val mappings =
        executeEngine(
          stagingTableIngestionProperties.getDatabaseName,
          cmd,
          "tableMappingsShow",
          crp).getPrimaryResults

      var found = false
      while (mappings.next && !found) {
        if (mappings.getString(0).equals(mappingReferenceName)) {
          val policyJson = mappings.getString(2).replace("\"", "'")
          val cmd = generateCreateTableMappingCommand(
            stagingTableIngestionProperties.getTableName,
            mappingKind,
            mappingReferenceName,
            policyJson)
          executeEngine(
            stagingTableIngestionProperties.getDatabaseName,
            cmd,
            "tableMappingCreate",
            crp)
          found = true
        }
      }
    }
  }
}

object ExtendedKustoClient {
  val DefaultDb: String = "NetDefaultDB"
  val BaseIntervalMs: Long = 1000L
  val MaxRetryIntervalMs: Long = 1000L * 10
}

case class ContainerAndSas(containerUrl: String, sas: String)

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.sink

import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.{KeyVaultAuthentication, KustoAuthentication}
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink._
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableGetSchemaAsRowsCommand
import com.microsoft.kusto.spark.utils.{
  ExtendedKustoClient,
  KeyVaultUtils,
  KustoClientCache,
  OperationMetrics,
  KustoDataSourceUtils => KDSU
}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

import java.security.InvalidParameterException
import java.time.{Clock, Instant}
import java.util
import scala.jdk.CollectionConverters._

final case class KustoWriteBuilder(info: LogicalWriteInfo) extends WriteBuilder {
  private val className = this.getClass.getSimpleName
  private val objectMapper = new ObjectMapper()

  override def buildForBatch(): BatchWrite = {
    val buildStartNanos = System.nanoTime()
    val parameters = info.options().asCaseSensitiveMap().asScala.toMap
    val sinkParameters = KDSU.parseSinkParameters(parameters)
    val sourceParams = sinkParameters.sourceParametersResults

    val keyVaultAuth: Option[KeyVaultAuthentication] = sourceParams.keyVaultAuth
    val authentication: KustoAuthentication =
      if (keyVaultAuth.isDefined) {
        val paramsFromKeyVault =
          KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultAuth.get)
        KDSU.mergeKeyVaultAndOptionsAuthentication(
          paramsFromKeyVault,
          Some(sourceParams.authenticationParameters))
      } else {
        sourceParams.authenticationParameters
      }

    val tableCoordinates = sourceParams.kustoCoordinates
    val writeOptions = sinkParameters.writeOptions
    val crp = sourceParams.clientRequestProperties

    val kustoClient = KustoClientCache.getClient(
      tableCoordinates.clusterUrl,
      authentication,
      tableCoordinates.ingestionUrl,
      tableCoordinates.clusterAlias)

    val table = tableCoordinates.table.get
    val batchIdIfExists = ""
    val tmpTableName: String = KDSU.generateTempTableName(
      s"v2_${info.queryId()}",
      table,
      writeOptions.requestId,
      batchIdIfExists,
      writeOptions.userTempTableName)

    val stagingTableIngestionProperties = getSparkIngestionProperties(writeOptions)

    val schemaShowCommandResult = OperationMetrics.timed(
      className,
      "v2.writeBuilder.schemaShow",
      Map("table" -> table, "requestId" -> writeOptions.requestId)) {
      kustoClient
        .executeEngine(
          tableCoordinates.database,
          generateTableGetSchemaAsRowsCommand(tableCoordinates.table.get),
          "schemaShow",
          crp)
        .getPrimaryResults
    }

    val targetSchema =
      schemaShowCommandResult.getData.asScala
        .map(c => objectMapper.readTree(c.get(0).toString))
        .toArray

    OperationMetrics.timed(
      className,
      "v2.writeBuilder.adjustSchema",
      Map("table" -> table, "requestId" -> writeOptions.requestId)) {
      com.microsoft.kusto.spark.utils.KustoIngestionUtils.adjustSchema(
        writeOptions.writeMode,
        writeOptions.adjustSchema,
        info.schema(),
        targetSchema,
        stagingTableIngestionProperties,
        writeOptions.tableCreateOptions,
        writeOptions.kustoCustomDebugWriteOptions)
    }

    val rebuiltOptions =
      writeOptions.copy(maybeSparkIngestionProperties = Some(stagingTableIngestionProperties))

    val tableExists = schemaShowCommandResult.count() > 0
    val shouldIngest = kustoClient.shouldIngestData(
      tableCoordinates,
      writeOptions.maybeSparkIngestionProperties,
      tableExists,
      crp)

    if (!shouldIngest) {
      KDSU.logInfo(
        className,
        s"${com.microsoft.kusto.spark.utils.KustoConstants.IngestSkippedTrace} '$table'")
      KustoBatchWrite.noOp
    } else {
      OperationMetrics.timed(
        className,
        "v2.writeBuilder.initStagingTable",
        Map(
          "table" -> table,
          "tmpTable" -> tmpTableName,
          "requestId" -> writeOptions.requestId)) {
        initializeStagingTable(
          kustoClient,
          tableCoordinates,
          tmpTableName,
          info.schema(),
          targetSchema,
          writeOptions,
          crp,
          tableExists,
          stagingTableIngestionProperties)
      }

      val sinkStartTime = Option(stagingTableIngestionProperties.creationTime)
        .getOrElse(Instant.now(Clock.systemUTC()))

      OperationMetrics.logMetric(
        className,
        "v2.writeBuilder.buildForBatch",
        (System.nanoTime() - buildStartNanos) / 1000000L,
        Map(
          "table" -> table,
          "writeFormat" -> writeOptions.writeFormat.toString,
          "writeMode" -> writeOptions.writeMode.toString,
          "requestId" -> writeOptions.requestId))

      KustoBatchWrite(
        tableCoordinates = tableCoordinates,
        authentication = authentication,
        writeOptions = rebuiltOptions,
        crp = crp,
        tmpTableName = tmpTableName,
        tableExists = tableExists,
        schema = info.schema(),
        sinkStartTime = sinkStartTime,
        kustoClient = kustoClient,
        batchIdIfExists = batchIdIfExists)
    }
  }

  private def initializeStagingTable(
      kustoClient: ExtendedKustoClient,
      tableCoordinates: KustoCoordinates,
      tmpTableName: String,
      sourceSchema: StructType,
      targetSchema: Array[com.fasterxml.jackson.databind.JsonNode],
      writeOptions: WriteOptions,
      crp: ClientRequestProperties,
      tableExists: Boolean,
      stagingTableIngestionProperties: SparkIngestionProperties): Unit = {

    if (writeOptions.userTempTableName.isDefined) {
      if (kustoClient
          .executeEngine(
            tableCoordinates.database,
            generateTableGetSchemaAsRowsCommand(writeOptions.userTempTableName.get),
            "schemaShow",
            crp)
          .getPrimaryResults
          .count() <= 0 ||
        !tableExists) {
        throw new InvalidParameterException(
          "Temp table name provided but the table does not exist. Either drop this " +
            "option or create the table beforehand.")
      }
    } else {
      kustoClient.initializeTablesBySchema(
        tableCoordinates,
        tmpTableName,
        sourceSchema,
        targetSchema,
        writeOptions,
        crp,
        stagingTableIngestionProperties.creationTime == null)
    }

    if (writeOptions.writeMode == WriteMode.Transactional) {
      kustoClient.setMappingOnStagingTableIfNeeded(
        stagingTableIngestionProperties,
        tableCoordinates.database,
        tmpTableName,
        tableCoordinates.table.get,
        crp)
    }

    if (stagingTableIngestionProperties.flushImmediately) {
      KDSU.logWarn(
        className,
        "It's not recommended to set flushImmediately to true on production")
    }
  }

  private def getSparkIngestionProperties(
      writeOptions: WriteOptions): SparkIngestionProperties = {
    val sparkIngestionProperties =
      writeOptions.maybeSparkIngestionProperties.getOrElse(new SparkIngestionProperties())
    sparkIngestionProperties.ingestIfNotExists = new util.ArrayList()
    sparkIngestionProperties
  }
}

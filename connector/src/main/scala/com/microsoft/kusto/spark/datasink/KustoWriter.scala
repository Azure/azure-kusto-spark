// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.ingest.result.IngestionResult
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.FinalizeHelper.finalizeIngestionWhenWorkersSucceeded
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableGetSchemaAsRowsCommand
import com.microsoft.kusto.spark.utils.KustoConstants.IngestSkippedTrace
import com.microsoft.kusto.spark.utils.{
  KustoClientCache,
  KustoIngestionUtils,
  KustoConstants => KCONST,
  KustoDataSourceUtils => KDSU
}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator

import java.security.InvalidParameterException
import java.time.{Clock, Instant}
import java.util
import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object KustoWriter {
  private val className = this.getClass.getSimpleName
  val TempIngestionTablePrefix = "sparkTempTable_"
  val DelayPeriodBetweenCalls: Int = KCONST.DefaultPeriodicSamplePeriod.toMillis.toInt
  private val objectMapper = new ObjectMapper()

  private[kusto] def write(
      batchId: Option[Long],
      data: DataFrame,
      tableCoordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions,
      crp: ClientRequestProperties): Unit = {
    val batchIdIfExists = batchId.map(b => s"${b.toString}").getOrElse("")
    val kustoClient = KustoClientCache.getClient(
      tableCoordinates.clusterUrl,
      authentication,
      tableCoordinates.ingestionUrl,
      tableCoordinates.clusterAlias)

    val table = tableCoordinates.table.get
    val tmpTableName: String = KDSU.generateTempTableName(
      data.sparkSession.sparkContext.appName,
      table,
      writeOptions.requestId,
      batchIdIfExists,
      writeOptions.userTempTableName)

    val stagingTableIngestionProperties = getSparkIngestionProperties(writeOptions)
    val schemaShowCommandResult = kustoClient
      .executeEngine(
        tableCoordinates.database,
        generateTableGetSchemaAsRowsCommand(tableCoordinates.table.get),
        "schemaShow",
        crp)
      .getPrimaryResults

    // Re-parse the schema JSON through the connector's (potentially shaded) ObjectMapper
    // to avoid ClassCastException when the Kusto SDK returns Jackson objects from a
    // different classloader (e.g. Databricks runtime provides unshaded Jackson while
    // the connector uber-jar shades it).
    val targetSchema =
      schemaShowCommandResult.getData.asScala
        .map(c => objectMapper.readTree(c.get(0).toString))
        .toArray

    KustoIngestionUtils.adjustSchema(
      writeOptions.writeMode,
      writeOptions.adjustSchema,
      data.schema,
      targetSchema,
      stagingTableIngestionProperties,
      writeOptions.tableCreateOptions,
      writeOptions.kustoCustomDebugWriteOptions)

    val rebuiltOptions =
      writeOptions.copy(maybeSparkIngestionProperties = Some(stagingTableIngestionProperties))

    val tableExists = schemaShowCommandResult.count() > 0
    val shouldIngest = kustoClient.shouldIngestData(
      tableCoordinates,
      writeOptions.maybeSparkIngestionProperties,
      tableExists,
      crp)

    if (!shouldIngest) {
      KDSU.logInfo(className, s"$IngestSkippedTrace '$table'")
    } else {
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
          data.schema,
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
          table,
          crp)
      }

      if (stagingTableIngestionProperties.flushImmediately) {
        KDSU.logWarn(
          className,
          "It's not recommended to set flushImmediately to true on production")
      }

      KDSU.logInfo(
        className,
        s"Routing write for table '$table' with writeFormat=${writeOptions.writeFormat}, " +
          s"writeMode=${writeOptions.writeMode}, requestId='${writeOptions.requestId}'")

      writeOptions.writeFormat match {
        case WriteFormat.CSV =>
          writeCSV(
            data,
            tableCoordinates,
            authentication,
            rebuiltOptions,
            crp,
            batchIdIfExists,
            tmpTableName,
            tableExists,
            stagingTableIngestionProperties,
            kustoClient)
        case WriteFormat.Parquet =>
          KustoParquetWriter.write(
            data,
            tableCoordinates,
            authentication,
            rebuiltOptions,
            crp,
            batchIdIfExists,
            tmpTableName,
            tableExists,
            stagingTableIngestionProperties,
            kustoClient)
      }
    }
  }

  private def writeCSV(
      data: DataFrame,
      tableCoordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions,
      crp: ClientRequestProperties,
      batchIdIfExists: String,
      tmpTableName: String,
      tableExists: Boolean,
      stagingTableIngestionProperties: SparkIngestionProperties,
      kustoClient: com.microsoft.kusto.spark.utils.ExtendedKustoClient): Unit = {
    val table = tableCoordinates.table.get
    val rdd = data.queryExecution.toRdd
    val partitionsResults = rdd.sparkContext.collectionAccumulator[PartitionResult]
    val parameters = KustoWriteResource(
      authentication = authentication,
      coordinates = tableCoordinates,
      schema = data.schema,
      writeOptions = writeOptions,
      tmpTableName = tmpTableName)
    val sinkStartTime = getCreationTime(stagingTableIngestionProperties)
    if (writeOptions.isAsync) {
      val asyncWork = rdd.foreachPartitionAsync { rows =>
        KustoCSVWriter.ingestRowsIntoTempTbl(rows, batchIdIfExists, partitionsResults, parameters)
      }
      KDSU.logInfo(className, s"asynchronous write to Kusto table '$table' in progress")

      if (writeOptions.writeMode == WriteMode.Transactional) {
        asyncWork.onComplete {
          case Success(_) =>
            finalizeIngestionWhenWorkersSucceeded(
              tableCoordinates,
              batchIdIfExists,
              tmpTableName,
              partitionsResults,
              writeOptions,
              crp,
              tableExists,
              rdd.sparkContext,
              authentication,
              kustoClient,
              sinkStartTime)
          case Failure(exception) =>
            if (writeOptions.userTempTableName.isEmpty) {
              kustoClient.cleanupIngestionByProducts(tableCoordinates.database, tmpTableName, crp)
            }
            KDSU.reportExceptionAndThrow(
              className,
              exception,
              "writing data",
              tableCoordinates.clusterUrl,
              tableCoordinates.database,
              table,
              shouldNotThrow = true)
            KDSU.logError(
              className,
              "The exception is not visible in the driver since we're in async mode")
        }
      }
    } else {
      try
        rdd.foreachPartition { rows =>
          KustoCSVWriter.ingestRowsIntoTempTbl(
            rows,
            batchIdIfExists,
            partitionsResults,
            parameters)
        }
      catch {
        case exception: Exception =>
          if (writeOptions.writeMode == WriteMode.Transactional) {
            if (writeOptions.userTempTableName.isEmpty) {
              kustoClient.cleanupIngestionByProducts(tableCoordinates.database, tmpTableName, crp)
            }
          }
          throw exception
      }
      if (writeOptions.writeMode == WriteMode.Transactional) {
        finalizeIngestionWhenWorkersSucceeded(
          tableCoordinates,
          batchIdIfExists,
          tmpTableName,
          partitionsResults,
          writeOptions,
          crp,
          tableExists,
          rdd.sparkContext,
          authentication,
          kustoClient,
          sinkStartTime)
      }
    }
  }

  private def getCreationTime(ingestionProperties: SparkIngestionProperties): Instant = {
    Option(ingestionProperties.creationTime) match {
      case Some(creationTimeVal) => creationTimeVal
      case None => Instant.now(Clock.systemUTC())
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

final case class BlobWriteResource(
    writer: java.io.BufferedWriter,
    gzip: java.util.zip.GZIPOutputStream,
    csvWriter: CountingWriter,
    blob: com.microsoft.azure.storage.blob.CloudBlockBlob,
    sas: String)
final case class KustoWriteResource(
    authentication: KustoAuthentication,
    coordinates: KustoCoordinates,
    schema: StructType,
    writeOptions: WriteOptions,
    tmpTableName: String)

final case class PartitionResult(ingestionResult: IngestionResult, partitionId: Int)

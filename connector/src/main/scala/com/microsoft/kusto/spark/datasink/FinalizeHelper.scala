// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.azure.data.tables.implementation.models.TableServiceErrorException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.ingest.result.{IngestionStatus, OperationStatus}
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.KustoWriter.DelayPeriodBetweenCalls
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.{generateExtentTagsDropByPrefixCommand, generateTableAlterMergePolicyCommand}
import com.microsoft.kusto.spark.utils.KustoConstants.IngestSkippedTrace
import com.microsoft.kusto.spark.utils.{ExtendedKustoClient, KustoClientCache, KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.util.CollectionAccumulator

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, TimeoutException}

object FinalizeHelper {
  private val myName = this.getClass.getSimpleName
  private val mapper = new ObjectMapper().registerModule(new JavaTimeModule())
  private[kusto] def finalizeIngestionWhenWorkersSucceeded(
      coordinates: KustoCoordinates,
      batchIdIfExists: String,
      tmpTableName: String,
      partitionsResults: CollectionAccumulator[PartitionResult],
      writeOptions: WriteOptions,
      crp: ClientRequestProperties,
      tableExists: Boolean,
      sparkContext: SparkContext,
      authentication: KustoAuthentication,
      kustoClient: ExtendedKustoClient,
      sinkStartTime: Instant): Unit = {
    if (!kustoClient.shouldIngestData(
        coordinates,
        writeOptions.ingestionProperties,
        tableExists,
        crp)) {
      KDSU.logInfo(myName, s"$IngestSkippedTrace '${coordinates.table}'")
    } else {
      val mergeTask = Future {
        val requestId = writeOptions.requestId
        val ingestionInfoString =
          s"RequestId: $requestId cluster: '${coordinates.clusterAlias}', " +
            s"database: '${coordinates.database}', table: '$tmpTableName' $batchIdIfExists"
        KDSU.logInfo(
          myName,
          s"Polling on ingestion results for requestId: $requestId, will move data to " +
            s"destination table when finished")

        try {
          if (writeOptions.pollingOnDriver) {
            partitionsResults.value.asScala.foreach(partitionResult =>
              pollOnResult(
                partitionResult,
                requestId,
                writeOptions.timeout.toMillis,
                ingestionInfoString,
                !writeOptions.ensureNoDupBlobs))
          } else {
            KDSU.logWarn(
              myName,
              "IMPORTANT: It's highly recommended to set pollingOnDriver to true on production!\tRead here why https://github.com/Azure/azure-kusto-spark/blob/master/docs/KustoSink.md#supported-options")
            // Specifiying numSlices = 1 so that only one task is created
            val resultsRdd =
              sparkContext.parallelize(partitionsResults.value.asScala, numSlices = 1)
            resultsRdd.sparkContext.setJobDescription("Polling on ingestion results")
            resultsRdd.foreachPartition((results: Iterator[PartitionResult]) =>
              results.foreach(partitionResult =>
                pollOnResult(
                  partitionResult,
                  requestId,
                  writeOptions.timeout.toMillis,
                  ingestionInfoString,
                  !writeOptions.ensureNoDupBlobs)))
          }

          if (partitionsResults.value.size > 0) {
            val pref = KDSU.getDedupTagsPrefix(writeOptions.requestId, batchIdIfExists)
            val moveOperation = (_: Int) => {
              val client = KustoClientCache.getClient(
                coordinates.clusterUrl,
                authentication,
                coordinates.ingestionUrl,
                coordinates.clusterAlias)
              client.executeEngine(
                coordinates.database,
                generateTableAlterMergePolicyCommand(
                  tmpTableName,
                  allowMerge = false,
                  allowRebuild = false),
                crp)
              // Drop dedup tags
              if (writeOptions.ensureNoDupBlobs) {
                client.retryAsyncOp(
                  coordinates.database,
                  generateExtentTagsDropByPrefixCommand(tmpTableName, pref),
                  crp,
                  writeOptions.timeout,
                  s"drops extents from temp table '$tmpTableName' ",
                  writeOptions.requestId)
              }
              client.moveExtents(
                coordinates.database,
                tmpTableName,
                coordinates.table.get,
                crp,
                writeOptions,
                sinkStartTime)
            }
            // Move data to real table
            // Protect tmp table from merge/rebuild and move data to the table requested by customer. This operation is atomic.
            // We are using the ingestIfNotExists Tags here too (on top of the check at the start of the flow) so that if
            // several flows started together only one of them would ingest
            KDSU.logInfo(
              myName,
              s"Final ingestion step: Moving extents from '$tmpTableName, requestId: ${writeOptions.requestId}," +
                s"$batchIdIfExists")

            if (writeOptions.pollingOnDriver) {
              moveOperation(0)
            } else {
              // Specifiying numSlices = 1 so that only one task is created
              val moveExtentsRdd = sparkContext.parallelize(Seq(1), numSlices = 1)
              moveExtentsRdd.sparkContext.setJobDescription("Moving extents to target table")
              moveExtentsRdd.foreach(moveOperation)
            }

            KDSU.logInfo(
              myName,
              s"write to Kusto table '${coordinates.table.get}' finished successfully " +
                s"requestId: ${writeOptions.requestId} $batchIdIfExists")
          } else {
            KDSU.logWarn(
              myName,
              s"write to Kusto table '${coordinates.table.get}' finished with no data written " +
                s"requestId: ${writeOptions.requestId} $batchIdIfExists")
          }
        } catch {
          case ex: Exception =>
            KDSU.reportExceptionAndThrow(
              myName,
              ex,
              "Trying to poll on pending ingestions",
              coordinates.clusterUrl,
              coordinates.database,
              coordinates.table.getOrElse("Unspecified table name"),
              writeOptions.requestId)
        } finally {
          kustoClient.cleanupIngestionByProducts(coordinates.database, tmpTableName, crp)
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
              "polling on ingestion status",
              coordinates.clusterUrl,
              coordinates.database,
              coordinates.table.getOrElse("Unspecified table name"))
        }
      }
    }
  }

  def pollOnResult(
      partitionResult: PartitionResult,
      requestId: String,
      timeout: Long,
      ingestionInfoString: String,
      shouldThrowOnTagsAlreadyExists: Boolean): Unit = {
    var finalRes: Option[IngestionStatus] = None
    KDSU
      .doWhile[Option[IngestionStatus]](
        () => {
          try {
            finalRes = Some(partitionResult.ingestionResult.getIngestionStatusCollection.get(0))
            finalRes
          } catch {
            case e: TableServiceErrorException =>
              KDSU.reportExceptionAndThrow(
                myName,
                e,
                s"TableServiceErrorException : RequestId: $requestId",
                shouldNotThrow = true)
              None
            case e: Exception =>
              KDSU.reportExceptionAndThrow(
                myName,
                e,
                s"Failed to fetch operation status. RequestId: $requestId")
              None
          }
        },
        0,
        DelayPeriodBetweenCalls,
        res => {
          val pending = res.isDefined && res.get.status == OperationStatus.Pending
          if (pending) {
            KDSU.logDebug(
              myName,
              s"Polling on result for partition: '${partitionResult.partitionId}' in requestId: $requestId, status is-'Pending'")
          }
          pending
        },
        res => finalRes = res,
        maxWaitTimeBetweenCallsMillis = KDSU.WriteInitialMaxWaitTime.toMillis.toInt,
        maxWaitTimeAfterMinute = KDSU.WriteMaxWaitTime.toMillis.toInt)
      .await(timeout, TimeUnit.MILLISECONDS)
    finalRes match {
      case Some(ingestResults) =>
        processIngestionStatusResults(
          partitionResult.partitionId,
          ingestionInfoString,
          shouldThrowOnTagsAlreadyExists,
          ingestResults)
      case None => throw new RuntimeException("Failed to poll on ingestion status.")
    }
  }

  def processIngestionStatusResults(
      partitionId: Int = 0,
      ingestionInfoString: String,
      shouldThrowOnTagsAlreadyExists: Boolean,
      ingestionStatusResult: IngestionStatus): Unit = {
    ingestionStatusResult.status match {
      case OperationStatus.Pending =>
        throw new RuntimeException(
          s"Ingestion to Kusto failed on timeout failure. $ingestionInfoString,  partition: '$partitionId'")
      case OperationStatus.Succeeded =>
        KDSU.logInfo(
          myName,
          s"Ingestion to Kusto succeeded. $ingestionInfoString,  partition: '$partitionId', " +
            s"from: '${ingestionStatusResult.ingestionSourcePath}' , Operation ${ingestionStatusResult.operationId}")
      case OperationStatus.Skipped =>
        // TODO: should we throw ?
        KDSU.logInfo(
          myName,
          s"Ingestion to Kusto skipped. $ingestionInfoString, " +
            s"partition: '$partitionId', from: '${ingestionStatusResult.ingestionSourcePath}', " +
            s"Operation ${ingestionStatusResult.operationId}")
      case otherStatus: Any =>
        // TODO error code should be added to java client
        if (ingestionStatusResult.errorCodeString != "Skipped_IngestByTagAlreadyExists") {
          throw new RuntimeException(s"Ingestion to Kusto failed with status '$otherStatus'." +
            s" $ingestionInfoString, partition: '$partitionId'. Ingestion info: '${mapper.writerWithDefaultPrettyPrinter
                .writeValueAsString(ingestionStatusResult)}'")
        } else if (shouldThrowOnTagsAlreadyExists) {
          // TODO - think about this logic and other cases that should not throw all (maybe everything that starts with skip? this actualy
          //  seems like a bug in engine that the operation status is not Skipped)
          //  (Skipped_IngestByTagAlreadyExists is relevant for dedup flow only as in other cases we cancel the ingestion altogether)
          throw new RuntimeException(s"Ingestion to Kusto skipped with status '$otherStatus'." +
            s" $ingestionInfoString, partition: '$partitionId'. Ingestion info: '${new ObjectMapper().writerWithDefaultPrettyPrinter
                .writeValueAsString(ingestionStatusResult)}'")
        }
        KDSU.logInfo(
          myName,
          s"Ingestion to Kusto failed. $ingestionInfoString, " +
            s"partition: '$partitionId', from: '${ingestionStatusResult.ingestionSourcePath}', " +
            s"Operation ${ingestionStatusResult.operationId}")
    }
  }
}

package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.ingest.result.{IngestionStatus, OperationStatus}
import com.microsoft.azure.storage.StorageException
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableAlterMergePolicyCommand
import com.microsoft.kusto.spark.utils.KustoConstants.IngestSkippedTrace
import com.microsoft.kusto.spark.utils.{KustoConstants, KustoDataSourceUtils => KDSU}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkContext
import org.apache.spark.util.CollectionAccumulator
import shaded.parquet.org.codehaus.jackson.map.ObjectMapper

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, TimeoutException}

object FinalizeHelper {
  private val logClassName = this.getClass.getSimpleName

  private[kusto] def finalizeIngestionWhenWorkersSucceeded(partitionsResults: CollectionAccumulator[PartitionResult],
                                                           sparkContext: SparkContext,
                                                           transactionWriteParams: TransactionWriteParams
                                                          ): Unit = {
    if (!transactionWriteParams.kustoClient.shouldIngestData(transactionWriteParams.tableCoordinates,
      transactionWriteParams.writeOptions.maybeSparkIngestionProperties, transactionWriteParams.tableExists,
      transactionWriteParams.crp)) {
      KDSU.logInfo(logClassName, s"$IngestSkippedTrace '${transactionWriteParams.tableCoordinates.table}'")
    } else {
      val mergeTask = Future {
        val loggerName = logClassName
        val requestId = transactionWriteParams.writeOptions.requestId
        val ingestionInfoString = s"RequestId: $requestId cluster: '${transactionWriteParams.tableCoordinates.clusterAlias}', " +
          s"database: '${transactionWriteParams.tableCoordinates.database}', table: '${transactionWriteParams.tmpTableName}' " +
          s"${transactionWriteParams.batchIdIfExists}"
        KDSU.logInfo(loggerName, s"Polling on ingestion results for requestId: $requestId, will move data to " +
          "destination table when finished")

        try {
          if (transactionWriteParams.writeOptions.pollingOnDriver) {
            partitionsResults.value.asScala.foreach(partitionResult => pollOnResult(partitionResult, loggerName, requestId,
              transactionWriteParams.writeOptions.timeout.toMillis,
              ingestionInfoString))
          } else {
            KDSU.logWarn(logClassName, "IMPORTANT: It's highly recommended to set pollingOnDriver to true " +
              "on production!\tRead here why " +
              "https://github.com/Azure/azure-kusto-spark/blob/master/docs/KustoSink.md#supported-options")
            // Specifiying numSlices = 1 so that only one task is created
            val resultsRdd = sparkContext.parallelize(partitionsResults.value.asScala, numSlices = 1)
            resultsRdd.sparkContext.setJobDescription("Polling on ingestion results")
            resultsRdd.foreachPartition((results: Iterator[PartitionResult]) => results.foreach(
              partitionResult => pollOnResult(partitionResult, loggerName, requestId,
                transactionWriteParams.writeOptions.timeout.toMillis,
                ingestionInfoString)
            ))
          }
          if (partitionsResults.value.size > 0) {
            val moveOperation = (_: Int) => {
              transactionWriteParams.kustoClient.executeEngine(transactionWriteParams.tableCoordinates.database,
                generateTableAlterMergePolicyCommand(transactionWriteParams.tmpTableName, allowMerge = false,
                  allowRebuild = false), transactionWriteParams.crp)
              transactionWriteParams.kustoClient.moveExtents(transactionWriteParams.tableCoordinates.database,
                transactionWriteParams.tmpTableName, transactionWriteParams.tableCoordinates.table.get,
                transactionWriteParams.crp, transactionWriteParams.writeOptions)
            }
            // Move data to real table
            // Protect tmp table from merge/rebuild and move data to the table requested by customer. This operation is atomic.
            // We are using the ingestIfNotExists Tags here too (on top of the check at the start of the flow) so that if
            // several flows started together only one of them would ingest
            KDSU.logInfo(logClassName, s"Final ingestion step: Moving extents from '${transactionWriteParams.tmpTableName}, " +
              s"requestId: ${transactionWriteParams.writeOptions.requestId},${transactionWriteParams.batchIdIfExists}")
            if (transactionWriteParams.writeOptions.pollingOnDriver) {
              moveOperation(0)
            } else {
              // Specifying numSlices = 1 so that only one task is created
              val moveExtentsRdd = sparkContext.parallelize(Seq(1), numSlices = 1)
              moveExtentsRdd.sparkContext.setJobDescription("Moving extents to target table")
              moveExtentsRdd.foreach(moveOperation)
            }

            KDSU.logInfo(logClassName, "Write to Kusto table " +
              s"'${transactionWriteParams.tableCoordinates.table.getOrElse("")}' finished successfully " +
              s"requestId: ${transactionWriteParams.writeOptions.requestId} ${transactionWriteParams.batchIdIfExists}")
          } else {
            KDSU.logWarn(logClassName, "Write to Kusto table " +
              s"'${transactionWriteParams.tableCoordinates.table.getOrElse("")}' finished with no data written " +
              s"requestId: ${transactionWriteParams.writeOptions.requestId} ${transactionWriteParams.batchIdIfExists}")
          }
        } catch {
          case ex: Exception =>
            KDSU.reportExceptionAndThrow(
              logClassName,
              ex,
              "Trying to poll on pending ingestions",
              transactionWriteParams.tableCoordinates.clusterUrl,
              transactionWriteParams.tableCoordinates.database,
              transactionWriteParams.tableCoordinates.table.getOrElse("Unspecified table name"),
              transactionWriteParams.writeOptions.requestId
            )
        } finally {
          transactionWriteParams.kustoClient.cleanupIngestionByProducts(transactionWriteParams.tableCoordinates.database,
            transactionWriteParams.tmpTableName, transactionWriteParams.crp)
        }
      }

      if (!transactionWriteParams.writeOptions.isAsync) {
        try {
          Await.result(mergeTask, transactionWriteParams.writeOptions.timeout)
        } catch {
          case _: TimeoutException =>
            KDSU.reportExceptionAndThrow(
              logClassName,
              new TimeoutException("Timed out polling on ingestion status"),
              "Polling on ingestion status", transactionWriteParams.tableCoordinates.clusterUrl,
              transactionWriteParams.tableCoordinates.database, transactionWriteParams.tableCoordinates.table.getOrElse
              ("Unspecified table name"))
        }
      }
    }
  }

  def pollOnResult(partitionResult: PartitionResult, loggerName: String, requestId: String, timeout: Long, ingestionInfoString: String): Unit = {
    var finalRes: Option[IngestionStatus] = None
    KDSU.doWhile[Option[IngestionStatus]](
      () => {
        try {
          finalRes = Some(partitionResult.ingestionResult.getIngestionStatusCollection.get(0))
          finalRes
        } catch {
          case e: StorageException =>
            KDSU.logWarn(loggerName, "Failed to fetch operation status transiently - will keep polling. " +
              s"RequestId: $requestId. Error: ${ExceptionUtils.getStackTrace(e)}")
            None
          case e: Exception => KDSU.reportExceptionAndThrow(loggerName, e, s"Failed to fetch operation status. RequestId: $requestId");
            None
        }
      },
      0,
      KustoConstants.DefaultPeriodicSamplePeriod.toMillis.toInt,
      res => {
        val pending = res.isDefined && res.get.status == OperationStatus.Pending
        if (pending) {
          KDSU.logDebug(loggerName, s"Polling on result for partition: '${partitionResult.partitionId}' in requestId: $requestId, status is-'Pending'")
        }
        pending
      },
      res => finalRes = res,
      maxWaitTimeBetweenCallsMillis = KDSU.WriteInitialMaxWaitTime.toMillis.toInt,
      maxWaitTimeAfterMinute = KDSU.WriteMaxWaitTime.toMillis.toInt)
      .await(timeout, TimeUnit.MILLISECONDS)

    if (finalRes.isDefined) {
      finalRes.get.status match {
        case OperationStatus.Pending =>
          throw new RuntimeException(s"Ingestion to Kusto failed on timeout failure. $ingestionInfoString, " +
            s"partition: '${partitionResult.partitionId}'")
        case OperationStatus.Succeeded =>
          KDSU.logInfo(loggerName, s"Ingestion to Kusto succeeded. $ingestionInfoString, " +
            s"partition: '${partitionResult.partitionId}', from: '${finalRes.get.ingestionSourcePath}', " +
            s"Operation ${finalRes.get.operationId}")
        case otherStatus: Any =>
          throw new RuntimeException(s"Ingestion to Kusto failed with status '$otherStatus'." +
            s" $ingestionInfoString, partition: '${partitionResult.partitionId}'. Ingestion info: '${
              new ObjectMapper()
                .writerWithDefaultPrettyPrinter
                .writeValueAsString(finalRes.get)
            }'")
      }
    } else {
      throw new RuntimeException("Failed to poll on ingestion status.")
    }
  }
}

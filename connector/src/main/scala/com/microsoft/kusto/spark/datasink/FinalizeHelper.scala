package com.microsoft.kusto.spark.datasink

import java.util.concurrent.TimeUnit

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.azure.kusto.ingest.result.{IngestionStatus, OperationStatus}
import com.microsoft.azure.storage.StorageException
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.KustoWriter.DelayPeriodBetweenCalls
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableAlterMergePolicyCommand
import com.microsoft.kusto.spark.utils.KustoConstants.IngestSkippedTrace
import org.apache.commons.lang3.exception.ExceptionUtils
import shaded.parquet.org.codehaus.jackson.map.ObjectMapper
import com.microsoft.kusto.spark.utils.{KustoClient, KustoClientCache, KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.util.CollectionAccumulator

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global

object FinalizeHelper {
  private val myName = this.getClass.getSimpleName

  private[kusto] def finalizeIngestionWhenWorkersSucceeded(coordinates: KustoCoordinates,
                                                           batchIdIfExists: String,
                                                           tmpTableName: String,
                                                           partitionsResults: CollectionAccumulator[PartitionResult],
                                                           writeOptions: WriteOptions,
                                                           crp: ClientRequestProperties,
                                                           tableExists: Boolean,
                                                           sparkContext: SparkContext,
                                                           authentication: KustoAuthentication,
                                                           kustoClient: KustoClient
                                                          ): Unit = {
    if (!kustoClient.shouldIngestData(coordinates, writeOptions.ingestionProperties, tableExists, crp)) {
      KDSU.logInfo(myName, s"$IngestSkippedTrace '${coordinates.table}'")
    } else {
      val mergeTask = Future {
        val loggerName = myName
        val requestId = writeOptions.requestId
        val ingestionInfoString = s"RequestId: $requestId cluster: '${coordinates.clusterAlias}', " +
          s"database: '${coordinates.database}', table: '$tmpTableName' $batchIdIfExists"
        KDSU.logInfo(loggerName, s"Polling on ingestion results for requestId: $requestId, will move data to " +
          s"destination table when finished")

        try {
          if (writeOptions.pollingOnDriver) {
            partitionsResults.value.asScala.foreach(partitionResult => pollOnResult(partitionResult,loggerName, requestId, writeOptions.timeout.toMillis,
              ingestionInfoString))
          } else {
            KDSU.logWarn(myName, "IMPORTANT: It's highly recommended to set pollingOnDriver to true on production!\tRead here why https://github.com/Azure/azure-kusto-spark/blob/master/docs/KustoSink.md#supported-options")
            // Specifiying numSlices = 1 so that only one task is created
            val resultsRdd = sparkContext.parallelize(partitionsResults.value.asScala, numSlices = 1)
            resultsRdd.sparkContext.setJobDescription("Polling on ingestion results")
            resultsRdd.foreachPartition((results: Iterator[PartitionResult]) => results.foreach(
              partitionResult => pollOnResult(partitionResult,loggerName, requestId, writeOptions.timeout.toMillis,
                ingestionInfoString)
            ))
          }

          if (partitionsResults.value.size > 0) {
            val moveOperation = (_: Int) => {
              val client = KustoClientCache.getClient(coordinates.clusterUrl, authentication, coordinates.ingestionUrl,
                coordinates.clusterAlias)
              client.engineClient.execute(coordinates.database, generateTableAlterMergePolicyCommand(tmpTableName,
                allowMerge = false,
                allowRebuild = false), crp)
              client.moveExtents(coordinates.database, tmpTableName, coordinates.table.get, crp, writeOptions)
            }
            // Move data to real table
            // Protect tmp table from merge/rebuild and move data to the table requested by customer. This operation is atomic.
            // We are using the ingestIfNotExists Tags here too (on top of the check at the start of the flow) so that if
            // several flows started together only one of them would ingest
            KDSU.logInfo(myName, s"Final ingestion step: Moving extents from '$tmpTableName, requestId: ${writeOptions.requestId}," +
              s"$batchIdIfExists")
            if (writeOptions.pollingOnDriver) {
              moveOperation(0)
            } else {
              // Specifiying numSlices = 1 so that only one task is created
              val moveExtentsRdd =  sparkContext.parallelize(Seq(1), numSlices = 1)
              moveExtentsRdd.sparkContext.setJobDescription("Moving extents to target table")
              moveExtentsRdd.foreach(moveOperation)
            }

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
              "Trying to poll on pending ingestions",
              coordinates.clusterUrl,
              coordinates.database,
              coordinates.table.getOrElse("Unspecified table name"),
              writeOptions.requestId
            )
        } finally {
          kustoClient.cleanupIngestionByProducts(coordinates.database, kustoClient.engineClient, tmpTableName, crp)
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

  def pollOnResult(partitionResult: PartitionResult, loggerName: String, requestId:String, timeout: Long, ingestionInfoString: String): Unit = {
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
      DelayPeriodBetweenCalls,
      res => res.isDefined && res.get.status == OperationStatus.Pending,
      res => finalRes = res,
      maxWaitTimeBetweenCalls = KDSU.WriteMaxWaitTime.toMillis.toInt)
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

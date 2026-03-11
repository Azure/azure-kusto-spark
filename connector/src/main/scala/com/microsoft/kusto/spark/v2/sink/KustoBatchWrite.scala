// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.sink

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.{WriteMode, WriteOptions}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.{
  generateExtentTagsDropByPrefixCommand,
  generateTableAlterMergePolicyCommand
}
import com.microsoft.kusto.spark.utils.{
  ExtendedKustoClient,
  OperationMetrics,
  KustoDataSourceUtils => KDSU
}
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  DataWriterFactory,
  PhysicalWriteInfo,
  WriterCommitMessage
}
import org.apache.spark.sql.types.StructType

import java.time.Instant

final case class KustoBatchWrite(
    tableCoordinates: KustoCoordinates,
    authentication: KustoAuthentication,
    writeOptions: WriteOptions,
    crp: ClientRequestProperties,
    tmpTableName: String,
    tableExists: Boolean,
    schema: StructType,
    sinkStartTime: Instant,
    kustoClient: ExtendedKustoClient,
    batchIdIfExists: String)
    extends BatchWrite {

  private val className = this.getClass.getSimpleName

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    KDSU.logInfo(
      className,
      s"Creating writer factory for ${info.numPartitions()} partitions, " +
        s"writeFormat=${writeOptions.writeFormat}, writeMode=${writeOptions.writeMode}")

    KustoDataWriterFactory(
      tableCoordinates = tableCoordinates,
      authentication = authentication,
      writeOptions = writeOptions,
      schema = schema,
      tmpTableName = tmpTableName,
      batchIdIfExists = batchIdIfExists)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val commitStartNanos = System.nanoTime()
    val kustoMessages = messages.collect { case m: KustoWriterCommitMessage => m }
    val totalIngestions = kustoMessages.map(_.ingestionCount).sum

    KDSU.logInfo(
      className,
      s"All ${kustoMessages.length} partitions committed successfully with " +
        s"$totalIngestions ingestion(s). writeMode=${writeOptions.writeMode}")

    if (writeOptions.writeMode == WriteMode.Transactional && totalIngestions > 0) {
      // Ingestion polling already happened on each executor in DataWriter.commit().
      // Driver just needs to do the atomic extent move.
      KDSU.logInfo(
        className,
        s"Starting transactional finalization: moving extents " +
          s"from '$tmpTableName' to '${tableCoordinates.table.get}'")

      val pref = KDSU.getDedupTagsPrefix(writeOptions.requestId, batchIdIfExists)
      kustoClient.executeEngine(
        tableCoordinates.database,
        generateTableAlterMergePolicyCommand(
          tmpTableName,
          allowMerge = false,
          allowRebuild = false),
        "alterMergePolicyCommand",
        crp)
      if (writeOptions.kustoCustomDebugWriteOptions.ensureNoDuplicatedBlobs) {
        kustoClient.retryAsyncOp(
          tableCoordinates.database,
          generateExtentTagsDropByPrefixCommand(tmpTableName, pref),
          crp,
          writeOptions.timeout,
          s"drops extents from temp table '$tmpTableName' ",
          "extentsDrop",
          writeOptions.requestId)
      }
      kustoClient.moveExtents(
        tableCoordinates.database,
        tmpTableName,
        tableCoordinates.table.get,
        crp,
        writeOptions,
        sinkStartTime)

      KDSU.logInfo(
        className,
        s"Write to Kusto table '${tableCoordinates.table.get}' finished successfully " +
          s"requestId: ${writeOptions.requestId} $batchIdIfExists")
    } else if (totalIngestions == 0) {
      KDSU.logWarn(
        className,
        s"Write to '${tableCoordinates.table.get}' completed with no data ingested. " +
          s"requestId: ${writeOptions.requestId}")
      kustoClient.cleanupIngestionByProducts(tableCoordinates.database, tmpTableName, crp)
    } else {
      // Queued mode — cleanup staging resources
      kustoClient.cleanupIngestionByProducts(tableCoordinates.database, tmpTableName, crp)
    }

    OperationMetrics.logMetric(
      className,
      "v2.batchWrite.commit",
      (System.nanoTime() - commitStartNanos) / 1000000L,
      Map(
        "table" -> tableCoordinates.table.getOrElse("unknown"),
        "partitions" -> kustoMessages.length.toString,
        "totalIngestions" -> totalIngestions.toString,
        "writeMode" -> writeOptions.writeMode.toString,
        "requestId" -> writeOptions.requestId))
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    KDSU.logWarn(
      className,
      s"Write aborted for table '${tableCoordinates.table.get}', " +
        s"requestId: ${writeOptions.requestId}. Cleaning up staging resources.")

    if (writeOptions.userTempTableName.isEmpty) {
      kustoClient.cleanupIngestionByProducts(tableCoordinates.database, tmpTableName, crp)
    }
  }
}

object KustoBatchWrite {
  // A no-op BatchWrite used when ingestion is skipped (e.g. ingestIfNotExists match)
  val noOp: BatchWrite = new BatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      KustoNoOpDataWriterFactory
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = ()
    override def abort(messages: Array[WriterCommitMessage]): Unit = ()
  }
}

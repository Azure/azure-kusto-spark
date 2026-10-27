// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.IngestionFormat.IngestionFormat
import com.microsoft.kusto.spark.datasink.{
  IngestionFormat,
  SchemaAdjustmentMode,
  WriteMode,
  WriteOptions
}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.generateTableAlterMergePolicyCommand
import com.microsoft.kusto.spark.utils.KustoConstants.IngestSkippedTrace
import com.microsoft.kusto.spark.utils.{
  ExtendedKustoClient,
  KustoConstants => KCONST,
  KustoDataSourceUtils => KDSU
}
import org.apache.http.client.utils.URIBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow

import java.net.URI
import java.time.{Duration, Instant}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * Runs a queued or transactional write through ingest-v2.
 *
 * `plan` is called before the write changes anything and decides whether ingest-v2 can handle the
 * write at all. `write` is called by KustoWriter after the shared preparation (schema checks,
 * table and staging table creation) and mirrors the existing path: queued writes go straight to
 * the table and are not awaited; transactional writes go to the staging table, are awaited, and
 * are then moved to the table.
 *
 * A failure is turned into an IngestV2FallbackException (so KustoWriter retries on the existing
 * path) only when no data can reach the target table twice: either nothing was submitted yet, or
 * the write is transactional with a connector-owned staging table that is dropped first and the
 * move to the target table has not started.
 */
object IngestV2WriterOrchestrator {
  private val myName = this.getClass.getSimpleName
  private[v2] val ConnectorVersion = KCONST.ClientName

  /** Decisions taken before the write starts. */
  final case class Plan(
      dmUrl: String,
      config: IngestionConfig,
      targets: UploadTargets,
      format: IngestionFormat)

  /** State prepared by KustoWriter for this write. */
  final case class WriteContext(
      data: DataFrame,
      coordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions,
      stagingWriteOptions: WriteOptions,
      tmpTableName: String,
      crp: ClientRequestProperties,
      tableExists: Boolean,
      kustoClient: ExtendedKustoClient,
      sinkStartTime: Instant,
      batchIdIfExists: String)

  /** The DM endpoint, derived exactly like the existing ingestion client does. */
  def dmUrl(coordinates: KustoCoordinates): String =
    coordinates.ingestionUrl.getOrElse(
      new URIBuilder()
        .setScheme("https")
        .setHost(KDSU.IngestPrefix + new URI(coordinates.clusterUrl).getHost)
        .toString)

  /** Returns the plan, or why this write must use the existing path. */
  def plan(
      coordinates: KustoCoordinates,
      authentication: KustoAuthentication,
      writeOptions: WriteOptions,
      config: IngestionConfig): Either[String, Plan] = {
    val debug = writeOptions.kustoCustomDebugWriteOptions
    val dm = dmUrl(coordinates)
    if (writeOptions.writeMode == WriteMode.KustoStreaming) {
      Left("streaming writes always use the existing path")
    } else if (writeOptions.maybeIngestionBlobStorage.exists(_.nonEmpty)) {
      Left("a user-provided ingestion storage is set")
    } else if (debug.ensureNoDuplicatedBlobs) {
      Left("ensureNoDuplicatedBlobs is set")
    } else {
      Try(config.selectUploadTargets()) match {
        case Failure(e) => Left(e.getMessage)
        case Success(targets) =>
          val format = effectiveFormat(writeOptions.ingestionFormat, targets)
          val generatesCsvMapping =
            writeOptions.adjustSchema == SchemaAdjustmentMode.GenerateDynamicCsvMapping ||
              debug.addSourceLocationTransform
          if (format == IngestionFormat.CSV && targets.isLake && writeOptions.storageTokenProvider.isEmpty) {
            Left(
              "the service asks for OneLake uploads but no storage token provider " +
                "(storageTokenProviderClasspath) is configured")
          } else if (format == IngestionFormat.Parquet && generatesCsvMapping) {
            Left("a generated CSV mapping cannot be used with the parquet ingestion format")
          } else {
            IngestV2RequestProperties.unsupportedReason(
              writeOptions.maybeSparkIngestionProperties,
              format) match {
              case Some(reason) => Left(reason)
              case None =>
                Try(
                  IngestV2ClientCache
                    .getClient(dm, authentication, ConnectorVersion)
                    .queuedClient) match {
                  case Failure(e) => Left(s"could not create the ingest-v2 client: $e")
                  case Success(_) => Right(Plan(dm, config, targets, format))
                }
            }
          }
      }
    }
  }

  /** Parquet is written with Spark's writer over abfss, which only works for lake folders. */
  private[v2] def effectiveFormat(
      requested: IngestionFormat,
      targets: UploadTargets): IngestionFormat =
    if (requested == IngestionFormat.Parquet && !targets.isLake) {
      KDSU.logWarn(
        myName,
        "The parquet ingestion format needs OneLake lake folders, but the service returned " +
          "storage containers; writing CSV instead")
      IngestionFormat.CSV
    } else {
      requested
    }

  def write(plan: Plan, ctx: WriteContext): Unit = {
    val options = ctx.writeOptions
    KDSU.logInfo(
      myName,
      s"Writing to '${ctx.coordinates.database}.${ctx.coordinates.table.get}' with ingest-v2 " +
        s"(mode: ${options.writeMode}, format: ${plan.format}, upload: ${plan.targets}), " +
        s"requestId: ${options.requestId} ${ctx.batchIdIfExists}")
    if (options.isAsync) {
      Future(run(plan, ctx, allowFallback = false)).onComplete {
        case Success(_) =>
        case Failure(e) =>
          KDSU.reportExceptionAndThrow(
            myName,
            e,
            "writing data with ingest-v2",
            ctx.coordinates.clusterUrl,
            ctx.coordinates.database,
            ctx.coordinates.table.get,
            options.requestId,
            shouldNotThrow = true)
          KDSU.logError(
            myName,
            "The exception is not visible in the driver since we're in async mode")
      }
      KDSU.logInfo(
        myName,
        s"asynchronous write to Kusto table '${ctx.coordinates.table.get}' in progress")
    } else {
      run(plan, ctx, allowFallback = true)
    }
  }

  private def run(plan: Plan, ctx: WriteContext, allowFallback: Boolean): Unit = {
    val options = ctx.writeOptions
    val transactional = options.writeMode == WriteMode.Transactional
    val ownsStagingTable = transactional && options.userTempTableName.isEmpty
    var dataMayBeSubmitted = false
    var commitStarted = false

    try {
      val operations = runJob(plan, ctx, () => dataMayBeSubmitted = true)
      if (transactional) {
        finalizeTransactional(plan, ctx, operations, () => commitStarted = true)
      } else {
        KDSU.logInfo(
          myName,
          s"Queued ${operations.size} ingestion operation(s) for '${ctx.coordinates.table.get}', " +
            s"requestId: ${options.requestId} ${ctx.batchIdIfExists}")
      }
    } catch {
      case NonFatal(e) =>
        if (ownsStagingTable && !commitStarted) {
          dropStagingTable(ctx)
        }
        val safeToRetry =
          !commitStarted && (!dataMayBeSubmitted || ownsStagingTable)
        if (allowFallback && safeToRetry) {
          throw new IngestV2FallbackException(
            s"ingest-v2 write failed before any data could reach '${ctx.coordinates.table.get}': $e",
            e,
            stagingTableUsed = ownsStagingTable)
        }
        throw e
    }
  }

  private def runJob(
      plan: Plan,
      ctx: WriteContext,
      markSubmitted: () => Unit): List[IngestV2SubmittedOperation] = {
    val options = ctx.stagingWriteOptions
    val database = ctx.coordinates.database
    val table =
      if (options.writeMode == WriteMode.Transactional) ctx.tmpTableName
      else ctx.coordinates.table.get

    if (plan.format == IngestionFormat.Parquet) {
      val client =
        IngestV2ClientCache
          .getClient(plan.dmUrl, ctx.authentication, ConnectorVersion)
          .queuedClient
      IngestV2ParquetWriter.ingestDataFrame(
        ctx.data,
        database,
        table,
        client,
        plan.targets.pathFor(0),
        plan.config.maxBlobsPerBatch,
        options,
        markSubmitted)
    } else {
      val dmUrl = plan.dmUrl
      val authentication = ctx.authentication
      val targets = plan.targets
      val maxBlobsPerBatch = plan.config.maxBlobsPerBatch
      val maxBlobSize =
        math.min(options.batchLimit.toLong * KCONST.OneMegaByte, plan.config.maxDataSizeBytes)
      val schema = ctx.data.schema
      val batchId = ctx.batchIdIfExists
      val rdd = ctx.data.queryExecution.toRdd
      val results = rdd.sparkContext.collectionAccumulator[IngestV2SubmittedOperation]
      markSubmitted()
      rdd.foreachPartition { rows: Iterator[InternalRow] =>
        if (rows.hasNext) {
          val client =
            IngestV2ClientCache.getClient(dmUrl, authentication, ConnectorVersion).queuedClient
          IngestV2QueuedWriter
            .ingestPartition(
              rows,
              schema,
              database,
              table,
              client,
              targets,
              maxBlobsPerBatch,
              maxBlobSize,
              options,
              batchId)
            .foreach(results.add)
        }
      }
      results.value.asScala.toList
    }
  }

  /**
   * Mirrors FinalizeHelper on the existing path: re-check ingest-by tags, wait for every
   * operation, protect the staging table from merges and move its extents to the table, then drop
   * it.
   */
  private def finalizeTransactional(
      plan: Plan,
      ctx: WriteContext,
      operations: List[IngestV2SubmittedOperation],
      markCommitStarted: () => Unit): Unit = {
    val options = ctx.writeOptions
    val coordinates = ctx.coordinates
    if (!ctx.kustoClient.shouldIngestData(
        coordinates,
        options.maybeSparkIngestionProperties,
        ctx.tableExists,
        ctx.crp)) {
      KDSU.logInfo(myName, s"$IngestSkippedTrace '${coordinates.table.get}'")
      return
    }

    var completed = false
    try {
      val client =
        IngestV2ClientCache
          .getClient(plan.dmUrl, ctx.authentication, ConnectorVersion)
          .queuedClient
      IngestV2StatusTracker.waitForCompletion(
        operations,
        client,
        Duration.ofMillis(options.timeout.toMillis),
        options.requestId)

      if (operations.nonEmpty) {
        KDSU.logInfo(
          myName,
          s"Final ingestion step: Moving extents from '${ctx.tmpTableName}', requestId: " +
            s"${options.requestId}, ${ctx.batchIdIfExists}")
        markCommitStarted()
        ctx.kustoClient.executeEngine(
          coordinates.database,
          generateTableAlterMergePolicyCommand(
            ctx.tmpTableName,
            allowMerge = false,
            allowRebuild = false),
          "alterMergePolicyCommand",
          ctx.crp)
        ctx.kustoClient.moveExtents(
          coordinates.database,
          ctx.tmpTableName,
          coordinates.table.get,
          ctx.crp,
          options,
          ctx.sinkStartTime)
        KDSU.logInfo(
          myName,
          s"write to Kusto table '${coordinates.table.get}' finished successfully " +
            s"requestId: ${options.requestId} ${ctx.batchIdIfExists}")
      } else {
        KDSU.logWarn(
          myName,
          s"write to Kusto table '${coordinates.table.get}' finished with no data written " +
            s"requestId: ${options.requestId} ${ctx.batchIdIfExists}")
      }
      completed = true
    } finally {
      // Like the existing path, the staging table is always dropped once the workers succeeded.
      // After a failure the drop is best effort, so it cannot hide the original error.
      if (completed) {
        ctx.kustoClient.cleanupIngestionByProducts(
          coordinates.database,
          ctx.tmpTableName,
          ctx.crp)
      } else {
        dropStagingTable(ctx)
      }
    }
  }

  private def dropStagingTable(ctx: WriteContext): Unit =
    try
      ctx.kustoClient.cleanupIngestionByProducts(
        ctx.coordinates.database,
        ctx.tmpTableName,
        ctx.crp)
    catch {
      case NonFatal(e) =>
        KDSU.logWarn(
          myName,
          s"Could not drop the staging table '${ctx.tmpTableName}' after a failed write; it is " +
            s"removed automatically by its retention policy: $e")
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.azure.storage.blob.{BlobContainerClient, BlobContainerClientBuilder}
import com.azure.storage.blob.models.ParallelTransferOptions
import com.azure.storage.common.Utility
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.models.{Format, IngestRequestProperties}
import com.microsoft.azure.kusto.ingest.v2.source.{BlobSource, CompressionType}
import com.microsoft.azure.storage.blob.{BlobRequestOptions, CloudBlockBlob}
import com.microsoft.kusto.spark.datasink.{
  CountingWriter,
  IngestionFormat,
  RowCSVWriterUtils,
  StorageTokenProvider,
  WriteMode,
  WriteOptions
}
import com.microsoft.kusto.spark.utils.{
  KustoConstants => KCONST,
  KustoDataSourceUtils => KDSU,
  KustoQueryUtils
}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import java.io.{BufferedWriter, OutputStream, OutputStreamWriter}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit, TimeoutException}
import java.util.zip.GZIPOutputStream
import java.util.{TimeZone, UUID}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

/**
 * Writes one Spark partition as gzipped CSV blobs to the storage chosen from the ingestion
 * configuration and submits them, in batches, through the ingest-v2 queued client.
 */
object IngestV2QueuedWriter {
  private val myName = this.getClass.getSimpleName
  private[v2] val SubmitTimeoutMinutes = 10L
  private val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("HH-mm-ss-SSSSSS").withZone(ZoneId.systemDefault)

  def ingestPartition(
      rows: Iterator[InternalRow],
      schema: StructType,
      database: String,
      table: String,
      queuedClient: QueuedIngestClient,
      targets: UploadTargets,
      maxBlobsPerBatch: Int,
      maxBlobSizeBytes: Long,
      writeOptions: WriteOptions,
      batchIdForTracing: String): List[IngestV2SubmittedOperation] = {

    val partitionId = TaskContext.getPartitionId()
    val timeZone = TimeZone.getTimeZone(writeOptions.timeZone).toZoneId
    val blobNamePrefix =
      KustoQueryUtils.simplifyName(s"${database}_${table}_${writeOptions.requestId}")
    val sparkProps = writeOptions.maybeSparkIngestionProperties
    val tracked = writeOptions.writeMode == WriteMode.Transactional
    val props =
      IngestV2RequestProperties.build(sparkProps, IngestionFormat.CSV, enableTracking = tracked)
    // Like the existing path, blobs sealed because they are full skip batching unless disabled.
    val fullBlobProps =
      if (writeOptions.kustoCustomDebugWriteOptions.disableFlushImmediately) props
      else
        IngestV2RequestProperties.build(
          sparkProps,
          IngestionFormat.CSV,
          forceSkipBatching = true,
          enableTracking = tracked)
    val target = BlobTarget(targets.pathFor(partitionId), targets.isLake, writeOptions)

    val pendingFull = ListBuffer[String]()
    val operations = ListBuffer[IngestV2SubmittedOperation]()
    var blobNumber = 0
    var current: Option[V2BlobWriteResource] = None

    def submitFull(): Unit = {
      if (pendingFull.nonEmpty) {
        operations += submit(queuedClient, database, table, pendingFull.toList, fullBlobProps)
        pendingFull.clear()
      }
    }

    def seal(): Option[String] = current.map { blob =>
      blob.close()
      current = None
      blobNumber += 1
      blob.blobUrl
    }

    try {
      for (row <- rows) {
        val blob = current.getOrElse {
          val name =
            s"${blobNamePrefix}_${UUID.randomUUID()}_${partitionId}_${blobNumber}_" +
              s"${formatter.format(Instant.now())}_spark.csv.gz"
          val created = target.newBlob(name)
          current = Some(created)
          created
        }
        RowCSVWriterUtils.writeRowAsCSV(row, schema, timeZone, blob.csvWriter)
        if (blob.csvWriter.getCounter >= maxBlobSizeBytes) {
          seal().foreach(pendingFull += _)
          if (pendingFull.size >= maxBlobsPerBatch) submitFull()
        }
      }
      submitFull()
      seal().foreach(last =>
        operations += submit(queuedClient, database, table, List(last), props))
    } catch {
      case e: Throwable =>
        current.foreach(_.abort())
        throw e
    }

    KDSU.logInfo(
      myName,
      s"Partition $partitionId ($batchIdForTracing): wrote $blobNumber blob(s) to " +
        s"${if (targets.isLake) "lake folder" else "storage container"}, submitted " +
        s"${operations.size} ingestion operation(s), requestId: ${writeOptions.requestId}")
    operations.toList
  }

  /**
   * Submits blobs that are already uploaded. The SDK does not retry, so a submission is resent
   * (with the same blobs and source ids) only when the service certainly did not take it:
   * throttling (429), service unavailable (503) or a connection that could not be opened. Other
   * failures, including timeouts, are not resent because the service may already have queued the
   * blobs and the REST API is not idempotent.
   *
   * The operation id is required only when the operation is going to be awaited (tracking on).
   */
  private[v2] def submit(
      queuedClient: QueuedIngestClient,
      database: String,
      table: String,
      blobUrls: List[String],
      props: IngestRequestProperties,
      format: Format = Format.csv,
      compression: CompressionType = CompressionType.GZIP): IngestV2SubmittedOperation = {
    val sources: java.util.List[BlobSource] = blobUrls.map { url =>
      new BlobSource(url, format, UUID.randomUUID(), compression)
    }.asJava
    val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(SubmitTimeoutMinutes)
    val response = withSubmitRetries(deadline, s"$database.$table") { left =>
      awaitSubmit(queuedClient.ingestAsyncJava(database, table, sources, props), left)
    }
    val operationId = Option(response.getIngestResponse)
      .flatMap(r => Option(r.getIngestionOperationId))
      .map(_.trim)
      .filter(_.nonEmpty)
    if (operationId.isEmpty && Option(props.getEnableTracking).exists(_.booleanValue())) {
      throw new IllegalStateException(
        "The ingestion service accepted the blobs but returned no operation id")
    }
    IngestV2SubmittedOperation(
      operationId.getOrElse(""),
      database,
      table,
      response.getIngestionType.name(),
      blobUrls.size)
  }

  private[v2] val MaxSubmitAttempts = 5
  private[v2] val SubmitRetryBaseMillis = 2000L
  private[v2] val SubmitRetryMaxMillis = 30000L
  private val StatusInMessage = """Status: (\d{3})""".r.unanchored
  private val OperationIdInMessage = """OperationId (\S+)\s*$""".r.unanchored

  private[v2] def withSubmitRetries[T](
      deadlineNanos: Long,
      target: String,
      sleep: Long => Unit = Thread.sleep)(call: Long => T): T = {
    var attempt = 1
    while (true) {
      val left = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime())
      try return call(math.max(1L, left))
      catch {
        case e: Exception if attempt < MaxSubmitAttempts && isSafeToResubmit(e) =>
          val cap = math.min(SubmitRetryMaxMillis, SubmitRetryBaseMillis << (attempt - 1))
          val wait = java.util.concurrent.ThreadLocalRandom.current().nextLong(cap / 2, cap + 1)
          val remaining = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime())
          if (wait >= remaining) throw e
          KDSU.logWarn(
            myName,
            s"Submitting blobs to '$target' failed (attempt $attempt of $MaxSubmitAttempts), " +
              s"retrying in ${wait}ms: $e")
          sleep(wait)
          attempt += 1
      }
    }
    throw new IllegalStateException("unreachable")
  }

  /**
   * True only when the service cannot have accepted the request: it was throttled or unavailable
   * and returned no operation id, or the connection could not be made.
   */
  private[v2] def isSafeToResubmit(e: Throwable): Boolean = e match {
    case ie: IngestException =>
      httpStatus(ie).exists(s => s == 429 || s == 503) && !hasOperationId(ie)
    case _ =>
      Iterator
        .iterate[Throwable](e)(_.getCause)
        .takeWhile(_ != null)
        .take(10)
        .exists {
          case _: java.net.ConnectException | _: java.net.UnknownHostException => true
          case _ => false
        }
  }

  /** The SDK adds "OperationId <id>" (or "N/A") to the message of a failed queued submission. */
  private[v2] def hasOperationId(e: IngestException): Boolean =
    Option(e.getMessage).exists {
      case OperationIdInMessage(id) => !id.equalsIgnoreCase("N/A")
      case _ => false
    }

  /** The SDK puts the HTTP status in `failureCode` or, for queued submissions, in the message. */
  private[v2] def httpStatus(e: IngestException): Option[Int] =
    Option(e.getFailureCode).map(_.intValue()).orElse(Option(e.getMessage).collect {
      case StatusInMessage(code) => code.toInt
    })

  /** Waits for a submission with a hard upper bound, unwrapping the SDK's error. */
  private[v2] def awaitSubmit[T](future: CompletableFuture[T], timeoutMillis: Long): T = {
    try future.get(timeoutMillis, TimeUnit.MILLISECONDS)
    catch {
      case _: TimeoutException =>
        future.cancel(true)
        throw new TimeoutException(
          s"Submitting blobs to ingest-v2 did not finish within $SubmitTimeoutMinutes minutes")
      case e: ExecutionException if e.getCause != null => throw e.getCause
    }
  }

  /**
   * Where the blobs of one partition are written. Lake folders are written with AAD tokens from
   * the StorageTokenProvider; storage containers with the SAS from the configuration.
   */
  private sealed trait BlobTarget {
    def newBlob(name: String): V2BlobWriteResource
  }

  private object BlobTarget {
    def apply(path: String, isLake: Boolean, writeOptions: WriteOptions): BlobTarget =
      if (isLake) {
        new LakeTarget(
          path,
          writeOptions.storageTokenProvider.getOrElse(
            throw new IllegalStateException(
              "Uploading to a OneLake lake folder requires a storage token provider " +
                "(storageTokenProviderClasspath)")))
      } else {
        new SasContainerTarget(path)
      }
  }

  private final class SasContainerTarget(containerUrlWithSas: String) extends BlobTarget {
    private val (baseUrl, sas) = {
      val idx = containerUrlWithSas.indexOf('?')
      if (idx < 0) (containerUrlWithSas, "")
      else (containerUrlWithSas.substring(0, idx), containerUrlWithSas.substring(idx + 1))
    }

    override def newBlob(name: String): V2BlobWriteResource = {
      val blobUrl = if (sas.isEmpty) s"$baseUrl/$name" else s"$baseUrl/$name?$sas"
      val blob = new CloudBlockBlob(new URI(blobUrl))
      val options = new BlobRequestOptions()
      options.setConcurrentRequestCount(4)
      V2BlobWriteResource(blob.openOutputStream(null, options, null), blobUrl)
    }
  }

  private final class LakeTarget(lakeFolder: String, tokenProvider: StorageTokenProvider)
      extends BlobTarget {
    private val location = LakeFolderLocation.parse(lakeFolder)
    private val containerClient: BlobContainerClient = new BlobContainerClientBuilder()
      .endpoint(location.containerUrl)
      .credential(new StorageProviderTokenCredential(tokenProvider))
      .buildClient()

    override def newBlob(name: String): V2BlobWriteResource = {
      val blobClient = containerClient.getBlobClient(location.blobPath(name))
      // Null request conditions overwrite, like getBlobOutputStream(true).
      val stream = blobClient.getBlockBlobClient
        .getBlobOutputStream(LakeUpload.TransferOptions, null, null, null, null)
      V2BlobWriteResource(stream, location.blobUrl(name))
    }
  }
}

private[v2] object LakeUpload {
  // Same memory per open blob as the storage-container path (4 blocks of 4 MiB in flight).
  val TransferOptions: ParallelTransferOptions = new ParallelTransferOptions()
    .setBlockSizeLong(4L * 1024 * 1024)
    .setMaxConcurrency(4)
}

/**
 * A lake folder URL split like the .NET SDK (`Kusto.Ingest.Common/Storage/LakeFolder.cs`): the
 * ".dfs." host label is replaced by ".blob.", the first path segment is the container (the
 * workspace) and the rest is the folder inside it. The URL is otherwise used exactly as returned
 * by the service; the host is not derived or checked here.
 */
private[v2] final case class LakeFolderLocation(
    containerUrl: String,
    folder: String,
    rawFolder: String) {

  /** Blob name inside the container, for the storage SDK (not URL encoded). */
  def blobPath(name: String): String = if (folder.isEmpty) name else s"$folder/$name"

  /**
   * URL of a blob in this folder, sent to the service. The folder keeps its '/' separators, like
   * the .NET client's `BlobClient.Uri`; the Java SDK's `getBlobUrl` encodes them as "%2F", which
   * the service cannot parse as a OneLake path.
   */
  def blobUrl(name: String): String =
    (Seq(containerUrl) ++ Seq(rawFolder).filter(_.nonEmpty) :+ Utility.urlEncode(name))
      .mkString("/")
}

private[v2] object LakeFolderLocation {
  def parse(lakeFolder: String): LakeFolderLocation = {
    val uri = new URI(lakeFolder.trim.replace(".dfs.", ".blob."))
    if (!"https".equalsIgnoreCase(uri.getScheme) || uri.getHost == null) {
      throw new IllegalArgumentException("The lake folder must be an absolute https URL")
    }
    val segments = Option(uri.getRawPath).getOrElse("").split('/').filter(_.nonEmpty)
    if (segments.isEmpty) {
      throw new IllegalArgumentException("The lake folder URL has no container segment")
    }
    LakeFolderLocation(
      containerUrl = s"https://${uri.getRawAuthority}/${segments.head}",
      folder = segments.tail.map(s => new URI(s"/$s").getPath.stripPrefix("/")).mkString("/"),
      rawFolder = segments.tail.mkString("/"))
  }
}

/** One gzipped CSV blob being written. */
private[v2] final case class V2BlobWriteResource(target: OutputStream, blobUrl: String) {
  private val gzip = new ReleasableGzipOutputStream(target)
  private val buffer =
    new BufferedWriter(
      new OutputStreamWriter(gzip, StandardCharsets.UTF_8),
      1000 * KCONST.DefaultBufferSize)
  val csvWriter: CountingWriter = CountingWriter(buffer)

  /** Flushes and commits the blob. */
  def close(): Unit = buffer.close()

  /** Best-effort release after a failure; the uncommitted blob is discarded by the service. */
  def abort(): Unit = {
    gzip.release()
    try target.close()
    catch { case _: Exception => }
  }
}

/** GZIP stream whose native compressor can be freed without writing to the target. */
private[v2] final class ReleasableGzipOutputStream(target: OutputStream)
    extends GZIPOutputStream(target) {
  def release(): Unit = `def`.end()
}

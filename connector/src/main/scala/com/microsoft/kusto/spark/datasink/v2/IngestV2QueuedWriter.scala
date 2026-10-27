// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.azure.storage.blob.{BlobContainerClient, BlobContainerClientBuilder}
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient
import com.microsoft.azure.kusto.ingest.v2.models.{Format, IngestRequestProperties}
import com.microsoft.azure.kusto.ingest.v2.source.{BlobSource, CompressionType}
import com.microsoft.azure.storage.blob.{BlobRequestOptions, CloudBlockBlob}
import com.microsoft.kusto.spark.datasink.{
  CountingWriter,
  IngestionFormat,
  RowCSVWriterUtils,
  StorageTokenProvider,
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
    val props = IngestV2RequestProperties.build(sparkProps, IngestionFormat.CSV)
    // Like the existing path, blobs sealed because they are full skip batching unless disabled.
    val fullBlobProps =
      if (writeOptions.kustoCustomDebugWriteOptions.disableFlushImmediately) props
      else
        IngestV2RequestProperties.build(sparkProps, IngestionFormat.CSV, forceSkipBatching = true)
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
    val response = awaitSubmit(queuedClient.ingestAsyncJava(database, table, sources, props))
    val operationId = Option(response.getIngestResponse)
      .flatMap(r => Option(r.getIngestionOperationId))
      .filter(_.trim.nonEmpty)
      .getOrElse(throw new IllegalStateException(
        "The ingestion service accepted the blobs but returned no operation id"))
    IngestV2SubmittedOperation(
      operationId,
      database,
      table,
      response.getIngestionType.name(),
      blobUrls.size)
  }

  /** Waits for a submission with a hard upper bound, unwrapping the SDK's error. */
  private[v2] def awaitSubmit[T](future: CompletableFuture[T]): T = {
    try future.get(SubmitTimeoutMinutes, TimeUnit.MINUTES)
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
      val stream = blobClient.getBlockBlobClient.getBlobOutputStream(true)
      V2BlobWriteResource(stream, blobClient.getBlobUrl)
    }
  }
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

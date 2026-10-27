// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import com.microsoft.kusto.spark.datasink.{IngestionFormat, WriteOptions}
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

import java.net.URI
import java.util.UUID

/**
 * Writes a DataFrame as Parquet files to a OneLake lake folder with Spark's own writer (over
 * abfss, authenticated by the Spark runtime), then submits the files through ingest-v2.
 *
 * Only lake folders are supported; storage containers use the CSV writer.
 */
object IngestV2ParquetWriter {
  private val myName = this.getClass.getSimpleName
  private[v2] val ValidEndpointsKey = "fs.azure.abfs.valid.endpoints"

  /** Where one write puts its Parquet files. */
  private[v2] final case class ParquetStagingLocation(abfssDir: String, blobDirUrl: String)

  private[v2] def stagingLocation(
      lakeFolder: String,
      requestId: String): ParquetStagingLocation = {
    val location = LakeFolderLocation.parse(lakeFolder)
    val blobUri = new URI(location.containerUrl)
    val container = blobUri.getRawPath.stripPrefix("/")
    // abfss needs the DFS endpoint of the same host.
    val dfsAuthority = blobUri.getRawAuthority.replace(".blob.", ".dfs.")
    val leaf = s"spark-ingest-v2/${KustoQueryUtils.simplifyName(requestId)}/${UUID.randomUUID()}"
    def join(folder: String) = (Seq(folder).filter(_.nonEmpty) :+ leaf).mkString("/")
    ParquetStagingLocation(
      abfssDir = s"abfss://$container@$dfsAuthority/${join(location.folder)}",
      blobDirUrl = s"${location.containerUrl}/${join(location.rawFolder)}")
  }

  /**
   * Hadoop settings for this write only (passed as writer options, so the shared Spark
   * configuration is never changed). The lake host is added to the ABFS allowed-endpoint list
   * only when the runtime defines such a list.
   */
  private[v2] def hadoopOptions(base: Configuration, abfssDir: String): Map[String, String] = {
    val host = new URI(abfssDir).getHost
    Option(base.get(ValidEndpointsKey)).map(_.trim) match {
      case Some(current) if current.nonEmpty =>
        val endpoints = current.split(',').map(_.trim).filter(_.nonEmpty)
        if (endpoints.exists(_.equalsIgnoreCase(host))) Map.empty
        else Map(ValidEndpointsKey -> s"$current,$host")
      case _ => Map.empty
    }
  }

  /**
   * Writes the files, then submits them in batches. `onSubmit` is called before the first batch
   * is sent, so the caller knows from then on that data may reach the table.
   */
  def ingestDataFrame(
      data: DataFrame,
      database: String,
      table: String,
      queuedClient: QueuedIngestClient,
      lakeFolder: String,
      maxBlobsPerBatch: Int,
      writeOptions: WriteOptions,
      onSubmit: () => Unit): List[IngestV2SubmittedOperation] = {

    val staging = stagingLocation(lakeFolder, writeOptions.requestId)
    val baseConf = data.sparkSession.sparkContext.hadoopConfiguration
    val options = hadoopOptions(baseConf, staging.abfssDir)

    KDSU.logInfo(
      myName,
      s"Writing Parquet files to the lake folder for $database.$table, requestId: ${writeOptions.requestId}")
    data.write
      .mode("errorifexists")
      .options(options)
      .option("compression", "snappy")
      .parquet(staging.abfssDir)

    val conf = new Configuration(baseConf)
    options.foreach { case (k, v) => conf.set(k, v) }
    val dir = new Path(staging.abfssDir)
    val files = FileSystem
      .get(dir.toUri, conf)
      .listStatus(dir)
      .filter(_.isFile)
      .map(_.getPath.getName)
      .filter(n => n.endsWith(".parquet") && !n.startsWith("_") && !n.startsWith("."))
      .sorted
      .toList

    if (files.isEmpty) {
      KDSU.logWarn(myName, s"No Parquet files were written, requestId: ${writeOptions.requestId}")
      return List.empty
    }

    val props =
      IngestV2RequestProperties.build(
        writeOptions.maybeSparkIngestionProperties,
        IngestionFormat.Parquet)
    val urls = files.map(name => s"${staging.blobDirUrl}/$name")
    onSubmit()
    val operations = urls
      .grouped(maxBlobsPerBatch)
      .map { batch =>
        IngestV2QueuedWriter.submit(
          queuedClient,
          database,
          table,
          batch,
          props,
          Format.parquet,
          CompressionType.NONE)
      }
      .toList

    KDSU.logInfo(
      myName,
      s"Submitted ${operations.size} ingestion operation(s) for ${files.size} Parquet file(s), " +
        s"requestId: ${writeOptions.requestId}")
    operations
  }
}

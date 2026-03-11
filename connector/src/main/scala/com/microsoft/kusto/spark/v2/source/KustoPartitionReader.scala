// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.source

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.datasource.{KustoFilter, KustoResponseDeserializer, KustoSchema}
import com.microsoft.kusto.spark.utils.{
  KustoClientCache,
  OperationMetrics,
  KustoDataSourceUtils => KDSU
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import java.net.URI
import scala.jdk.CollectionConverters._

/**
 * Reads results from a direct Kusto query (single mode). Executes the query against the Kusto
 * engine, deserializes the response, and converts each [[Row]] to [[InternalRow]].
 */
final class KustoSingleModePartitionReader(partition: KustoSingleModeInputPartition)
    extends PartitionReader[InternalRow] {

  private val className = this.getClass.getSimpleName
  private val startNanos = System.nanoTime()

  private val kustoClient = KustoClientCache.getClient(
    partition.coordinates.clusterUrl,
    partition.authentication,
    partition.coordinates.ingestionUrl,
    partition.coordinates.clusterAlias)

  private val filteredQuery = KustoFilter.pruneAndFilter(
    KustoSchema(partition.readSchema, Set()),
    partition.query,
    partition.filtering)

  private val crp = new ClientRequestProperties()

  private val kustoResult = kustoClient
    .executeEngine(
      partition.coordinates.database,
      filteredQuery,
      "v2SingleModeRead",
      crp,
      isMgmtCommand = false)
    .getPrimaryResults

  private val deserializer = KustoResponseDeserializer(kustoResult)
  private val rows: java.util.Iterator[Row] = deserializer.toRows.iterator()
  private val converters: Array[Any => Any] =
    deserializer.getSchema.sparkSchema.fields.map(f =>
      CatalystTypeConverters.createToCatalystConverter(f.dataType))

  private var currentRow: InternalRow = _
  private var rowCount: Long = 0L

  override def next(): Boolean = {
    if (rows.hasNext) {
      val row = rows.next()
      val values = new Array[Any](row.length)
      var i = 0
      while (i < row.length) {
        values(i) =
          if (row.isNullAt(i)) null
          else converters(i)(row.get(i))
        i += 1
      }
      currentRow = InternalRow.fromSeq(values.toIndexedSeq)
      rowCount += 1
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    OperationMetrics.logMetric(
      className,
      "v2.reader.singleMode",
      (System.nanoTime() - startNanos) / 1000000L,
      Map("rows" -> rowCount.toString, "requestId" -> partition.requestId))
  }
}

/**
 * Reads exported parquet files from blob storage (distributed mode). Configures Hadoop to access
 * the blob via SAS or account key, lists parquet files, and reads them using Spark's
 * ParquetReadSupport.
 */
final class KustoDistributedPartitionReader(partition: KustoDistributedInputPartition)
    extends PartitionReader[InternalRow] {

  private val className = this.getClass.getSimpleName
  private val startNanos = System.nanoTime()

  private val hadoopConf: Configuration = buildHadoopConf()
  private val parquetFiles: Iterator[Path] = listParquetFiles()
  private var currentReader: org.apache.parquet.hadoop.ParquetReader[InternalRow] = _
  private var currentRow: InternalRow = _
  private var rowCount: Long = 0L

  override def next(): Boolean = {
    // Try to read from current reader
    if (currentReader != null) {
      val row = currentReader.read()
      if (row != null) {
        currentRow = row.copy()
        rowCount += 1
        return true
      }
      currentReader.close()
      currentReader = null
    }
    // Move to next parquet file
    while (parquetFiles.hasNext) {
      val path = parquetFiles.next()
      KDSU.logDebug(className, s"Opening parquet file: $path")
      currentReader = org.apache.parquet.hadoop.ParquetReader
        .builder(new ParquetReadSupport(), path)
        .withConf(hadoopConf)
        .build()
      val row = currentReader.read()
      if (row != null) {
        currentRow = row.copy()
        rowCount += 1
        return true
      }
      currentReader.close()
      currentReader = null
    }
    false
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    if (currentReader != null) {
      currentReader.close()
      currentReader = null
    }
    OperationMetrics.logMetric(
      className,
      "v2.reader.distributed",
      (System.nanoTime() - startNanos) / 1000000L,
      Map(
        "rows" -> rowCount.toString,
        "blobPath" -> partition.blobPath,
        "requestId" -> partition.requestId))
  }

  private def buildHadoopConf(): Configuration = {
    val conf = new Configuration()
    // Set the requested Spark schema for Spark's ParquetReadSupport
    conf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, partition.readSchema.json)
    conf.set(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      SQLConf.PARQUET_BINARY_AS_STRING.defaultValueString)
    conf.set(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.defaultValueString)

    // Configure storage access
    val endpointSuffix = partition.endpointSuffix
    val sasKey =
      if (partition.sasKey.startsWith("?")) partition.sasKey.substring(1) else partition.sasKey

    if (partition.storageProtocol.equalsIgnoreCase("abfs") ||
      partition.storageProtocol.equalsIgnoreCase("abfss")) {
      conf.set("fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem")
      conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem")
      conf.set("fs.azure.account.auth.type", "SAS")
      conf.set(
        s"fs.azure.sas.fixed.token.${partition.blobContainer}.${partition.storageAccountName}.blob.$endpointSuffix",
        partition.sasKey)
    } else {
      conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      conf.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      conf.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      if (sasKey.nonEmpty) {
        conf.set(
          s"fs.azure.sas.${partition.blobContainer}.${partition.storageAccountName}.blob.$endpointSuffix",
          sasKey)
      }
      if (partition.storageAccountKey != null && partition.storageAccountKey.nonEmpty) {
        conf.set(
          s"fs.azure.account.key.${partition.storageAccountName}.blob.$endpointSuffix",
          partition.storageAccountKey)
      }
    }
    conf
  }

  private def listParquetFiles(): Iterator[Path] = {
    val blobPath = partition.blobPath
    val fs = FileSystem.get(new URI(blobPath), hadoopConf)
    val dirPath = new Path(blobPath)
    if (!fs.exists(dirPath)) {
      KDSU.logWarn(className, s"Export directory does not exist: $blobPath")
      return Iterator.empty
    }
    val iter = fs.listFiles(dirPath, true)
    val files = scala.collection.mutable.ListBuffer.empty[Path]
    while (iter.hasNext) {
      val status = iter.next()
      val name = status.getPath.getName
      if (!name.startsWith("_") && name.endsWith(".parquet")) {
        files += status.getPath
      }
    }
    KDSU.logInfo(
      className,
      s"Found ${files.size} parquet file(s) in $blobPath for requestId: ${partition.requestId}")
    files.iterator
  }
}

/** Empty reader that immediately signals no data. */
final class KustoEmptyPartitionReader extends PartitionReader[InternalRow] {
  override def next(): Boolean = false
  override def get(): InternalRow = null
  override def close(): Unit = ()
}

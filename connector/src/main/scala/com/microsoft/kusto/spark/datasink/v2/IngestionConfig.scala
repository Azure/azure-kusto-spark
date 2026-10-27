// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

import scala.jdk.CollectionConverters._

/**
 * Parsed ingestion configuration from the DM config API (`/v1/rest/ingestion/configuration`).
 *
 * Enum-like fields are kept as the raw strings returned by the service and are always compared
 * case-insensitively, mirroring the .NET SDK (`JsonStringEnumConverter`) and the Java SDK
 * (`ManagedUploader`, `equals("Lake", ignoreCase = true)`). The service sends e.g. "Rest" and
 * "Lake".
 *
 * `blobPaths` may carry SAS tokens, so `toString` never prints the paths.
 */
final case class IngestionConfig(
    preferredIngestionMethod: Option[String],
    preferredUploadMethod: Option[String],
    maxBlobsPerBatch: Int,
    maxDataSizeBytes: Long,
    blobPaths: Seq[String],
    oneLakePaths: Seq[String]) {

  def prefersRestIngestion: Boolean =
    preferredIngestionMethod.exists(_.trim.equalsIgnoreCase(IngestionConfig.RestIngestionMethod))

  def prefersLakeUpload: Boolean =
    preferredUploadMethod.exists(_.trim.equalsIgnoreCase(IngestionConfig.LakeUploadMethod))

  /** The service asks for ingest-v2: REST ingestion and/or OneLake upload. */
  def isV2Preferred: Boolean = prefersRestIngestion || prefersLakeUpload

  /**
   * Picks the upload targets exactly like the SDKs (`ManagedUploader.SelectContainers`):
   *   - none available -> error
   *   - only lake folders -> lake
   *   - only storage containers -> storage
   *   - both -> `preferredUploadMethod` ("Lake" -> lake, anything else -> storage)
   */
  def selectUploadTargets(): UploadTargets = {
    val hasLake = oneLakePaths.nonEmpty
    val hasStorage = blobPaths.nonEmpty
    if (!hasLake && !hasStorage) {
      throw new IllegalStateException(
        "The ingestion configuration returned no storage containers and no lake folders")
    } else if (!hasStorage) {
      UploadTargets(oneLakePaths, isLake = true)
    } else if (!hasLake) {
      UploadTargets(blobPaths, isLake = false)
    } else if (prefersLakeUpload) {
      UploadTargets(oneLakePaths, isLake = true)
    } else {
      UploadTargets(blobPaths, isLake = false)
    }
  }

  override def toString: String =
    s"IngestionConfig(preferredIngestionMethod=${preferredIngestionMethod.orNull}, " +
      s"preferredUploadMethod=${preferredUploadMethod.orNull}, maxBlobsPerBatch=$maxBlobsPerBatch, " +
      s"maxDataSizeBytes=$maxDataSizeBytes, storageContainers=${blobPaths.size}, " +
      s"lakeFolders=${oneLakePaths.size})"
}

/** Upload targets chosen from the configuration; `paths` are never empty. */
final case class UploadTargets(paths: Seq[String], isLake: Boolean) {
  require(paths.nonEmpty, "upload targets must not be empty")

  /** Spreads partitions over the available targets. */
  def pathFor(index: Int): String = paths(Math.floorMod(index, paths.size))

  override def toString: String =
    s"UploadTargets(kind=${if (isLake) "Lake" else "Storage"}, count=${paths.size})"
}

object IngestionConfig {
  private val myName = this.getClass.getSimpleName

  val RestIngestionMethod = "Rest"
  val LakeUploadMethod = "Lake"
  val DefaultMaxBlobsPerBatch = 70
  // 4 GiB, matches the previous connector default.
  val DefaultMaxDataSizeBytes: Long = 4L * 1024 * 1024 * 1024

  /**
   * Parses the SDK response. Returns None if the response is null or cannot be read. Non-positive
   * limits fall back to the defaults, like the .NET `CachedConfiguration`.
   */
  def fromConfigurationResponse(response: ConfigurationResponse): Option[IngestionConfig] = {
    if (response == null) {
      return None
    }

    try {
      val ingestionSettings = Option(response.getIngestionSettings)
      val containerSettings = Option(response.getContainerSettings)

      def nonBlank(s: String): Option[String] = Option(s).map(_.trim).filter(_.nonEmpty)

      val maxBlobsPerBatch = ingestionSettings
        .flatMap(s => Option(s.getMaxBlobsPerBatch))
        .map(_.longValue())
        .filter(v => v > 0 && v <= Int.MaxValue)
        .map(_.toInt)
        .getOrElse(DefaultMaxBlobsPerBatch)

      val maxDataSize = ingestionSettings
        .flatMap(s => Option(s.getMaxDataSize))
        .map(_.longValue())
        .filter(_ > 0)
        .getOrElse(DefaultMaxDataSizeBytes)

      val blobPaths = containerSettings
        .flatMap(s => Option(s.getContainers))
        .map(_.asScala.toSeq.flatMap(c => Option(c).flatMap(x => nonBlank(x.getPath))))
        .getOrElse(Seq.empty)

      val oneLakePaths = containerSettings
        .flatMap(s => Option(s.getLakeFolders))
        .map(_.asScala.toSeq.flatMap(c => Option(c).flatMap(x => nonBlank(x.getPath))))
        .getOrElse(Seq.empty)

      Some(
        IngestionConfig(
          preferredIngestionMethod = ingestionSettings.flatMap(s =>
            nonBlank(s.getPreferredIngestionMethod)),
          preferredUploadMethod = containerSettings.flatMap(s =>
            nonBlank(s.getPreferredUploadMethod)),
          maxBlobsPerBatch = maxBlobsPerBatch,
          maxDataSizeBytes = maxDataSize,
          blobPaths = blobPaths,
          oneLakePaths = oneLakePaths))
    } catch {
      case e: Exception =>
        KDSU.logWarn(
          myName,
          s"Failed to parse the ingestion configuration: ${e.getClass.getSimpleName}: ${e.getMessage}")
        None
    }
  }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

import scala.jdk.CollectionConverters._

/**
 * Parsed ingestion configuration from DM config API.
 *
 * Contains settings that control ingestion behavior:
 *   - preferredIngestionMethod: "REST" (V2) or "Legacy" (V1)
 *   - preferredUploadMethod: "Blob" or "Lake" (OneLake)
 *   - Batch limits and storage paths
 */
case class IngestionConfig(
    preferredIngestionMethod: String, // "REST" or "Legacy"
    preferredUploadMethod: String, // "Blob" or "Lake"
    maxBlobsPerBatch: Int,
    maxDataSizeBytes: Long,
    blobPaths: Seq[String],
    oneLakePaths: Seq[String])

object IngestionConfig {
  private val myName = this.getClass.getSimpleName

  /**
   * Parse ConfigurationResponse from V2 SDK into IngestionConfig.
   *
   * Extracts ingestion settings according to the config API contract:
   *   - preferredIngestionMethod: THE decision field for V1 vs V2
   *   - Container and batch settings
   *
   * Returns None if ConfigurationResponse is null or cannot be parsed.
   *
   * Note: Uses reflection to access Kotlin data class properties since the V2 SDK is written in
   * Kotlin.
   */
  def fromConfigurationResponse(response: ConfigurationResponse): Option[IngestionConfig] = {
    if (response == null) {
      return None
    }

    try {
      // Access Kotlin data class properties directly (not via reflection)
      val ingestionSettings = response.getIngestionSettings
      val containerSettings = response.getContainerSettings

      // Extract fields directly from Kotlin data classes
      val preferredIngestionMethod =
        if (ingestionSettings != null)
          Option(ingestionSettings.getPreferredIngestionMethod).map(_.toString).getOrElse("Legacy")
        else "Legacy"

      val preferredUploadMethod =
        if (containerSettings != null)
          Option(containerSettings.getPreferredUploadMethod).map(_.toString).getOrElse("Blob")
        else "Blob"

      val maxBlobsPerBatch: Int =
        if (ingestionSettings != null)
          Option(ingestionSettings.getMaxBlobsPerBatch).map(_.asInstanceOf[Number].intValue()).getOrElse(70)
        else 70

      val maxDataSize: Long =
        if (ingestionSettings != null)
          Option(ingestionSettings.getMaxDataSize).map(_.asInstanceOf[Number].longValue()).getOrElse(4294967296L)
        else 4294967296L

      // Extract container paths (null-safe at every level)
      val blobPaths =
        if (containerSettings != null && containerSettings.getContainers != null)
          containerSettings.getContainers.asScala.flatMap(c => Option(c.getPath)).toSeq
        else Seq.empty[String]

      val oneLakePaths =
        if (containerSettings != null && containerSettings.getLakeFolders != null)
          containerSettings.getLakeFolders.asScala.flatMap(f => Option(f.getPath)).toSeq
        else Seq.empty[String]

      Some(
        IngestionConfig(
          preferredIngestionMethod = preferredIngestionMethod,
          preferredUploadMethod = preferredUploadMethod,
          maxBlobsPerBatch = maxBlobsPerBatch,
          maxDataSizeBytes = maxDataSize,
          blobPaths = blobPaths,
          oneLakePaths = oneLakePaths))

    } catch {
      case e: Exception =>
        KDSU.logWarn(
          myName,
          s"Failed to parse ConfigurationResponse: ${e.getClass.getSimpleName}: ${e.getMessage}")
        KDSU.logDebug(myName, s"Parse error stack trace: ${e.getStackTrace.take(5).mkString("\n  ")}")
        None
    }
  }
}

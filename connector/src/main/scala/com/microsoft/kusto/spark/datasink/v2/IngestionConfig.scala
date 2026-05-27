// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse

/**
 * Parsed ingestion configuration from DM config API.
 *
 * Contains settings that control ingestion behavior:
 * - preferredIngestionMethod: "REST" (V2) or "Legacy" (V1)
 * - preferredUploadMethod: "Blob" or "Lake" (OneLake)
 * - Batch limits and storage paths
 */
case class IngestionConfig(
    preferredIngestionMethod: String, // "REST" or "Legacy"
    preferredUploadMethod: String, // "Blob" or "Lake"
    maxBlobsPerBatch: Int,
    maxDataSizeBytes: Long,
    blobPaths: Seq[String],
    oneLakePaths: Seq[String])

object IngestionConfig {

  /**
   * Parse ConfigurationResponse from V2 SDK into IngestionConfig.
   *
   * Extracts ingestion settings according to the config API contract:
   * - preferredIngestionMethod: THE decision field for V1 vs V2
   * - Container and batch settings
   *
   * Returns None if ConfigurationResponse is null or cannot be parsed.
   */
  def fromConfigurationResponse(
      response: ConfigurationResponse): Option[IngestionConfig] = {
    if (response == null) {
      return None
    }

    try {
      // Extract ingestion settings
      val ingestionSettings = Option(response.getIngestionSettings)
      val containerSettings = Option(response.getContainerSettings)

      val preferredIngestionMethod =
        ingestionSettings
          .flatMap(s => Option(s.getPreferredIngestionMethod))
          .getOrElse("Legacy") // Default to Legacy if missing

      val preferredUploadMethod =
        containerSettings
          .flatMap(s => Option(s.getPreferredUploadMethod))
          .getOrElse("Blob") // Default to Blob if missing

      val maxBlobsPerBatch =
        ingestionSettings
          .flatMap(s => Option(s.getMaxBlobsPerBatch))
          .map(_.intValue())
          .getOrElse(70) // Default from V2 SDK

      val maxDataSizeBytes =
        ingestionSettings
          .flatMap(s => Option(s.getMaxDataSizeBytes))
          .map(_.longValue())
          .getOrElse(4294967296L) // 4GB default

      val blobPaths =
        containerSettings
          .flatMap(s => Option(s.getBlobPaths))
          .map(_.toArray.map(_.toString).toSeq)
          .getOrElse(Seq.empty)

      val oneLakePaths =
        containerSettings
          .flatMap(s => Option(s.getOneLakePaths))
          .map(_.toArray.map(_.toString).toSeq)
          .getOrElse(Seq.empty)

      Some(
        IngestionConfig(
          preferredIngestionMethod = preferredIngestionMethod,
          preferredUploadMethod = preferredUploadMethod,
          maxBlobsPerBatch = maxBlobsPerBatch,
          maxDataSizeBytes = maxDataSizeBytes,
          blobPaths = blobPaths,
          oneLakePaths = oneLakePaths
        ))

    } catch {
      case e: Exception =>
        // Failed to parse config response
        None
    }
  }
}

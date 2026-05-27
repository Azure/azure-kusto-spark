// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse

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
      // Access Kotlin data class properties via getters
      val ingestionSettings = Option(response.getIngestionSettings)
      val containerSettings = Option(response.getContainerSettings)

      // Extract preferredIngestionMethod using reflection (Kotlin property)
      val preferredIngestionMethod =
        ingestionSettings
          .flatMap(s =>
            Option(s.getClass.getMethod("getPreferredIngestionMethod").invoke(s))
              .map(_.toString))
          .getOrElse("Legacy") // Default to Legacy if missing

      // Extract preferredUploadMethod using reflection
      val preferredUploadMethod =
        containerSettings
          .flatMap(s =>
            Option(s.getClass.getMethod("getPreferredUploadMethod").invoke(s))
              .map(_.toString))
          .getOrElse("Blob") // Default to Blob if missing

      // Extract maxBlobsPerBatch using reflection
      val maxBlobsPerBatch =
        ingestionSettings
          .flatMap(s =>
            Option(s.getClass.getMethod("getMaxBlobsPerBatch").invoke(s))
              .map(_.asInstanceOf[Int]))
          .getOrElse(70) // Default from V2 SDK

      // For now, use defaults for fields we can't easily access
      // These will be enhanced once we understand the exact Kotlin API
      val maxDataSizeBytes = 4294967296L // 4GB default
      val blobPaths = Seq.empty[String] // Will be populated from actual API
      val oneLakePaths = Seq.empty[String]

      Some(
        IngestionConfig(
          preferredIngestionMethod = preferredIngestionMethod,
          preferredUploadMethod = preferredUploadMethod,
          maxBlobsPerBatch = maxBlobsPerBatch,
          maxDataSizeBytes = maxDataSizeBytes,
          blobPaths = blobPaths,
          oneLakePaths = oneLakePaths))

    } catch {
      case e: Exception =>
        // Failed to parse config response
        None
    }
  }
}

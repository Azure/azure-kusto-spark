// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

/**
 * Thrown by IngestV2WriterOrchestrator when the config API indicates V2 is not supported
 * or is unavailable. KustoWriter catches this to fall back to V1 legacy ingestion.
 */
class IngestV2FallbackException(message: String) extends RuntimeException(message)

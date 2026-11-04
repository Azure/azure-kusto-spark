// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

/**
 * Test-only switches, so end-to-end tests can run the ingest-v2 path against clusters whose
 * configuration does not ask for it, and see ingest-v2 failures instead of a silent fallback.
 * They are not options and are never set by the connector itself.
 */
private[kusto] object IngestV2TestHooks {
  @volatile var configOverride: Option[IngestionConfig => IngestionConfig] = None
  @volatile var failInsteadOfFallback: Boolean = false

  def reset(): Unit = {
    configOverride = None
    failInsteadOfFallback = false
  }
}

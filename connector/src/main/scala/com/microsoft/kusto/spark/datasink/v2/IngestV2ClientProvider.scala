// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.builders.{
  ManagedStreamingIngestClientBuilder,
  QueuedIngestClientBuilder
}
import com.microsoft.azure.kusto.ingest.v2.client.{
  ManagedStreamingIngestClient,
  QueuedIngestClient
}
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

/**
 * Self-contained client provider for the kusto-ingest-v2 SDK. Builds and caches ingest-v2 SDK
 * clients independently of the v1 ExtendedKustoClient and KustoClientCache.
 *
 * This class owns its own lifecycle and can be removed or replaced without affecting the v1 path.
 */
class IngestV2ClientProvider(
    dmUrl: String,
    authentication: KustoAuthentication,
    connectorVersion: String) {

  private val myName = this.getClass.getSimpleName
  private val tokenCredential: TokenCredential =
    IngestV2Authentication.createTokenCredential(authentication)

  lazy val queuedClient: QueuedIngestClient = {
    KDSU.logInfo(myName, s"Creating QueuedIngestClient for DM: $dmUrl")
    QueuedIngestClientBuilder
      .create(dmUrl)
      .withAuthentication(tokenCredential)
      .withClientDetails("Kusto.Spark.Connector", connectorVersion, "")
      .build()
  }

  lazy val managedStreamingClient: ManagedStreamingIngestClient = {
    KDSU.logInfo(myName, s"Creating ManagedStreamingIngestClient for DM: $dmUrl")
    ManagedStreamingIngestClientBuilder
      .create(dmUrl)
      .withAuthentication(tokenCredential)
      .withClientDetails("Kusto.Spark.Connector", connectorVersion, "")
      .build()
  }

  def close(): Unit = {
    KDSU.logDebug(myName, "Closing ingest-v2 clients")
    try { queuedClient.close() }
    catch { case _: Exception => }
    try { managedStreamingClient.close() }
    catch { case _: Exception => }
  }
}

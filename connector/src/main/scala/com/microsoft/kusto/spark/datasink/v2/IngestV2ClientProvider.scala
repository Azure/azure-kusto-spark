// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}

/**
 * Lazily builds the ingest-v2 SDK queued client for one DM endpoint and authentication.
 */
class IngestV2ClientProvider(
    dmUrl: String,
    authentication: KustoAuthentication,
    connectorVersion: String) {

  private val myName = this.getClass.getSimpleName
  @volatile private var queuedClientCreated = false

  lazy val queuedClient: QueuedIngestClient = {
    KDSU.logInfo(myName, s"Creating QueuedIngestClient for DM: $dmUrl")
    val client = QueuedIngestClientBuilder
      .create(dmUrl)
      .withAuthentication(IngestV2Authentication.createTokenCredential(authentication))
      .withClientDetails(IngestV2ClientProvider.ApplicationName, connectorVersion, "")
      .build()
    queuedClientCreated = true
    client
  }

  def close(): Unit = {
    if (queuedClientCreated) {
      try queuedClient.close()
      catch {
        case e: Exception =>
          KDSU.logDebug(myName, s"Ignoring failure while closing the ingest client: $e")
      }
    }
  }
}

object IngestV2ClientProvider {
  val ApplicationName = "Kusto.Spark.Connector"
}

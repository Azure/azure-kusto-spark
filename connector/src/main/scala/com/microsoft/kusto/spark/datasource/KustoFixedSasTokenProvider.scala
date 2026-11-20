// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider

/**
 * Account-scoped fixed SAS provider. ABFS prefers any configured SAS provider over
 * 'fs.azure.sas.fixed.token', so explicit connector credentials name this provider for their
 * account to override an inherited one. Hadoop's FixedSASTokenProvider cannot be configured by
 * class name because it has no no-argument constructor.
 */
class KustoFixedSasTokenProvider extends SASTokenProvider {
  private var token: String = _

  override def initialize(configuration: Configuration, accountName: String): Unit = {
    val key = s"${KustoFixedSasTokenProvider.FixedTokenKey}.$accountName"
    token = Option(configuration.getPassword(key)).map(new String(_)).orNull
    require(token != null && token.nonEmpty, s"'$key' must be set")
  }

  override def getSASToken(
      account: String,
      fileSystem: String,
      path: String,
      operation: String): String = token
}

object KustoFixedSasTokenProvider {
  val FixedTokenKey = "fs.azure.sas.fixed.token"
}

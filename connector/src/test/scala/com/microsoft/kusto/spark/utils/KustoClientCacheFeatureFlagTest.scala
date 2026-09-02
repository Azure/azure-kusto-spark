// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.kusto.spark.authentication.KustoAccessTokenAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasource.DistributedReadModeTransientCacheKey
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KustoClientCacheFeatureFlagTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  private val clusterUrl = "https://somecluster.kusto.windows.net"
  private val ingestionUrl = Some("https://ingest-somecluster.kusto.windows.net")
  private val authentication = KustoAccessTokenAuthentication("not-a-real-token")
  private val clusterAlias = "somecluster"

  override protected def beforeEach(): Unit = {
    KustoClientCache.clientCache.clear()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    KustoClientCache.clientCache.clear()
    super.afterEach()
  }

  "getClient" should "keep the export storage API disabled for the existing call" in {
    val client =
      KustoClientCache.getClient(clusterUrl, authentication, ingestionUrl, clusterAlias)

    client.exportStorageApiEnabled shouldBe false
    KustoClientCache
      .getClient(
        clusterUrl,
        authentication,
        ingestionUrl,
        clusterAlias,
        enableExportStorageApi = false) should be theSameInstanceAs client
  }

  it should "isolate enabled and disabled clients in the cache" in {
    val disabled =
      KustoClientCache.getClient(clusterUrl, authentication, ingestionUrl, clusterAlias)
    val enabled = KustoClientCache.getClient(
      clusterUrl,
      authentication,
      ingestionUrl,
      clusterAlias,
      enableExportStorageApi = true)

    disabled.exportStorageApiEnabled shouldBe false
    enabled.exportStorageApiEnabled shouldBe true
    enabled should not be theSameInstanceAs(disabled)
    KustoClientCache
      .getClient(
        clusterUrl,
        authentication,
        ingestionUrl,
        clusterAlias,
        enableExportStorageApi = true) should be theSameInstanceAs enabled
  }

  "DistributedReadModeTransientCacheKey" should "isolate enabled and disabled export paths" in {
    val coordinates =
      KustoCoordinates(clusterUrl, clusterAlias, "database", ingestionUrl = ingestionUrl)

    DistributedReadModeTransientCacheKey(
      "query",
      coordinates,
      authentication,
      enableExportStorageApi = false) should not equal DistributedReadModeTransientCacheKey(
      "query",
      coordinates,
      authentication,
      enableExportStorageApi = true)
  }
}

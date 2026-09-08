// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{ClientRequestProperties, KustoOperationResult}
import io.github.resilience4j.retry.RetryConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.collection.concurrent.TrieMap

/** Thread-safety tests for opt-in export storage discovery. */
class ContainerProviderConcurrencyTest extends AnyFlatSpec with Matchers {
  private val clusterAlias = "somecluster"
  private val dmUrl = "https://ingest-somecluster.kusto.fabric.microsoft.com"
  private val engineUrl = "https://somecluster.kusto.fabric.microsoft.com"
  private val exportContainersCommand = ".show export containers"
  private val threads = 32

  private class CountingKustoClient
      extends ExtendedKustoClient(
        new ConnectionStringBuilder(engineUrl),
        new ConnectionStringBuilder(dmUrl),
        clusterAlias) {
    val commandCalls = new AtomicInteger(0)
    @volatile var failRefresh = false

    override def executeDM(
        command: String,
        maybeCrp: Option[ClientRequestProperties],
        activityName: String,
        retryConfig: Option[RetryConfig]): KustoOperationResult = {
      command shouldBe exportContainersCommand
      commandCalls.incrementAndGet()
      if (failRefresh) throw new RuntimeException("temporary command failure")
      new KustoOperationResult(
        """{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"StorageRoot","DataType":"String","ColumnType":"string"}],
          |"Rows":[["https://account.blob.core.windows.net/export?sv=x"]]}]}""".stripMargin,
        "v1")
    }
  }

  private class CountingExportStorageClient(
      result: Option[ExportStorageTargets],
      refreshInterval: Duration = DmExportStorageClient.DefaultRefreshInterval)
      extends DmExportStorageClient(new ConnectionStringBuilder(dmUrl), clusterAlias) {
    val callCount = new AtomicInteger(0)
    override private[kusto] def resolveExportStorage: ExportStorageResolution = {
      callCount.incrementAndGet()
      // Widen the window a real HTTP round trip would have, so a stampede is visible.
      Thread.sleep(50)
      ExportStorageResolution(result, refreshInterval)
    }
  }

  /** Releases every thread at once so the calls genuinely overlap. */
  private def runConcurrently(count: Int)(body: Int => Unit): Seq[Throwable] = {
    val pool = Executors.newFixedThreadPool(count)
    val start = new CountDownLatch(1)
    val done = new CountDownLatch(count)
    val errors = TrieMap.empty[Int, Throwable]
    (0 until count).foreach { i =>
      pool.submit(new Runnable {
        override def run(): Unit = {
          try {
            start.await()
            body(i)
          } catch {
            case t: Throwable => errors.put(i, t)
          } finally done.countDown()
        }
      })
    }
    try {
      start.countDown()
      done.await(60, TimeUnit.SECONDS) shouldBe true
      errors.values.toSeq
    } finally {
      start.countDown()
      pool.shutdownNow()
    }
  }

  "getExportContainers" should "issue a single service call when many threads race on a cold cache" in {
    val exportStorageClient = new CountingExportStorageClient(Some(ExportStorageTargets(
      containers = Seq.empty,
      lakeFolders = Seq(
        "https://ws.z45.daily-onelake.fabric.microsoft.com/ws-guid/artifact/Ingestions/export/a"))))
    val provider = new ContainerProvider(
      new CountingKustoClient,
      clusterAlias,
      exportContainersCommand,
      KustoConstants.StorageExpirySeconds,
      Some(exportStorageClient))

    val errors = runConcurrently(threads) { _ =>
      provider.getExportContainers should not be empty
    }

    errors shouldBe empty
    exportStorageClient.callCount.get() shouldBe 1
  }

  it should "issue one service refresh when callers race after the service interval" in {
    val exportStorageClient = new CountingExportStorageClient(
      Some(ExportStorageTargets(
        containers = Seq.empty,
        lakeFolders = Seq(
          "https://ws.z45.daily-onelake.fabric.microsoft.com/ws-guid/artifact/Ingestions/export/a"))),
      Duration.ofMillis(500))
    val provider = new ContainerProvider(
      new CountingKustoClient,
      clusterAlias,
      exportContainersCommand,
      KustoConstants.StorageExpirySeconds,
      Some(exportStorageClient))

    provider.getExportContainers should not be empty
    Thread.sleep(650)

    val errors = runConcurrently(threads) { _ =>
      provider.getExportContainers should not be empty
    }

    errors shouldBe empty
    exportStorageClient.callCount.get() shouldBe 2
  }

  it should "serialize command fallback when API-enabled callers race on a cold cache" in {
    val kustoClient = new CountingKustoClient
    val exportStorageClient = new CountingExportStorageClient(None)
    val provider = new ContainerProvider(
      kustoClient,
      clusterAlias,
      exportContainersCommand,
      KustoConstants.StorageExpirySeconds,
      Some(exportStorageClient))

    val errors = runConcurrently(threads) { _ =>
      provider.getExportContainers should not be empty
    }

    errors shouldBe empty
    exportStorageClient.callCount.get() shouldBe 1
    kustoClient.commandCalls.get() shouldBe 1
  }

  it should "back off failed command refreshes only for API-enabled exports" in {
    val kustoClient = new CountingKustoClient
    val exportStorageClient = new CountingExportStorageClient(None, Duration.ofMillis(500))
    val provider = new ContainerProvider(
      kustoClient,
      clusterAlias,
      exportContainersCommand,
      KustoConstants.StorageExpirySeconds,
      Some(exportStorageClient))

    provider.getExportContainers should not be empty
    kustoClient.failRefresh = true
    Thread.sleep(650)

    val errors = runConcurrently(threads) { _ =>
      provider.getExportContainers should not be empty
    }

    errors shouldBe empty
    exportStorageClient.callCount.get() shouldBe 2
    kustoClient.commandCalls.get() shouldBe 2

    kustoClient.failRefresh = false
    Thread.sleep(5100)
    provider.getExportContainers should not be empty
    exportStorageClient.callCount.get() shouldBe 3
    kustoClient.commandCalls.get() shouldBe 3
  }
}

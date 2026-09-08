// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

/** Concurrency and caching tests for export-storage mode discovery. */
class DmExportStorageClientGateTest extends AnyFlatSpec with Matchers {
  private val dmUrl = "https://ingest-somecluster.kusto.fabric.microsoft.com"
  private val clusterAlias = "somecluster"
  private val threads = 32

  /** Counts and optionally delays mode detection. */
  private class GateClient(
      answer: () => Option[ExportStorageMode],
      delayMs: Long = 0,
      refreshInterval: Duration = DmExportStorageClient.DefaultRefreshInterval,
      targets: ExportStorageTargets = ExportStorageTargets(
        containers = Seq("https://account.blob.core.windows.net/export?sv=x"),
        lakeFolders = Seq("https://ws.z45.onelake.fabric.microsoft.com/ws/artifact/export/f")))
      extends DmExportStorageClient(
        // Calls are intercepted, so a static token avoids network authentication.
        ConnectionStringBuilder.createWithAadAccessTokenAuthentication(dmUrl, "not-a-real-token"),
        clusterAlias) {
    val detections = new AtomicInteger(0)
    val fetches = new AtomicInteger(0)

    override protected def detectExportStorageDecision(): Option[ExportStorageDecision] = {
      detections.incrementAndGet()
      if (delayMs > 0) Thread.sleep(delayMs)
      answer().map(ExportStorageDecision(_, refreshInterval))
    }

    override protected def fetchExportStorage(attempt: Int): ExportStorageTargets = {
      fetches.incrementAndGet()
      targets
    }
  }

  private def gate(answer: Option[ExportStorageMode], delayMs: Long = 0) =
    new GateClient(() => answer, delayMs)

  private def hammer(client: GateClient)(call: GateClient => Any): Seq[Any] = {
    val pool = Executors.newFixedThreadPool(threads)
    val startLine = new CountDownLatch(1)
    try {
      val futures = (1 to threads).map { _ =>
        pool.submit[Any](() => {
          startLine.await()
          call(client)
        })
      }
      startLine.countDown()
      futures.map(_.get(30, TimeUnit.SECONDS))
    } finally {
      pool.shutdownNow()
    }
  }

  "the configuration gate" should "ask the service once for a burst of concurrent callers" in {
    val client = gate(Some(ExportStorageMode.LakeFolders), delayMs = 200)
    val answers = hammer(client)(_.exportStorageMode)
    answers.distinct shouldBe Seq(ExportStorageMode.LakeFolders)
    client.detections.get() shouldBe 1
  }

  it should "remember a definitive negative instead of re-asking on every refresh" in {
    val client = gate(Some(ExportStorageMode.LegacyCommand), delayMs = 50)
    val answers = hammer(client)(_.exportStorageMode)
    answers.distinct shouldBe Seq(ExportStorageMode.LegacyCommand)
    client.detections.get() shouldBe 1
    client.exportStorageMode shouldBe ExportStorageMode.LegacyCommand
    client.detections.get() shouldBe 1
  }

  it should "refresh the selected mode after the service interval" in {
    val answers = new AtomicInteger(0)
    val client = new GateClient(
      () =>
        if (answers.getAndIncrement() == 0) Some(ExportStorageMode.LakeFolders)
        else Some(ExportStorageMode.BlobContainers),
      refreshInterval = Duration.ofMillis(500))

    client.exportStorageMode shouldBe ExportStorageMode.LakeFolders
    client.exportStorageMode shouldBe ExportStorageMode.LakeFolders
    client.detections.get() shouldBe 1

    Thread.sleep(650)

    client.exportStorageMode shouldBe ExportStorageMode.BlobContainers
    client.detections.get() shouldBe 2
  }

  it should "not remember an inconclusive answer" in {
    val client = gate(None)
    client.exportStorageMode shouldBe ExportStorageMode.LegacyCommand
    client.exportStorageMode shouldBe ExportStorageMode.LegacyCommand
    client.detections.get() shouldBe 2
  }

  it should "recover as soon as the service answers again" in {
    val outage = new AtomicInteger(2)
    val client = new GateClient(() =>
      if (outage.getAndDecrement() > 0) None else Some(ExportStorageMode.BlobContainers))
    client.exportStorageMode shouldBe ExportStorageMode.LegacyCommand
    client.exportStorageMode shouldBe ExportStorageMode.LegacyCommand
    client.exportStorageMode shouldBe ExportStorageMode.BlobContainers
    client.exportStorageMode shouldBe ExportStorageMode.BlobContainers
    client.detections.get() shouldBe 3
  }

  "getExportStorage" should "not call the export storage API in legacy mode" in {
    val client = gate(Some(ExportStorageMode.LegacyCommand))
    client.getExportStorage shouldBe None
    client.resolveExportStorage.refreshInterval shouldBe
      DmExportStorageClient.DefaultRefreshInterval
    client.fetches.get() shouldBe 0
  }

  it should "not call the export storage API when the gate is inconclusive" in {
    val client = gate(None)
    client.getExportStorage shouldBe None
    client.fetches.get() shouldBe 0
  }

  it should "return only lake folders in lake mode" in {
    val client = gate(Some(ExportStorageMode.LakeFolders))
    val result = client.getExportStorage.get
    result.lakeFolders should have size 1
    result.containers shouldBe empty
    client.fetches.get() shouldBe 1
  }

  it should "return only blob containers in blob mode" in {
    val client = gate(Some(ExportStorageMode.BlobContainers))
    val result = client.getExportStorage.get
    result.containers should have size 1
    result.lakeFolders shouldBe empty
    client.fetches.get() shouldBe 1
  }

  it should "fall back when the API has no target for the selected mode" in {
    val lakeModeWithOnlyBlobs = new GateClient(
      () => Some(ExportStorageMode.LakeFolders),
      targets = ExportStorageTargets(
        containers = Seq("https://account.blob.core.windows.net/export?sv=x"),
        lakeFolders = Seq.empty))
    lakeModeWithOnlyBlobs.getExportStorage shouldBe None

    val blobModeWithOnlyLake = new GateClient(
      () => Some(ExportStorageMode.BlobContainers),
      targets = ExportStorageTargets(
        containers = Seq.empty,
        lakeFolders = Seq("https://ws.z45.onelake.fabric.microsoft.com/ws/artifact/export/f")))
    blobModeWithOnlyLake.getExportStorage shouldBe None
  }

  it should "stay consistent when concurrent callers race the gate and the fetch" in {
    val client = gate(Some(ExportStorageMode.LakeFolders), delayMs = 100)
    val results = hammer(client)(_.getExportStorage)
    results.distinct should have size 1
    results.head.asInstanceOf[Option[ExportStorageTargets]].map(_.lakeFolders.size) shouldBe Some(
      1)
    client.detections.get() shouldBe 1
    // ContainerProvider, not this client, serializes target fetches.
    client.fetches.get() shouldBe threads
  }
}

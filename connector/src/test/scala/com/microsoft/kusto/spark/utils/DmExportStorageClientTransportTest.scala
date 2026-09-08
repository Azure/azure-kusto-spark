// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.azure.core.http.{HttpClient, HttpHeaders, HttpRequest, HttpResponse}
import com.microsoft.azure.kusto.data.auth.{CloudInfo, ConnectionStringBuilder}
import com.microsoft.azure.kusto.data.{ClientRequestProperties, KustoOperationResult}
import io.github.resilience4j.retry.RetryConfig
import org.awaitility.Awaitility.await
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import reactor.core.publisher.{Flux, Mono}

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.zip.GZIPOutputStream
import scala.collection.JavaConverters._

/** Request-layer tests with an in-memory HTTP transport. */
class DmExportStorageClientTransportTest extends AnyFlatSpec with Matchers {
  private val dmUrl = "https://ingest-somecluster.kusto.fabric.microsoft.com"
  private val notKustoUrl = "https://ingest-somecluster.example.com"
  private val clusterAlias = "somecluster"
  private val accessToken = "not-a-real-token"

  // Avoid network metadata lookup without bypassing trusted-host validation.
  CloudInfo.manuallyAddToCache(dmUrl, Mono.just(CloudInfo.DEFAULT_CLOUD))
  CloudInfo.manuallyAddToCache(notKustoUrl, Mono.just(CloudInfo.DEFAULT_CLOUD))

  private val lakeFolder =
    "https://ws.z45.onelake.fabric.microsoft.com/ws/artifact/Ingestions/export/f"

  private val restLake =
    """{"containerSettings":{"containers":[{"path":"https://blob/c?sas"}],
      |"lakeFolders":[{"path":"https://ws/lake"}],"preferredUploadMethod":"Lake",
      |"refreshInterval":"01:00:00"},
      |"ingestionSettings":{"preferredIngestionMethod":"Rest"}}""".stripMargin

  private val queueStorage =
    """{"containerSettings":{"containers":[{"path":"https://blob/c?sas"}],
      |"lakeFolders":[{"path":"https://ws/lake"}],"preferredUploadMethod":"Storage"},
      |"ingestionSettings":{"preferredIngestionMethod":"Queue"}}""".stripMargin

  private val exportTargets =
    s"""{"exportStorage":{"containers":[{"path":"https://blob/export?sas"}],
       |"lakeFolders":[{"path":"$lakeFolder"}]}}""".stripMargin

  private class StubResponse(
      request: HttpRequest,
      status: Int,
      bytes: Array[Byte],
      headers: Map[String, String])
      extends HttpResponse(request) {
    private def body = new String(bytes, StandardCharsets.UTF_8)
    override def getStatusCode: Int = status
    override def getHeaderValue(name: String): String = headers.getOrElse(name, null)
    override def getHeaders: HttpHeaders = new HttpHeaders(headers.asJava)
    override def getBody: Flux[ByteBuffer] = Mono.just(ByteBuffer.wrap(bytes)).flux()
    override def getBodyAsByteArray: Mono[Array[Byte]] = Mono.just(bytes)
    override def getBodyAsString: Mono[String] = Mono.just(body)
    override def getBodyAsString(charset: java.nio.charset.Charset): Mono[String] =
      Mono.just(body)
  }

  private class StubHttpClient(respond: HttpRequest => (Int, String, Map[String, String]))
      extends HttpClient {
    val requests = new ConcurrentLinkedQueue[HttpRequest]()

    override def send(request: HttpRequest): Mono[HttpResponse] = {
      requests.add(request)
      val (status, body, headers) = respond(request)
      Mono.just(new StubResponse(request, status, body.getBytes(StandardCharsets.UTF_8), headers))
    }

    def urls: Seq[String] = requests.asScala.map(_.getUrl.toString).toSeq
    def configCalls: Int = urls.count(_.contains("/ingestion/configuration"))
    def exportCalls: Int = urls.count(_.contains("/ingestion/exportstorage"))
  }

  private class TransportClient(val stub: StubHttpClient, url: String = dmUrl)
      extends DmExportStorageClient(
        ConnectionStringBuilder.createWithAadAccessTokenAuthentication(url, accessToken),
        clusterAlias) {
    override protected lazy val httpClient: HttpClient = stub
  }

  private class CommandClient
      extends ExtendedKustoClient(
        new ConnectionStringBuilder("https://somecluster.kusto.fabric.microsoft.com"),
        new ConnectionStringBuilder(dmUrl),
        clusterAlias) {
    var calls = 0

    override def executeDM(
        command: String,
        maybeCrp: Option[ClientRequestProperties],
        activityName: String,
        retryConfig: Option[RetryConfig]): KustoOperationResult = {
      command shouldBe ".show export containers"
      calls += 1
      new KustoOperationResult(
        """{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"StorageRoot","DataType":"String","ColumnType":"string"}],
          |"Rows":[["https://legacy.blob.core.windows.net/export?sv=x"]]}]}""".stripMargin,
        "v1")
    }
  }

  private def routed(
      config: => (Int, String, Map[String, String]),
      export: => (Int, String, Map[String, String]) = (200, exportTargets, Map.empty)) =
    new StubHttpClient(request =>
      if (request.getUrl.toString.contains("/ingestion/configuration")) config else export)

  private def ok(body: String) = (200, body, Map.empty[String, String])

  private def gzip(body: String): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val gzip = new GZIPOutputStream(output)
    try {
      gzip.write(body.getBytes(StandardCharsets.UTF_8))
    } finally {
      gzip.close()
    }
    output.toByteArray
  }

  behavior of "DmExportStorageClient over a real HTTP layer"

  it should "call the configuration route first, and only then the export route" in {
    val stub = routed(ok(restLake))
    val client = new TransportClient(stub)

    val resolution = client.resolveExportStorage
    val targets = resolution.targets

    targets.map(_.lakeFolders) shouldBe Some(Seq(lakeFolder))
    resolution.refreshInterval shouldBe java.time.Duration.ofHours(1)
    stub.urls should have size 2
    stub.urls.head shouldBe s"$dmUrl/v1/rest/ingestion/configuration"
    stub.urls(1) shouldBe s"$dmUrl/v1/rest/ingestion/exportstorage"
  }

  it should "send the bearer token and the request id on every call" in {
    val stub = routed(ok(restLake))
    new TransportClient(stub).getExportStorage

    stub.requests.asScala.foreach { request =>
      request.getHeaders.getValue("Authorization") shouldBe s"Bearer $accessToken"
      request.getHeaders.getValue("Accept") shouldBe "application/json"
      request.getHeaders.getValue("x-ms-client-request-id") should not be empty
    }
  }

  it should "not touch the export route when the cluster advertises Storage and Queue" in {
    val stub = routed(ok(queueStorage))

    new TransportClient(stub).getExportStorage shouldBe None

    stub.configCalls shouldBe 1
    stub.exportCalls shouldBe 0
  }

  it should "not engage for Queue and Lake" in {
    val queueLake =
      """{"containerSettings":{"preferredUploadMethod":"Lake"},
        |"ingestionSettings":{"preferredIngestionMethod":"Queue"}}""".stripMargin
    val stub = routed(ok(queueLake))

    new TransportClient(stub).getExportStorage shouldBe None
    stub.exportCalls shouldBe 0
  }

  it should "select blob containers for Rest and Storage" in {
    val restStorage =
      """{"containerSettings":{"preferredUploadMethod":"Storage"},
        |"ingestionSettings":{"preferredIngestionMethod":"Rest"}}""".stripMargin
    val stub = routed(ok(restStorage))

    val targets = new TransportClient(stub).getExportStorage.get
    targets.containers shouldBe Seq("https://blob/export?sas")
    targets.lakeFolders shouldBe empty
    stub.exportCalls shouldBe 1
  }

  it should "refuse to send a token over a non-https endpoint" in {
    val stub = routed(ok(restLake))
    val client = new TransportClient(stub, url = "http://ingest-somecluster.kusto.windows.net")

    val thrown = intercept[UntrustedEndpointException](client.getExportStorage)

    thrown.getMessage should include("non-https")
    withClue("the token must not have left the process") {
      stub.requests shouldBe empty
    }
  }

  it should "refuse to send a token to a host the SDK rejects as untrusted" in {
    val stub = routed(ok(restLake))
    val client = new TransportClient(stub, url = notKustoUrl)

    val thrown = intercept[UntrustedEndpointException](client.getExportStorage)

    thrown.getMessage should include("not recognise it as a trusted endpoint")
    withClue("the token must not have left the process") {
      stub.requests shouldBe empty
    }
  }

  it should "not report an unreachable validation as an untrusted endpoint" in {
    val unresolvable = "https://ingest-nosuchcluster-9f3a2b.kusto.fabric.microsoft.com"
    val stub = routed(ok(restLake))
    val client = new TransportClient(stub, url = unresolvable)

    withClue("an unreachable check falls back rather than failing the read") {
      client.getExportStorage shouldBe None
    }
    stub.requests shouldBe empty
  }

  it should "fall back when the configuration route rejects the REST request" in {
    val stub = routed((401, """{"error":"unauthorized"}""", Map.empty[String, String]))
    val client = new TransportClient(stub)

    client.resolveExportStorage shouldBe
      ExportStorageResolution(None, DmExportStorageClient.DefaultRefreshInterval)
    client.getExportStorage shouldBe None

    stub.configCalls shouldBe 1
    stub.exportCalls shouldBe 0
  }

  it should "fall back when the export route rejects the REST request" in {
    val stub =
      routed(ok(restLake), export = (403, """{"error":"forbidden"}""", Map.empty[String, String]))

    new TransportClient(stub).resolveExportStorage shouldBe
      ExportStorageResolution(None, Duration.ofHours(1))
    stub.configCalls shouldBe 1
    stub.exportCalls shouldBe 1
  }

  it should "treat a 404 on the configuration route as an answer and remember it" in {
    val stub = routed((404, "", Map.empty[String, String]))
    val client = new TransportClient(stub)

    client.resolveExportStorage shouldBe
      ExportStorageResolution(None, DmExportStorageClient.DefaultRefreshInterval)
    client.getExportStorage shouldBe None

    withClue("a 404 is an answer, so the second call must not re-ask within the window") {
      stub.configCalls shouldBe 1
    }
    stub.exportCalls shouldBe 0
  }

  it should "not remember an unparseable 200, so the next refresh asks again" in {
    val stub = routed(ok("this is not json"))
    val client = new TransportClient(stub)

    client.getExportStorage shouldBe None
    client.getExportStorage shouldBe None

    withClue("an inconclusive answer must not be cached") {
      // Two calls, each of which retries once: the gate is bounded to two attempts.
      stub.configCalls shouldBe 4
    }
  }

  it should "retry a non-object configuration section before falling back" in {
    val stub = routed(ok("""{"containerSettings":[]}"""))

    new TransportClient(stub).resolveExportStorage shouldBe
      ExportStorageResolution(None, Duration.ofSeconds(5))

    stub.configCalls shouldBe 2
    stub.exportCalls shouldBe 0
  }

  it should "retry a non-object export storage section before falling back" in {
    val stub = routed(ok(restLake), export = ok("""{"exportStorage":[]}"""))

    new TransportClient(stub).resolveExportStorage shouldBe
      ExportStorageResolution(None, Duration.ofSeconds(5))

    stub.configCalls shouldBe 1
    stub.exportCalls shouldBe 3
  }

  it should "recover once the configuration route starts answering" in {
    val failing = new AtomicBoolean(true)
    val stub = new StubHttpClient(request =>
      if (request.getUrl.toString.contains("/ingestion/configuration")) {
        if (failing.get()) (503, "unavailable", Map.empty[String, String]) else ok(restLake)
      } else (200, exportTargets, Map.empty[String, String]))
    val client = new TransportClient(stub)

    client.getExportStorage shouldBe None
    stub.configCalls shouldBe 2
    failing.set(false)

    client.getExportStorage.map(_.lakeFolders) shouldBe Some(Seq(lakeFolder))
    stub.configCalls shouldBe 3
  }

  Seq("configuration", "exportstorage").foreach { failingRoute =>
    it should s"rediscover API targets after a transient $failingRoute failure and successful legacy fallback" in {
      val failing = new AtomicBoolean(true)
      val stub = new StubHttpClient(request =>
        if (failing.get() && request.getUrl.getPath.endsWith(failingRoute)) {
          (503, "unavailable", Map.empty[String, String])
        } else if (request.getUrl.getPath.endsWith("configuration")) {
          ok(restLake)
        } else {
          ok(exportTargets)
        })
      val commandClient = new CommandClient
      val provider = new ContainerProvider(
        commandClient,
        clusterAlias,
        ".show export containers",
        maybeExportStorageClient = Some(new TransportClient(stub)))

      val fallback = provider.getExportContainers
      fallback.map(_.containerUrl) shouldBe Seq("https://legacy.blob.core.windows.net/export")
      commandClient.calls shouldBe 1
      val callsBeforeRecovery = stub.requests.size()

      failing.set(false)
      provider.getExportContainers shouldBe fallback
      stub.requests.size() shouldBe callsBeforeRecovery

      await()
        .atMost(Duration.ofSeconds(8))
        .until(() => provider.getExportContainers.map(_.containerUrl) == Seq(lakeFolder))
      commandClient.calls shouldBe 1
      stub.configCalls shouldBe (if (failingRoute == "configuration") 3 else 1)
      stub.exportCalls shouldBe (if (failingRoute == "configuration") 1 else 4)

      val callsAfterRecovery = stub.requests.size()
      provider.getExportContainers.map(_.containerUrl) shouldBe Seq(lakeFolder)
      stub.requests.size() shouldBe callsAfterRecovery
    }
  }

  it should "stop asking the export route for the window after it 404s" in {
    val stub = routed(ok(restLake), export = (404, "", Map.empty[String, String]))
    val client = new TransportClient(stub)

    client.resolveExportStorage shouldBe
      ExportStorageResolution(None, Duration.ofHours(1))
    client.getExportStorage shouldBe None

    withClue("the 404 is remembered, so the route is asked once") {
      stub.exportCalls shouldBe 1
    }
  }

  it should "refuse a body that declares itself larger than the limit" in {
    val oversized =
      Map("Content-Length" -> (DmExportStorageClient.MaxResponseBodyBytes + 1).toString)
    val stub = routed((200, restLake, oversized))

    new TransportClient(stub).getExportStorage shouldBe None
  }

  it should "stop buffering a chunked body when it crosses the limit" in {
    val oversizedBody = "x" * (DmExportStorageClient.MaxResponseBodyBytes.toInt + 1)
    val stub = routed(ok(oversizedBody))

    new TransportClient(stub).getExportStorage shouldBe None
    stub.configCalls shouldBe 1
  }

  it should "stop decoding a compressed body when it crosses the limit" in {
    val bytes = gzip("x" * (DmExportStorageClient.MaxResponseBodyBytes.toInt + 1))
    val stub = new StubHttpClient(_ => throw new IllegalStateException("unused")) {
      override def send(request: HttpRequest): Mono[HttpResponse] = {
        requests.add(request)
        Mono.just(new StubResponse(request, 200, bytes, Map("Content-Encoding" -> "gzip")))
      }
    }

    new TransportClient(stub).getExportStorage shouldBe None
    stub.configCalls shouldBe 1
  }

  it should "avoid retrying permanent client failures" in {
    val stub = routed((400, """{"error":"bad request"}""", Map.empty[String, String]))
    val client = new TransportClient(stub)

    client.resolveExportStorage shouldBe
      ExportStorageResolution(None, DmExportStorageClient.DefaultRefreshInterval)
    client.getExportStorage shouldBe None
    stub.configCalls shouldBe 1
  }

  it should "retain the service interval after a permanent export-storage failure" in {
    val stub =
      routed(ok(restLake), export = (400, "bad request", Map.empty[String, String]))

    new TransportClient(stub).resolveExportStorage shouldBe
      ExportStorageResolution(None, Duration.ofHours(1))
    stub.configCalls shouldBe 1
    stub.exportCalls shouldBe 1
  }

  it should "bound retries for transient export-storage failures" in {
    val stub =
      routed(ok(restLake), export = (503, "unavailable", Map.empty[String, String]))

    new TransportClient(stub).resolveExportStorage shouldBe
      ExportStorageResolution(None, Duration.ofSeconds(5))
    stub.configCalls shouldBe 1
    stub.exportCalls shouldBe 3
  }

  it should "stop retrying when a throttled request is interrupted" in {
    val stub = routed((429, "throttled", Map("Retry-After" -> "5")))
    val failure = new AtomicReference[Throwable]()
    val interruptPreserved = new AtomicBoolean(false)
    val thread = new Thread(new Runnable {
      override def run(): Unit =
        try {
          new TransportClient(stub).getExportStorage
        } catch {
          case throwable: Throwable =>
            failure.set(throwable)
            interruptPreserved.set(Thread.currentThread().isInterrupted)
        }
    })

    thread.start()
    val requestDeadline = System.currentTimeMillis() + 2000
    while (stub.configCalls == 0 && System.currentTimeMillis() < requestDeadline) {
      Thread.sleep(10)
    }
    stub.configCalls shouldBe 1

    thread.interrupt()
    thread.join(2000)

    thread.isAlive shouldBe false
    failure.get() shouldBe a[InterruptedException]
    interruptPreserved.get() shouldBe true
    stub.configCalls shouldBe 1
  }

  it should "fall back rather than fail when the route is unreachable" in {
    val stub = new StubHttpClient(_ => throw new RuntimeException("connection reset"))

    new TransportClient(stub).resolveExportStorage shouldBe
      ExportStorageResolution(None, Duration.ofSeconds(5))
  }

  it should "fall back when the export route answers with no targets at all" in {
    val empty = """{"exportStorage":{"containers":[],"lakeFolders":[]}}"""
    val stub = routed(ok(restLake), export = (200, empty, Map.empty[String, String]))

    new TransportClient(stub).resolveExportStorage shouldBe
      ExportStorageResolution(None, Duration.ofHours(1))
  }
}

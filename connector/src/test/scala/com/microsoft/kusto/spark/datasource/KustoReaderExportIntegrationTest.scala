// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import com.azure.storage.blob.{BlobContainerClient, BlobContainerClientBuilder}
import com.azure.storage.blob.models.BlobStorageException
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.kusto.data.{Client, ClientFactory, ClientRequestProperties}
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.exceptions.DataServiceException
import com.microsoft.azure.kusto.data.http.HttpClientFactory
import com.microsoft.kusto.spark.authentication.KustoAccessTokenAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.utils.ExtendedKustoClient
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.hadoop.fs.{
  FileStatus,
  FSDataOutputStream,
  Path => HadoopPath,
  RawLocalFileSystem
}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.schema.MessageTypeParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.mockito.MockMakers
import org.mockito.Mockito.{mock, when, withSettings}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{InetAddress, InetSocketAddress, URI, URLDecoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal

// NIO file creation and listing avoid Hadoop's Windows native helpers.
class KustoReadbackFileSystem extends RawLocalFileSystem {
  override def getUri: URI = URI.create("kustotest:///")

  override def create(
      path: HadoopPath,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    create(
      path,
      FsPermission.getFileDefault,
      overwrite,
      bufferSize,
      replication,
      blockSize,
      progress)

  override def create(
      path: HadoopPath,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    val options =
      if (overwrite) Array(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      else Array(StandardOpenOption.CREATE_NEW)
    new FSDataOutputStream(
      Files.newOutputStream(pathToFile(path).toPath, options: _*),
      statistics)
  }

  override def listStatus(path: HadoopPath): Array[FileStatus] = {
    val status = getFileStatus(path)
    if (!status.isDirectory) Array(status)
    else {
      val children = Files.list(pathToFile(path).toPath)
      try
        children
          .iterator()
          .asScala
          .map(child => getFileStatus(new HadoopPath(path, child.getFileName.toString)))
          .toArray
      finally children.close()
    }
  }
}

/**
 * Validates SDK HTTP -> blob prefix checks -> export path selection -> local Parquet. Cloud
 * filesystem authentication is not exercised.
 */
class KustoReaderExportIntegrationTest extends AnyFlatSpec with Matchers {
  private val mapper = new ObjectMapper()
  private val database = "local-export-test"
  private val requestId = "local-export-integration"
  private val namePrefix = """namePrefix="([^"]+)"""".r
  private val capacity =
    "CommandType: 'DataExportToFile', Capacity: 1, Origin: 'CapacityPolicy/Export'."

  private case class Outcome(
      submissionStatus: Int = 200,
      state: String = "Completed",
      pollStatuses: Seq[Int] = Seq(200))
  private case class Export(
      csl: String,
      clientRequestId: String,
      outcome: Outcome,
      operationId: String) {
    def prefix: String = namePrefix.findFirstMatchIn(csl).get.group(1)
    def directory: String = prefix.replaceFirst("part[0-9]+$", "")
  }

  private def table(columns: Seq[String], values: Seq[String]): String = {
    val columnJson = columns
      .map(name => s"""{"ColumnName":"$name","DataType":"String","ColumnType":"string"}""")
      .mkString(",")
    val row = values.map(mapper.writeValueAsString).mkString(",")
    s"""{"Tables":[{"TableName":"Table_0","Columns":[$columnJson],"Rows":[[$row]]}]}"""
  }

  private class Fixture(outcomes: Seq[Outcome], spark: SparkSession) {
    val exports = new ConcurrentLinkedQueue[Export]()
    val polls = new ConcurrentLinkedQueue[(String, Int)]()
    val blobNames = TrieMap.empty[String, Seq[String]]
    val blobRequests = new ConcurrentLinkedQueue[(String, Map[String, String])]()
    val emptyFirstBlobPage = TrieMap.empty[String, Boolean]
    val deniedBlobContainers = TrieMap.empty[String, Boolean]
    val delays = mutable.ArrayBuffer.empty[Long]
    private val handlerFailure = new AtomicReference[Throwable]()
    private val executor = Executors.newSingleThreadExecutor()
    private val server =
      HttpServer.create(new InetSocketAddress(InetAddress.getByName("localhost"), 0), 0)
    private val operations = mutable.Map.empty[String, (Outcome, Int)]
    private var closeTransport: () => Unit = () => ()
    private lazy val transport = {
      val created = HttpClientFactory.create(null)
      created match {
        case closeable: AutoCloseable => closeTransport = () => closeable.close()
        case _ => () // Some HTTP providers use shared resources.
      }
      created
    }
    private val endpoint = s"http://localhost:${server.getAddress.getPort}"
    private val csb =
      ConnectionStringBuilder.createWithAadAccessTokenAuthentication(endpoint, "unused")
    private val client = new ExtendedKustoClient(csb, csb, "localhost") {
      override lazy val engineClient: Client = ClientFactory.createClient(csb, transport)
    }
    val reader: KustoReader = new KustoReader(client) {
      override private[kusto] def waitBeforeExportRetry(delayMillis: Long): Unit = {
        delays += delayMillis
      }
    }
    private val crp = new ClientRequestProperties()
    crp.setClientRequestId(requestId)
    private val request = KustoReadRequest(
      spark,
      KustoSchema(StructType(Seq(StructField("value", LongType))), Set.empty),
      KustoCoordinates(endpoint, "localhost", database, None),
      "LocalSource",
      KustoAccessTokenAuthentication("unused"),
      2.minutes,
      Some(crp),
      requestId)
    private val storage = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials(
          "https://dummyaccount.blob.core.windows.net/dummycontainer?sig=unused")))

    def blobContainer(name: String): BlobContainerClient = new BlobContainerClientBuilder()
      .endpoint(endpoint)
      .containerName(name)
      .buildClient()

    private def reply(
        exchange: HttpExchange,
        status: Int,
        body: String,
        contentType: String = "application/json"): Unit = {
      val bytes = body.getBytes(UTF_8)
      exchange.getResponseHeaders.set("Content-Type", contentType)
      exchange.sendResponseHeaders(status, bytes.length.toLong)
      exchange.getResponseBody.write(bytes)
    }

    private def listBlobs(exchange: HttpExchange, container: String): Unit = {
      require(exchange.getRequestMethod == "GET")
      val query = exchange.getRequestURI.getRawQuery
        .split("&")
        .map { part =>
          val pair = part.split("=", 2)
          URLDecoder.decode(pair(0), UTF_8.name()) -> URLDecoder.decode(pair(1), UTF_8.name())
        }
        .toMap
      require(query("restype") == "container" && query("comp") == "list")
      blobRequests.add((container, query))
      if (deniedBlobContainers.getOrElse(container, false)) {
        reply(
          exchange,
          403,
          "<Error><Code>AuthorizationPermissionMismatch</Code><Message>Denied</Message></Error>",
          "application/xml")
      } else {
        val prefix = query.getOrElse("prefix", "")
        val delimiter = query.get("delimiter").filter(_.nonEmpty)
        val items = blobNames(container)
          .filter(_.startsWith(prefix))
          .map { name =>
            val remainder = name.substring(prefix.length)
            delimiter match {
              case Some(separator) if remainder.contains(separator) =>
                (prefix + remainder.take(remainder.indexOf(separator) + separator.length), true)
              case _ => (name, false)
            }
          }
          .distinct
          .sortBy(_._1)
        val offset = query.get("marker").fold(0)(_.toInt)
        val emptyPage =
          emptyFirstBlobPage.getOrElse(container, false) && !query.contains("marker")
        val page = if (emptyPage) Seq.empty else items.slice(offset, offset + 1)
        val nextMarker =
          if (emptyPage) "0"
          else if (offset + 1 < items.size) (offset + 1).toString
          else ""
        val blobs = page.map {
          case (name, true) => s"<BlobPrefix><Name>$name</Name></BlobPrefix>"
          case (name, false) =>
            s"<Blob><Name>$name</Name><Properties><BlobType>BlockBlob</BlobType></Properties></Blob>"
        }.mkString
        reply(
          exchange,
          200,
          s"""<?xml version="1.0" encoding="utf-8"?>
             |<EnumerationResults ServiceEndpoint="$endpoint/" ContainerName="$container">
             |<Prefix>$prefix</Prefix><Blobs>$blobs</Blobs><NextMarker>$nextMarker</NextMarker>
             |</EnumerationResults>""".stripMargin,
          "application/xml")
      }
    }

    private def handle(exchange: HttpExchange): Unit = {
      require(exchange.getRemoteAddress.getAddress.isLoopbackAddress)
      require(exchange.getRequestHeaders.getFirst("Authorization") == null)
      exchange.getRequestURI.getPath match {
        // An ephemeral localhost port is not the SDK's pre-cached, portless localhost URL.
        case "/v1/rest/auth/metadata" => reply(exchange, 404, "{}")
        case "/v1/rest/mgmt" =>
          require(exchange.getRequestMethod == "POST")
          val json = mapper.readTree(exchange.getRequestBody)
          require(json.path("db").asText() == database)
          val csl = json.path("csl").asText()
          if (csl.startsWith(".export async ")) {
            val index = exports.size()
            val currentTestOutcome = outcomes
              .lift(index)
              .getOrElse(
                throw new IllegalStateException(s"Unexpected export submission ${index + 1}"))
            val operationId = new UUID(0, index.toLong + 1).toString
            exports.add(
              Export(
                csl,
                exchange.getRequestHeaders.getFirst("x-ms-client-request-id"),
                currentTestOutcome,
                operationId))
            if (currentTestOutcome.submissionStatus == 200) {
              operations(operationId) = (currentTestOutcome, 0)
              reply(exchange, 200, table(Seq("OperationId"), Seq(operationId)))
            } else
              reply(
                exchange,
                currentTestOutcome.submissionStatus,
                """{"message":"submission outcome unknown"}""")
          } else {
            require(csl.startsWith(".show operations "), s"Unexpected management command: $csl")
            val operationId = csl.stripPrefix(".show operations ")
            val (currentTestOutcome, index) = operations(operationId)
            val status = currentTestOutcome.pollStatuses
              .lift(index)
              .getOrElse(
                throw new IllegalStateException(s"Unexpected extra poll for $operationId"))
            operations(operationId) = (currentTestOutcome, index + 1)
            polls.add((operationId, status))
            val body =
              if (status == 200)
                table(
                  Seq("OperationId", "State", "Status"),
                  Seq(
                    operationId,
                    currentTestOutcome.state,
                    if (currentTestOutcome.state == "Throttled") capacity else ""))
              else """{"message":"status lookup throttled"}"""
            reply(exchange, status, body)
          }
        case path if path.startsWith("/blob-") =>
          listBlobs(exchange, path.stripPrefix("/"))
        case path => throw new IllegalStateException(s"Unexpected SDK route: $path")
      }
    }

    def start(): Unit = {
      server.setExecutor(executor)
      server.createContext(
        "/",
        new HttpHandler {
          override def handle(exchange: HttpExchange): Unit = {
            try Fixture.this.handle(exchange)
            catch {
              case NonFatal(error) =>
                handlerFailure.compareAndSet(null, error)
                reply(exchange, 500, """{"message":"local fixture failure"}""")
            } finally exchange.close()
          }
        })
      server.start()
    }

    def run(partitions: Int = 1): Seq[String] = reader.exportPartitionsToBlob(
      (0 until partitions).map(id => KustoPartition(None, id)),
      request,
      storage,
      "read/",
      KustoReadOptions(
        partitionOptions = PartitionOptions(partitions, None, Some("hash")),
        queryFilterPushDown = None),
      KustoFiltering())

    def close(): Unit = {
      try server.stop(0)
      finally {
        executor.shutdownNow()
        try
          assert(
            executor.awaitTermination(5, TimeUnit.SECONDS),
            "HTTP executor did not terminate")
        finally closeTransport()
      }
      Option(handlerFailure.get()).foreach(error =>
        throw new AssertionError("HTTP fixture failed", error))
    }
  }

  // HTTP-only cases need Spark version metadata, not a SparkContext or mocked SDK execution.
  private def versionOnlySpark: SparkSession = {
    val spark = mock(classOf[SparkSession], withSettings().mockMaker(MockMakers.SUBCLASS))
    when(spark.version).thenReturn(org.apache.spark.SPARK_VERSION)
    spark
  }

  private def withFixture(outcomes: Seq[Outcome], spark: SparkSession = versionOnlySpark)(
      test: Fixture => Unit): Unit = {
    val fixture = new Fixture(outcomes, spark)
    try {
      fixture.start()
      test(fixture)
    } finally fixture.close()
  }

  private def withLocalSpark(test: (SparkSession, Path) => Unit): Unit = {
    val previousActive = SparkSession.getActiveSession
    val previousDefault = SparkSession.getDefaultSession
    val root = Files.createTempDirectory("kusto-export-integration-")
    var ownedContext: Option[SparkContext] = None
    try {
      val existingSession = previousActive
        .filterNot(_.sparkContext.isStopped)
        .orElse(previousDefault.filterNot(_.sparkContext.isStopped))
      val spark = existingSession match {
        case Some(existing) =>
          require(
            existing.sparkContext.master.startsWith("local"),
            "Parquet regression requires local Spark")
          existing.newSession()
        case None =>
          // Own the context explicitly; a session builder can reuse another suite's context.
          val context = new SparkContext(
            new SparkConf()
              .setMaster("local[1]")
              .setAppName("KustoReaderExportIntegrationTest")
              .set("spark.ui.enabled", "false")
              .set("spark.sql.warehouse.dir", root.resolve("warehouse").toUri.toString))
          ownedContext = Some(context)
          SparkSession.builder().getOrCreate()
      }
      val conf = spark.sparkContext.hadoopConfiguration
      val settings = Map(
        "fs.kustotest.impl" -> classOf[KustoReadbackFileSystem].getName,
        "fs.kustotest.impl.disable.cache" -> "true")
      val previous = settings.keys.map(key => key -> Option(conf.get(key))).toMap
      try {
        settings.foreach { case (key, value) => conf.set(key, value) }
        test(spark, root)
      } finally {
        previous.foreach { case (key, value) =>
          value match {
            case Some(original) => conf.set(key, original)
            case None => conf.unset(key)
          }
        }
      }
    } finally {
      try ownedContext.foreach(_.stop())
      finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        previousActive.foreach(SparkSession.setActiveSession)
        previousDefault.foreach(SparkSession.setDefaultSession)
        val paths = Files.walk(root)
        val created =
          try paths.iterator().asScala.toVector
          finally paths.close()
        created.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))
      }
    }
  }

  "Local SDK export integration" should "retry HTTP 429 with distinct prefixes and request IDs" in {
    withFixture(Seq(Outcome(submissionStatus = 429), Outcome())) { f =>
      val directories = f.run()
      val sent = f.exports.asScala.toSeq
      sent.size shouldBe 2
      sent.map(_.prefix).distinct.size shouldBe 2
      sent.map(_.clientRequestId).distinct.size shouldBe 2
      all(sent.map(_.clientRequestId)) should startWith(s"$requestId;")
      directories shouldBe Seq(sent.last.directory)
      f.polls.asScala.toSeq shouldBe Seq((sent.last.operationId, 200))
      f.delays.size shouldBe 1
    }
  }

  it should "read only successful attempts when retry output is uneven across blob containers" in {
    withLocalSpark { (spark, root) =>
      withFixture(Seq(Outcome(state = "Throttled"), Outcome(), Outcome()), spark) { f =>
        val directories = f.run(partitions = 2)
        val sent = f.exports.asScala.toSeq
        sent.size shouldBe 3 // Two submissions for partition 0; one for partition 1.
        sent.count(_.directory.contains("/part0/")) shouldBe 2
        sent.count(_.directory.contains("/part1/")) shouldBe 1
        sent.take(2).map(_.operationId).distinct.size shouldBe 2
        sent.map(_.prefix).distinct.size shouldBe 3
        sent.map(_.clientRequestId).distinct.size shouldBe 3
        f.polls.asScala.toSeq shouldBe sent.map(e => (e.operationId, 200))
        f.delays.size shouldBe 1
        directories shouldBe sent.tail.map(_.directory)
        directories.distinct.size shouldBe 2
        def local(container: String, directory: String): String = {
          val path = root.resolve(container).resolve(directory).normalize()
          require(path.startsWith(root), "Export path escaped the local fixture")
          new URI("kustotest", null, path.toUri.getPath, null, null).toString
        }
        val output =
          Seq((sent(0), "blob-a", 999L), (sent(1), "blob-b", 10L), (sent(2), "blob-a", 20L))
        output.groupBy(_._2).foreach { case (container, attempts) =>
          f.blobNames(container) = attempts.map(_._1.directory + "part.parquet")
        }
        output.foreach { case (attempt, container, value) =>
          val path = root.resolve(container).resolve(attempt.directory)
          Files.createDirectories(path)
          val schema =
            MessageTypeParser.parseMessageType("message export { required int64 value; }")
          // NIO fixture writes avoid Hadoop's Windows-only winutils prerequisite.
          val writer = ExampleParquetWriter
            .builder(new HadoopPath(local(container, attempt.directory), "part.parquet"))
            .withConf(spark.sparkContext.hadoopConfiguration)
            .withType(schema)
            .build()
          try writer.write(new SimpleGroupFactory(schema).newGroup().append("value", value))
          finally writer.close()
        }
        spark.read
          .parquet(local("blob-a", sent.head.directory))
          .collect()
          .map(_.getLong(0))
          .toSeq shouldBe Seq(999L)
        val storage = new TransientStorageParameters(
          Array("blob-a", "blob-b").map(container =>
            new TransientStorageCredentials(
              s"https://dummyaccount.blob.core.windows.net/$container?sig=unused")))
        val paths = KustoReader.exportedPaths(
          storage,
          directories,
          "wasbs",
          (credential, directory) =>
            KustoReader.blobDirectoryExists(f.blobContainer(credential.blobContainer), directory))
        paths shouldBe Seq(
          s"wasbs://blob-b@dummyaccount.blob.core.windows.net/${sent(1).directory}",
          s"wasbs://blob-a@dummyaccount.blob.core.windows.net/${sent(2).directory}")
        val probes = f.blobRequests.asScala.toSeq
        probes.size shouldBe 4
        probes.map(_._2.get("prefix")).toSet shouldBe directories.map(Some(_)).toSet
        val successDirs = paths.map { path =>
          val uri = URI.create(path)
          local(uri.getUserInfo, uri.getPath.stripPrefix("/"))
        }
        val rows = spark.read.parquet(successDirs: _*).collect().map(_.getLong(0)).sorted.toSeq
        rows shouldBe Seq(10L, 20L)
        rows should not contain 999L
      }
    }
  }

  "Blob directory probing" should "ignore failed attempts, sibling prefixes, and empty containers" in {
    withFixture(Seq.empty) { f =>
      val directory = "read/part0/attempt2/"
      f.blobNames("blob-other") = Seq(
        "read/part0/attempt1/part.parquet",
        "read/part0/attempt2",
        "read/part0/attempt2_other/part.parquet",
        "read/part1/attempt2/part.parquet")
      f.blobNames("blob-empty") = Seq.empty

      KustoReader.blobDirectoryExists(f.blobContainer("blob-other"), directory) shouldBe false
      KustoReader.blobDirectoryExists(f.blobContainer("blob-empty"), directory) shouldBe false
      val probes = f.blobRequests.asScala.toSeq
      probes.size shouldBe 2
      all(probes.map(_._2.get("prefix"))) shouldBe Some(directory)
    }
  }

  it should "find nested attempt output after an empty page with a continuation token" in {
    withFixture(Seq.empty) { f =>
      val directory = "read/part0/attempt2/"
      f.blobNames("blob-paged") = Seq(directory + "nested/part.parquet")
      f.emptyFirstBlobPage("blob-paged") = true

      KustoReader.blobDirectoryExists(f.blobContainer("blob-paged"), directory) shouldBe true
      val probes = f.blobRequests.asScala.toSeq
      probes.map(_._2.get("marker")) shouldBe Seq(None, Some("0"))
      all(probes.map(_._2.get("prefix"))) shouldBe Some(directory)
    }
  }

  it should "stop listing once it finds a matching blob" in {
    withFixture(Seq.empty) { f =>
      val directory = "read/part0/attempt2/"
      f.blobNames("blob-many") = (1 to 3).map(i => s"${directory}part$i.parquet")

      KustoReader.blobDirectoryExists(f.blobContainer("blob-many"), directory) shouldBe true
      f.blobRequests.size() shouldBe 1
      f.blobRequests.peek()._2.get("prefix") shouldBe Some(directory)
    }
  }

  it should "propagate blob-listing authorization failures instead of treating them as empty output" in {
    withFixture(Seq.empty) { f =>
      f.deniedBlobContainers("blob-denied") = true

      val error = intercept[BlobStorageException] {
        KustoReader.blobDirectoryExists(f.blobContainer("blob-denied"), "read/part0/attempt2/")
      }

      error.getStatusCode shouldBe 403
      f.blobRequests.size() shouldBe 1
    }
  }

  "Local SDK export integration" should "retry polling HTTP 429 on the same operation without another export" in {
    withFixture(Seq(Outcome(pollStatuses = Seq(429, 200)))) { f =>
      val directories = f.run()
      val sent = f.exports.asScala.toSeq
      sent.size shouldBe 1
      directories shouldBe Seq(sent.head.directory)
      f.polls.asScala.toSeq shouldBe Seq(
        (sent.head.operationId, 429),
        (sent.head.operationId, 200))
      f.delays shouldBe empty
    }
  }

  it should "not replay an ambiguous HTTP 500 submission" in {
    withFixture(Seq(Outcome(submissionStatus = 500))) { f =>
      val error = intercept[DataServiceException](f.run())
      error.isPermanent shouldBe false
      error.getMessage should include("submission outcome unknown")
      f.exports.size() shouldBe 1
      f.polls shouldBe empty
      f.delays shouldBe empty
    }
  }
}

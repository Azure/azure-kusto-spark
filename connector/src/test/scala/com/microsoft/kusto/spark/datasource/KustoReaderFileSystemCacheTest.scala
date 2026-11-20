// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path => HadoopPath}
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration
import org.apache.hadoop.fs.azurebfs.services.{AuthType => AbfsAuthType}
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.MessageTypeParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.FileNotFoundException
import java.net.URI
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

// Uses Hadoop's real filesystem cache and Spark's Parquet reader with local file bytes.
class CredentialReadbackFileSystem extends KustoReadbackFileSystem {
  private var uri = URI.create("abfss:///")
  private var auth: AbfsAuthType = _
  private var credential: String = _
  @volatile var closed = false

  override def getUri: URI = Option(uri).getOrElse(URI.create("abfss:///"))

  override def initialize(name: URI, config: Configuration): Unit = {
    uri = new URI(name.getScheme, name.getAuthority, "/", null, null)
    super.initialize(name, config)
    val account = name.getHost
    val abfs = new AbfsConfiguration(config, account)
    auth = abfs.getAuthType(account)
    credential = auth match {
      case AbfsAuthType.SharedKey => abfs.getStorageAccountKey
      case AbfsAuthType.SAS =>
        abfs.getSASTokenProvider.getSASToken(account, name.getUserInfo, "/", "read")
      case _ => "ambient"
    }
    CredentialReadbackFileSystem.instances.add(this)
  }

  override def open(path: HadoopPath, bufferSize: Int): FSDataInputStream = {
    require(!closed, "An active read's filesystem was closed")
    CredentialReadbackFileSystem.reads.add((uri.toString, auth, credential))
    super.open(path, bufferSize)
  }

  override def close(): Unit = {
    closed = true
    super.close()
  }
}

object CredentialReadbackFileSystem {
  val instances = new ConcurrentLinkedQueue[CredentialReadbackFileSystem]()
  val reads = new ConcurrentLinkedQueue[(String, AbfsAuthType, String)]()
}

class KustoReaderFileSystemCacheTest extends AnyFlatSpec with Matchers {
  private def withReadback(test: (SparkSession, String, String, Configuration) => Unit): Unit = {
    val previousActive = SparkSession.getActiveSession
    val previousDefault = SparkSession.getDefaultSession
    val root = Files.createTempDirectory("kusto-fs-cache-")
    var ownedContext: Option[SparkContext] = None
    val account = "readback" + UUID.randomUUID().toString.replace("-", "")
    val host = s"$account.blob.core.windows.net"
    try {
      val existing = previousActive
        .filterNot(_.sparkContext.isStopped)
        .orElse(previousDefault.filterNot(_.sparkContext.isStopped))
      val spark = existing match {
        case Some(session) =>
          require(session.sparkContext.master.startsWith("local"))
          session.newSession()
        case None =>
          val context = new SparkContext(
            new SparkConf()
              .setMaster("local[1]")
              .setAppName("KustoReaderFileSystemCacheTest")
              .set("spark.ui.enabled", "false"))
          ownedContext = Some(context)
          SparkSession.builder().getOrCreate()
      }
      val schema =
        MessageTypeParser.parseMessageType("message readback { required int64 value; }")
      val writer = ExampleParquetWriter
        .builder(new LocalOutputFile(root.resolve("part.parquet")))
        .withType(schema)
        .build()
      try writer.write(new SimpleGroupFactory(schema).newGroup().append("value", 42L))
      finally writer.close()
      val path = new URI("abfss", s"container@$host", root.toUri.getPath, null, null).toString
      val conf = spark.sparkContext.hadoopConfiguration
      val settings = Map(
        "fs.azure.account.key.unrelated.blob.core.windows.net" -> "unrelated-secret",
        "fs.abfs.impl" -> classOf[CredentialReadbackFileSystem].getName,
        "fs.abfs.impl.disable.cache" -> "false",
        "fs.abfss.impl" -> classOf[CredentialReadbackFileSystem].getName,
        "fs.abfss.impl.disable.cache" -> "false",
        s"fs.azure.account.auth.type.$host" -> "SAS",
        s"fs.azure.account.hns.enabled.$host" -> "false",
        s"fs.azure.sas.fixed.token.$host" -> "sig=previous")
      val previous = settings.keys.map(key => key -> Option(conf.get(key))).toMap
      try {
        settings.foreach { case (key, value) => conf.set(key, value) }
        test(spark, account, path, conf)
      } finally {
        CredentialReadbackFileSystem.instances
          .iterator()
          .asScala
          .filter(_.getUri.getHost == host)
          .foreach { fs =>
            fs.close()
            CredentialReadbackFileSystem.instances.remove(fs)
          }
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
        val stream = Files.walk(root)
        val paths =
          try stream.iterator().asScala.toVector
          finally stream.close()
        paths.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))
      }
    }
  }

  private def storage(account: String, sas: String): TransientStorageParameters =
    new TransientStorageParameters(Array(
      new TransientStorageCredentials(s"https://$account.blob.core.windows.net/container?$sas")))

  private def assertDelegatesClosed(cached: CredentialReadbackFileSystem): Unit = {
    val delegates = CredentialReadbackFileSystem.instances
      .iterator()
      .asScala
      .toVector
      .filter(fs => fs.getUri.getHost == cached.getUri.getHost && (fs ne cached))
    delegates should not be empty
    all(delegates.map(_.closed)) shouldBe true
    cached.closed shouldBe false
  }

  private def readbackFileSystem(
      spark: SparkSession,
      path: String,
      credentials: TransientStorageParameters): FileSystem = {
    val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
    KustoReader
      .blobReadOptions(spark, credentials, URI.create(path).getScheme)
      .foreach { case (key, value) => conf.set(key, value) }
    FileSystem.get(URI.create(path), conf)
  }

  "Parquet readback" should "use current credentials for the same URI across SAS and SharedKey reads" in {
    withReadback { (spark, account, path, conf) =>
      val uri = URI.create(path)
      val cached = FileSystem.get(uri, conf).asInstanceOf[CredentialReadbackFileSystem]
      val key = "dGVzdC1rZXk="
      val configurations = Seq(
        (storage(account, "sig=first"), AbfsAuthType.SAS, "sig=first"),
        (
          new TransientStorageParameters(
            Array(new TransientStorageCredentials(account, key, "container"))),
          AbfsAuthType.SharedKey,
          key),
        (storage(account, "sig=last"), AbfsAuthType.SAS, "sig=last"))

      configurations.foreach { case (credentials, auth, secret) =>
        KustoReader
          .blobReadOptions(spark, credentials, "abfss")
          .keys
          .exists(_.contains("unrelated.blob.core.windows.net")) shouldBe false
        CredentialReadbackFileSystem.reads.clear()
        val rows = KustoReader.readParquet(spark, Seq(path), credentials, "abfss").collect()
        rows.map(_.getLong(0)).toSeq shouldBe Seq(42L)
        val reads = CredentialReadbackFileSystem.reads.iterator().asScala.toVector
        reads should not be empty
        all(reads.map { case (_, kind, value) => (kind, value) }) shouldBe ((auth, secret))
        FileSystem.get(uri, conf) should be theSameInstanceAs cached
        cached.closed shouldBe false
        conf.getBoolean("fs.abfss.impl.disable.cache", false) shouldBe false
        conf.get(s"fs.azure.sas.fixed.token.${uri.getHost}") shouldBe "sig=previous"
        spark.conf.getOption("spark.hadoop.fs.abfss.impl.disable.cache") shouldBe empty
        assertDelegatesClosed(cached)
      }
    }
  }

  it should "retain each lazy read's credentials after another read is planned" in {
    withReadback { (spark, account, path, conf) =>
      val cached =
        FileSystem.get(URI.create(path), conf).asInstanceOf[CredentialReadbackFileSystem]
      val first =
        KustoReader.readParquet(spark, Seq(path), storage(account, "sig=first"), "abfss").rdd
      val second =
        KustoReader.readParquet(spark, Seq(path), storage(account, "sig=second"), "abfss").rdd
      conf.set(
        s"fs.azure.sas.fixed.token.${URI.create(path).getHost}",
        "sig=changed-after-planning")
      Seq(first -> "sig=first", second -> "sig=second").foreach { case (rows, token) =>
        CredentialReadbackFileSystem.reads.clear()
        rows.collect().map(_.getLong(0)).toSeq shouldBe Seq(42L)
        val reads = CredentialReadbackFileSystem.reads.iterator().asScala.toVector
        reads should not be empty
        all(reads.map(_._3)) shouldBe token
        assertDelegatesClosed(cached)
      }
    }
  }

  it should "keep overlapping streams isolated and close only their own clients" in {
    withReadback { (spark, account, path, conf) =>
      val cached =
        FileSystem.get(URI.create(path), conf).asInstanceOf[CredentialReadbackFileSystem]
      val firstFs = readbackFileSystem(spark, path, storage(account, "sig=first"))
      val secondFs = readbackFileSystem(spark, path, storage(account, "sig=second"))
      val file = new HadoopPath(new HadoopPath(path), "part.parquet")
      CredentialReadbackFileSystem.reads.clear()
      try {
        val first = firstFs.open(file)
        try {
          val second = secondFs.open(file)
          try {
            first.read() shouldBe 'P'.toInt
            first.close()
            second.read() shouldBe 'P'.toInt
            second.seek(0)
            val magic = new Array[Byte](4)
            second.readFully(0, magic)
            new String(magic, java.nio.charset.StandardCharsets.US_ASCII) shouldBe "PAR1"
            CredentialReadbackFileSystem.reads.iterator().asScala.map(_._3).toSet shouldBe
              Set("sig=first", "sig=second")
            cached.closed shouldBe false
          } finally second.close()
        } finally first.close()
        assertDelegatesClosed(cached)
      } finally {
        firstFs.close()
        secondFs.close()
      }
    }
  }

  it should "close clients after missing-file metadata and open failures" in {
    withReadback { (spark, account, path, conf) =>
      val cached =
        FileSystem.get(URI.create(path), conf).asInstanceOf[CredentialReadbackFileSystem]
      val fs = readbackFileSystem(spark, path, storage(account, "sig=current"))
      val missing = new HadoopPath(new HadoopPath(path), "missing.parquet")
      try {
        intercept[FileNotFoundException](fs.getFileStatus(missing))
        assertDelegatesClosed(cached)
        intercept[FileNotFoundException](fs.open(missing))
        assertDelegatesClosed(cached)
      } finally fs.close()
    }
  }

  it should "isolate ABFS glob reads without changing the session implementation" in {
    withReadback { (spark, account, path, conf) =>
      val abfsPath = path.replaceFirst("^abfss:", "abfs:")
      val cached =
        FileSystem.get(URI.create(abfsPath), conf).asInstanceOf[CredentialReadbackFileSystem]
      CredentialReadbackFileSystem.reads.clear()
      KustoReader
        .readParquet(spark, Seq(abfsPath + "part*.parquet"), storage(account, "sig=abfs"), "abfs")
        .collect()
        .map(_.getLong(0))
        .toSeq shouldBe Seq(42L)
      all(
        CredentialReadbackFileSystem.reads
          .iterator()
          .asScala
          .toVector
          .map(_._3)) shouldBe "sig=abfs"
      conf.get("fs.abfs.impl") shouldBe classOf[CredentialReadbackFileSystem].getName
      assertDelegatesClosed(cached)
    }
  }

  it should "refresh blob impersonation probes without closing a cached client" in {
    withReadback { (spark, account, path, conf) =>
      val cached =
        FileSystem.get(URI.create(path), conf).asInstanceOf[CredentialReadbackFileSystem]
      val credential = new TransientStorageCredentials(
        s"https://$account.blob.core.windows.net/container;impersonate")
      conf.set(s"fs.azure.sas.fixed.token.${URI.create(path).getHost}", "sig=refreshed")
      KustoReader.dirExist(
        spark,
        credential,
        URI.create(path).getPath.stripPrefix("/"),
        "core.windows.net") shouldBe true
      assertDelegatesClosed(cached)
      val fresh =
        CredentialReadbackFileSystem.instances.iterator().asScala.toVector.filter(_ ne cached)
      all(
        fresh.map(
          _.getConf.get(
            s"fs.azure.sas.fixed.token.${URI.create(path).getHost}"))) shouldBe "sig=refreshed"
      FileSystem.get(URI.create(path), conf) should be theSameInstanceAs cached
    }
  }

  it should "preserve ambient cached filesystems for OneLake reads" in {
    withReadback { (spark, _, path, conf) =>
      val cached =
        FileSystem.get(URI.create(path), conf).asInstanceOf[CredentialReadbackFileSystem]
      val oneLake = new TransientStorageCredentials()
      oneLake.oneLakeUrl =
        "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse.Lakehouse/Files/export"
      oneLake.validate()
      val ambient = new TransientStorageParameters(Array(oneLake))
      CredentialReadbackFileSystem.reads.clear()
      KustoReader
        .readParquet(spark, Seq(path), ambient, "abfss")
        .collect()
        .map(_.getLong(0))
        .toSeq shouldBe Seq(42L)
      all(
        CredentialReadbackFileSystem.reads
          .iterator()
          .asScala
          .toVector
          .map(_._3)) shouldBe "sig=previous"
      FileSystem.get(URI.create(path), conf) should be theSameInstanceAs cached
      cached.closed shouldBe false
    }
  }

  "Cached exports" should "only be reused with matching storage credentials and protocol" in {
    val initial = storage("account", "sig=first")
    val cached = DistributedReadModeTransientCacheEntry(
      Seq("abfss://container@account.blob.core.windows.net/export"),
      initial,
      "abfss")
    KustoReader.cachedExportMatches(
      cached,
      storage("account", "sig=first"),
      "abfss") shouldBe true
    KustoReader.cachedExportMatches(
      cached,
      storage("account", "sig=second"),
      "abfss") shouldBe false
    KustoReader.cachedExportMatches(
      cached,
      storage("otheraccount", "sig=first"),
      "abfss") shouldBe false
    KustoReader.cachedExportMatches(cached, initial, "abfs") shouldBe false
    val key = new TransientStorageParameters(
      Array(new TransientStorageCredentials("account", "dGVzdC1rZXk=", "container")))
    KustoReader.cachedExportMatches(cached, key, "abfss") shouldBe false
    val keyCache = cached.copy(storage = key)
    KustoReader.cachedExportMatches(keyCache, key, "abfss") shouldBe true
    val rotated = new TransientStorageParameters(
      Array(new TransientStorageCredentials("account", "cm90YXRlZC1rZXk=", "container")))
    KustoReader.cachedExportMatches(keyCache, rotated, "abfss") shouldBe false
  }
}

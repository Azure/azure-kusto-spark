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
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{FileNotFoundException, IOException}
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
  var taskAttempt: Option[Long] = None
  var openStreams = 0

  override def getUri: URI = Option(uri).getOrElse(URI.create("abfss:///"))

  override def initialize(name: URI, config: Configuration): Unit = {
    uri = new URI(name.getScheme, name.getAuthority, "/", null, null)
    super.initialize(name, config)
    taskAttempt = Option(TaskContext.get()).map(_.taskAttemptId())
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
    val stream = super.open(path, bufferSize)
    openStreams += 1
    new FSDataInputStream(stream) {
      private var streamClosed = false
      override def close(): Unit = synchronized {
        if (!streamClosed) {
          streamClosed = true
          try super.close()
          finally openStreams -= 1
        }
      }
    }
  }

  override def close(): Unit = {
    closed = true
    super.close()
    if (getConf != null && getConf.getBoolean("kusto.test.fail.close", false)) {
      throw new IOException("test close failure")
    }
  }
}

class ConflictingSasTokenProvider
    extends org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider {
  override def initialize(configuration: Configuration, accountName: String): Unit = ()
  override def getSASToken(account: String, fs: String, path: String, op: String): String =
    "sig=provider"
}

class ConflictingKeyProvider extends org.apache.hadoop.fs.azurebfs.services.KeyProvider {
  override def getStorageAccountKey(accountName: String, conf: Configuration): String =
    "cHJvdmlkZXI="
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

  it should "override ambient providers for explicit credentials and keep them for impersonation" in {
    withReadback { (spark, account, path, conf) =>
      val host = URI.create(path).getHost
      val providers = Map(
        s"fs.azure.sas.token.provider.type.$host" -> classOf[ConflictingSasTokenProvider].getName,
        s"fs.azure.account.keyprovider.$host" -> classOf[ConflictingKeyProvider].getName)
      providers.foreach { case (key, value) => conf.set(key, value) }
      try {
        val key = "dGVzdC1rZXk="
        val keyStorage = new TransientStorageParameters(
          Array(new TransientStorageCredentials(account, key, "container")))
        val impersonation = new TransientStorageParameters(
          Array(new TransientStorageCredentials(s"https://$host/container;impersonate")))
        Seq(
          (storage(account, "sig=explicit"), AbfsAuthType.SAS, "sig=explicit"),
          (keyStorage, AbfsAuthType.SharedKey, key),
          (impersonation, AbfsAuthType.SAS, "sig=provider")).foreach {
          case (credentials, auth, secret) =>
            CredentialReadbackFileSystem.reads.clear()
            KustoReader
              .readParquet(spark, Seq(path), credentials, "abfss")
              .collect()
              .map(_.getLong(0))
              .toSeq shouldBe Seq(42L)
            val reads = CredentialReadbackFileSystem.reads.iterator().asScala.toVector
            reads should not be empty
            all(reads.map { case (_, kind, value) => (kind, value) }) shouldBe ((auth, secret))
        }
      } finally providers.keys.foreach(conf.unset)
    }
  }

  private def impersonationOf(host: String): TransientStorageParameters =
    new TransientStorageParameters(
      Array(new TransientStorageCredentials(s"https://$host/container;impersonate")))

  private def installExplicit(
      spark: SparkSession,
      conf: Configuration,
      credentials: TransientStorageParameters): Unit =
    KustoReader.setHadoopAuth(
      credentials,
      "abfss",
      conf,
      spark.conf,
      java.time.Instant.now(),
      useAbfs = true)

  private def readsOf(
      spark: SparkSession,
      path: String,
      credentials: TransientStorageParameters): Vector[(AbfsAuthType, String)] = {
    CredentialReadbackFileSystem.reads.clear()
    KustoReader
      .readParquet(spark, Seq(path), credentials, "abfss")
      .collect()
      .map(_.getLong(0))
      .toSeq shouldBe Seq(42L)
    val reads = CredentialReadbackFileSystem.reads.iterator().asScala.toVector
    reads should not be empty
    reads.map { case (_, kind, value) => (kind, value) }
  }

  it should "read impersonation with ambient settings after explicit credentials were installed" in {
    withReadback { (spark, account, path, conf) =>
      val host = URI.create(path).getHost
      installExplicit(
        spark,
        conf,
        new TransientStorageParameters(
          Array(new TransientStorageCredentials(account, "dGVzdC1rZXk=", "container"))))
      // Session-wide settings keep the explicit credential for native readers.
      conf.get(s"fs.azure.account.auth.type.$host") shouldBe "SharedKey"
      all(readsOf(spark, path, impersonationOf(host))) shouldBe ((
        AbfsAuthType.SAS,
        "sig=previous"))
      installExplicit(spark, conf, storage(account, "sig=explicit"))
      all(readsOf(spark, path, impersonationOf(host))) shouldBe ((
        AbfsAuthType.SAS,
        "sig=previous"))
    }
  }

  it should "keep a planned impersonation read on ambient settings when explicit ones change" in {
    withReadback { (spark, account, path, conf) =>
      val host = URI.create(path).getHost
      val planned = KustoReader.readParquet(spark, Seq(path), impersonationOf(host), "abfss").rdd
      installExplicit(spark, conf, storage(account, "sig=concurrent"))
      CredentialReadbackFileSystem.reads.clear()
      planned.collect().map(_.getLong(0)).toSeq shouldBe Seq(42L)
      all(CredentialReadbackFileSystem.reads.iterator().asScala.toVector.map(_._3)) shouldBe
        "sig=previous"
    }
  }

  it should "use settings changed after the connector installed explicit credentials" in {
    withReadback { (spark, account, path, conf) =>
      val host = URI.create(path).getHost
      installExplicit(spark, conf, storage(account, "sig=explicit"))
      conf.set(s"fs.azure.sas.fixed.token.$host", "sig=user")
      all(readsOf(spark, path, impersonationOf(host))) shouldBe ((AbfsAuthType.SAS, "sig=user"))
    }
  }

  it should "drop installed credentials that had no ambient value" in {
    withReadback { (spark, account, path, conf) =>
      val host = URI.create(path).getHost
      val keys = Seq(s"fs.azure.account.auth.type.$host", s"fs.azure.sas.fixed.token.$host")
      conf.set(keys.head, "Custom")
      conf.unset(keys(1))
      try {
        installExplicit(
          spark,
          conf,
          new TransientStorageParameters(
            Array(new TransientStorageCredentials(account, "dGVzdC1rZXk=", "container"))))
        conf.get(s"fs.azure.account.key.$host") shouldBe "dGVzdC1rZXk="
        KustoReader
          .blobReadOptions(spark, impersonationOf(host), "abfss")(
            KustoBlobReadFileSystem.UnsetKeys)
          .split(',') should contain(s"fs.azure.account.key.$host")
        CredentialReadbackFileSystem.instances.clear()
        all(readsOf(spark, path, impersonationOf(host))) shouldBe ((
          AbfsAuthType.Custom,
          "ambient"))
        val delegates = CredentialReadbackFileSystem.instances.iterator().asScala.toVector
        delegates should not be empty
        all(delegates.map(_.getConf.get(s"fs.azure.account.key.$host"))) shouldBe null
      } finally
        spark.conf.getAll.keys
          .filter(_.endsWith(s".$host"))
          .foreach(spark.conf.unset)
    }
  }

  it should "track explicit credentials per Spark session" in {
    withReadback { (spark, account, path, conf) =>
      val host = URI.create(path).getHost
      val other = spark.newSession()
      other.conf.set(s"spark.hadoop.fs.azure.sas.fixed.token.$host", "sig=other-ambient")
      installExplicit(spark, conf, storage(account, "sig=first"))
      installExplicit(other, conf, storage(account, "sig=second"))
      all(readsOf(spark, path, impersonationOf(host))) shouldBe ((
        AbfsAuthType.SAS,
        "sig=previous"))
      all(readsOf(other, path, impersonationOf(host))) shouldBe
        ((AbfsAuthType.SAS, "sig=other-ambient"))
      all(readsOf(spark, path, storage(account, "sig=explicit"))) shouldBe
        ((AbfsAuthType.SAS, "sig=explicit"))
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

  it should "reuse one delegate for driver metadata across multiple files" in {
    withReadback { (spark, account, path, _) =>
      val root = new HadoopPath(path).toUri.getPath
      val file = new java.io.File(root, "part.parquet").toPath
      (1 to 4).foreach(index => Files.copy(file, file.resolveSibling(s"part-$index.parquet")))
      KustoReader
        .readParquet(spark, Seq(path), storage(account, "sig=metadata"), "abfss")
        .collect()
        .map(_.getLong(0))
        .toSeq shouldBe Seq.fill(5)(42L)
      val instances = CredentialReadbackFileSystem.instances
        .iterator()
        .asScala
        .toVector
        .filter(_.getUri.getHost == URI.create(path).getHost)
      instances.count(_.taskAttempt.isEmpty) shouldBe 1
      all(instances.map(_.closed)) shouldBe true
    }
  }

  it should "reuse one delegate within a task and release it at task completion" in {
    withReadback { (spark, account, path, conf) =>
      val options = KustoReader.blobReadOptions(spark, storage(account, "sig=task"), "abfss")
      val taskConf = new Configuration(conf)
      options.foreach { case (key, value) => taskConf.set(key, value) }
      val serialized = new SerializableConfiguration(taskConf)
      val file = new HadoopPath(new HadoopPath(path), "part.parquet").toString
      val magic = spark.sparkContext
        .parallelize(Seq(file), 1)
        .map { name =>
          val filePath = new HadoopPath(name)
          val first = filePath.getFileSystem(serialized.value)
          first.getFileStatus(filePath)
          first.listStatus(filePath.getParent)
          val second = filePath.getFileSystem(serialized.value)
          val input = second.open(filePath)
          try input.read()
          finally {
            input.close()
            first.close()
            second.close()
          }
        }
        .collect()
      magic.toSeq shouldBe Seq('P'.toInt)
      val instances = CredentialReadbackFileSystem.instances
        .iterator()
        .asScala
        .toVector
        .filter(_.getUri.getHost == URI.create(path).getHost)
      instances.size shouldBe 1
      all(instances.map(_.closed)) shouldBe true
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

  it should "retain an export-time snapshot after caller storage mutation" in {
    val source = storage("account", "sig=original")
    val snapshot = KustoReader.snapshotStorage(source)
    val cached = DistributedReadModeTransientCacheEntry(Seq("export"), snapshot, "abfss")
    source.storageCredentials.head.sasKey = "sig=changed"
    source.storageCredentials.head.blobContainer = "changed"
    source.endpointSuffix = "core.chinacloudapi.cn"
    source.storageCredentials(0) = new TransientStorageCredentials("other", "dGVzdA==", "other")
    snapshot.storageCredentials.head.sasKey shouldBe "?sig=original"
    snapshot.storageCredentials.head.blobContainer shouldBe "container"
    snapshot.endpointSuffix shouldBe "core.windows.net"
    KustoReader.cachedExportMatches(
      cached,
      storage("account", "sig=original"),
      "abfss") shouldBe true
    KustoReader.cachedExportMatches(cached, source, "abfss") shouldBe false
  }

  "Read lifecycle cleanup" should "close abandoned streams on driver failure" in {
    withReadback { (spark, account, path, conf) =>
      val options = KustoReader.blobReadOptions(spark, storage(account, "sig=failed"), "abfss")
      val local = new Configuration(conf)
      options.foreach { case (key, value) => local.set(key, value) }
      val expected = new IOException("test planning failure")
      val error = intercept[IOException] {
        KustoBlobReadFileSystem.withReadScope(options(KustoBlobReadFileSystem.ReadId)) {
          val file = new HadoopPath(new HadoopPath(path), "part.parquet")
          file.getFileSystem(local).open(file)
          throw expected
        }
      }
      error should be theSameInstanceAs expected
      val created = CredentialReadbackFileSystem.instances
        .iterator()
        .asScala
        .toVector
        .filter(_.getUri.getHost == URI.create(path).getHost)
      created.size shouldBe 1
      all(created.map(_.closed)) shouldBe true
      all(created.map(_.openStreams)) shouldBe 0
    }
  }

  it should "preserve open failures when delegate cleanup also fails" in {
    withReadback { (spark, account, path, conf) =>
      val options = KustoReader.blobReadOptions(spark, storage(account, "sig=failed"), "abfss")
      val local = new Configuration(conf)
      options.foreach { case (key, value) => local.set(key, value) }
      local.setBoolean("kusto.test.fail.close", true)
      val missing = new HadoopPath(new HadoopPath(path), "missing.parquet")
      val fs = missing.getFileSystem(local)
      try {
        val error = intercept[FileNotFoundException](fs.open(missing))
        error.getSuppressed.map(_.getMessage).toSeq shouldBe Seq("test close failure")
      } finally {
        fs.close()
        CredentialReadbackFileSystem.instances
          .iterator()
          .asScala
          .filter(_.getUri.getHost == URI.create(path).getHost)
          .foreach(_.getConf.unset("kusto.test.fail.close"))
      }
    }
  }

  it should "close abandoned streams and delegates after a failed Spark task" in {
    withReadback { (spark, account, path, conf) =>
      val local = new Configuration(conf)
      KustoReader
        .blobReadOptions(spark, storage(account, "sig=task-failure"), "abfss")
        .foreach { case (key, value) => local.set(key, value) }
      val serialized = new SerializableConfiguration(local)
      val file = new HadoopPath(new HadoopPath(path), "part.parquet").toString
      intercept[org.apache.spark.SparkException] {
        spark.sparkContext
          .parallelize(Seq(file), 1)
          .map[Int] { name =>
            val filePath = new HadoopPath(name)
            filePath.getFileSystem(serialized.value).open(filePath)
            throw new IOException("test task failure")
          }
          .collect()
      }
      val created = CredentialReadbackFileSystem.instances
        .iterator()
        .asScala
        .toVector
        .filter(_.getUri.getHost == URI.create(path).getHost)
      created should not be empty
      all(created.map(_.closed)) shouldBe true
      all(created.map(_.openStreams)) shouldBe 0
    }
  }

  it should "allow first filesystem access from a task completion listener" in {
    withReadback { (spark, account, path, conf) =>
      val local = new Configuration(conf)
      KustoReader
        .blobReadOptions(spark, storage(account, "sig=completion"), "abfss")
        .foreach { case (key, value) => local.set(key, value) }
      val serialized = new SerializableConfiguration(local)
      val file = new HadoopPath(new HadoopPath(path), "part.parquet").toString
      spark.sparkContext
        .parallelize(Seq(file), 1)
        .map { name =>
          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            val filePath = new HadoopPath(name)
            val fs = filePath.getFileSystem(serialized.value)
            try {
              val input = fs.open(filePath)
              try input.read()
              finally input.close()
            } finally fs.close()
          }
          1
        }
        .collect()
        .toSeq shouldBe Seq(1)
      val created = CredentialReadbackFileSystem.instances
        .iterator()
        .asScala
        .toVector
        .filter(_.getUri.getHost == URI.create(path).getHost)
      created should not be empty
      all(created.map(_.closed)) shouldBe true
      all(created.map(_.openStreams)) shouldBe 0
    }
  }
}

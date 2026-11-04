// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

import com.microsoft.azure.kusto.ingest.v2.models.{
  ConfigurationResponse,
  ContainerInfo,
  ContainerSettings,
  IngestionSettings,
  Status
}
import com.microsoft.kusto.spark.authentication.KustoAccessTokenAuthentication
import com.microsoft.kusto.spark.common.{KustoCoordinates, KustoDebugOptions}
import com.microsoft.kusto.spark.datasink._
import com.microsoft.kusto.spark.utils.{KustoCustomDebugWriteOptions, KustoDataSourceUtils}
import org.apache.hadoop.conf.Configuration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import java.util.Base64
import scala.collection.JavaConverters._

class IngestV2UnitTests extends AnyFlatSpec with Matchers {

  private val sas = "sv=2024&sig=secret"
  private val storage = s"https://acct.blob.core.windows.net/c1?$sas"
  private val lake =
    "https://ws1.z6.dfs.fabric.microsoft.com/11111111-2222-3333-4444-555555555555/item/Files/ingest"
  private val workspaceLake =
    "https://87e63f193aeb40a99407b9cdb8315ea9.z87.dfs.fabric.microsoft.com/87e63f19-3aeb-40a9-9407-b9cdb8315ea9/item/Ingestions"
  private val coordinates = KustoCoordinates(
    "https://trd-abc.z6.kusto.fabric.microsoft.com",
    "trd-abc",
    "db",
    Some("table"))

  private def config(
      ingestionMethod: Option[String] = None,
      uploadMethod: Option[String] = None,
      blobs: Seq[String] = Seq(storage),
      lakes: Seq[String] = Seq.empty) =
    IngestionConfig(ingestionMethod, uploadMethod, 70, 1024L, blobs, lakes)

  private def options(
      writeMode: WriteMode.Value = WriteMode.Transactional,
      enableIngestV2: Boolean = true,
      debug: KustoCustomDebugWriteOptions = KustoCustomDebugWriteOptions(),
      props: Option[SparkIngestionProperties] = None,
      format: IngestionFormat.Value = IngestionFormat.CSV,
      adjustSchema: SchemaAdjustmentMode.Value = SchemaAdjustmentMode.NoAdjustment,
      userStorage: Option[Array[IngestionStorageParameters]] = None,
      tokenProvider: Option[StorageTokenProvider] = None) =
    WriteOptions(
      writeMode = writeMode,
      kustoCustomDebugWriteOptions = debug,
      enableIngestV2 = enableIngestV2,
      maybeSparkIngestionProperties = props,
      ingestionFormat = format,
      adjustSchema = adjustSchema,
      maybeIngestionBlobStorage = userStorage,
      storageTokenProvider = tokenProvider)

  private val auth = KustoAccessTokenAuthentication("token")
  private val mapping =
    """[{"columnName":"Id","columnType":"int","properties":{"Ordinal":"0"}},""" +
      """{"columnName":"Name","columnType":"string","properties":{"Ordinal":"1"}}]"""

  "enableIngestV2" should "be off unless the option is set" in {
    val base = Map(
      KustoSinkOptions.KUSTO_CLUSTER -> "https://c.kusto.windows.net",
      KustoSinkOptions.KUSTO_DATABASE -> "db",
      KustoSinkOptions.KUSTO_TABLE -> "t",
      KustoSinkOptions.KUSTO_ACCESS_TOKEN -> "token")
    KustoDataSourceUtils.parseSinkParameters(base).writeOptions.enableIngestV2 shouldBe false
    KustoDataSourceUtils
      .parseSinkParameters(base + (KustoDebugOptions.KUSTO_ENABLE_INGEST_V2 -> "true"))
      .writeOptions
      .enableIngestV2 shouldBe true
  }

  "KustoWriter.planIngestV2" should "never use ingest-v2 when off or streaming, without calling the service" in {
    // An unreachable DM: if the configuration were requested, the call would fail or hang.
    val offline = coordinates.copy(ingestionUrl = Some("https://ingest-unreachable.invalid"))
    KustoWriter.planIngestV2(offline, auth, options(enableIngestV2 = false)) shouldBe None
    KustoWriter.planIngestV2(
      offline,
      auth,
      options(writeMode = WriteMode.KustoStreaming)) shouldBe None
  }

  "IngestionConfig" should "prefer ingest-v2 only for Rest ingestion or Lake upload, ignoring case" in {
    config(ingestionMethod = Some("Rest")).isV2Preferred shouldBe true
    config(ingestionMethod = Some(" rest ")).isV2Preferred shouldBe true
    config(uploadMethod = Some("LAKE")).isV2Preferred shouldBe true
    config(
      ingestionMethod = Some("Queue"),
      uploadMethod = Some("Storage")).isV2Preferred shouldBe false
    config().isV2Preferred shouldBe false
  }

  it should "select upload targets like the SDK" in {
    config(lakes = Seq(lake)).selectUploadTargets() shouldBe UploadTargets(
      Seq(storage),
      isLake = false)
    config(uploadMethod = Some("lake"), lakes = Seq(lake)).selectUploadTargets() shouldBe
      UploadTargets(Seq(lake), isLake = true)
    config(blobs = Seq.empty, lakes = Seq(lake)).selectUploadTargets().isLake shouldBe true
    config(uploadMethod = Some("Lake")).selectUploadTargets().isLake shouldBe false
    an[IllegalStateException] should be thrownBy config(blobs = Seq.empty).selectUploadTargets()
  }

  it should "never print container URLs" in {
    val text = config(lakes = Seq(lake)).toString + config().selectUploadTargets().toString
    text should not include "secret"
    text should not include "acct"
  }

  it should "read the service response and replace invalid limits with defaults" in {
    val response = new ConfigurationResponse(
      new ContainerSettings(
        List(new ContainerInfo(storage), new ContainerInfo(" ")).asJava,
        List(new ContainerInfo(lake)).asJava,
        "01:00:00",
        "Lake"),
      new IngestionSettings(0L, -1L, "Rest"))
    val parsed = IngestionConfig.fromConfigurationResponse(response).get
    parsed.blobPaths shouldBe Seq(storage)
    parsed.oneLakePaths shouldBe Seq(lake)
    parsed.maxBlobsPerBatch shouldBe IngestionConfig.DefaultMaxBlobsPerBatch
    parsed.maxDataSizeBytes shouldBe IngestionConfig.DefaultMaxDataSizeBytes
    parsed.prefersLakeUpload shouldBe true
    IngestionConfig.fromConfigurationResponse(null) shouldBe None
  }

  it should "never put more blobs in one request than the service accepts" in {
    def parsed(maxBlobs: Long) =
      IngestionConfig
        .fromConfigurationResponse(
          new ConfigurationResponse(
            new ContainerSettings(List(new ContainerInfo(storage)).asJava, null, null, null),
            new IngestionSettings(maxBlobs, 1L, null)))
        .get
        .maxBlobsPerBatch
    parsed(100) shouldBe IngestionConfig.MaxBlobsPerRequest
    parsed(5) shouldBe 5
  }

  "UploadTargets.pathFor" should "spread partitions, including negative ids" in {
    val targets = UploadTargets(Seq("a", "b"), isLake = false)
    Seq(0, 1, 2, -1).map(targets.pathFor) shouldBe Seq("a", "b", "a", "b")
  }

  "IngestV2ConfigurationProvider" should "parse .NET time spans and bound the cache time" in {
    import IngestV2ConfigurationProvider._
    parseTimeSpan("00:30:00") shouldBe Some(Duration.ofMinutes(30))
    parseTimeSpan("1.02:00:00.5") shouldBe Some(Duration.ofHours(26))
    parseTimeSpan("PT1H") shouldBe None
    parseTimeSpan(null) shouldBe None
    cacheTtl(Some(Duration.ofSeconds(5))) shouldBe Duration.ofSeconds(5)
    cacheTtl(Some(Duration.ZERO)) shouldBe MaxCacheTtl
    cacheTtl(Some(Duration.ofSeconds(-1))) shouldBe MaxCacheTtl
    cacheTtl(Some(Duration.ofMinutes(10))) shouldBe Duration.ofMinutes(10)
    cacheTtl(Some(Duration.ofDays(1))) shouldBe MaxCacheTtl
    cacheTtl(None) shouldBe MaxCacheTtl
  }

  "AccessTokens" should "use the JWT expiry and treat other tokens as expiring now" in {
    val exp = 4102444800L // 2100-01-01
    def part(s: String) =
      Base64.getUrlEncoder.withoutPadding.encodeToString(s.getBytes(StandardCharsets.UTF_8))
    val jwt = s"${part("""{"alg":"none"}""")}.${part(s"""{"exp":$exp}""")}.sig"
    AccessTokens.fromRawToken(jwt).getExpiresAt.toEpochSecond shouldBe exp
    AccessTokens.expiryOf("not-a-jwt") shouldBe None
    AccessTokens
      .fromRawToken("not-a-jwt")
      .getExpiresAt
      .isAfter(OffsetDateTime.now().plusMinutes(1)) shouldBe false
  }

  "KustoResourceCredential" should "use the endpoint audience like the .NET Kusto.Ingest.V2 client" in {
    import com.azure.core.credential.{AccessToken, TokenCredential, TokenRequestContext}
    import com.microsoft.azure.kusto.data.auth.CloudInfo
    import com.microsoft.azure.kusto.data.exceptions.{
      DataClientException,
      DataServiceException,
      ExceptionUtils,
      KustoClientInvalidConnectionStringException
    }
    import reactor.core.publisher.Mono

    def endpoint(name: String, metadata: Mono[CloudInfo]): String = {
      val dm = s"https://ingest-$name.kusto.windows.net"
      CloudInfo.manuallyAddToCache(dm, metadata)
      dm
    }
    def scopesSeen(dm: String, rescope: Boolean = true): Set[String] = {
      val seen = new java.util.concurrent.CopyOnWriteArrayList[String]()
      val inner = new TokenCredential {
        override def getToken(r: TokenRequestContext) =
          Mono.fromCallable(() => getTokenSync(r))
        override def getTokenSync(r: TokenRequestContext) = {
          seen.addAll(r.getScopes)
          new AccessToken("t", OffsetDateTime.now().plusHours(1))
        }
      }
      val credential = new KustoResourceCredential(inner, dm, rescope)
      val sdkRequest = new TokenRequestContext().addScopes(s"$dm/.default")
      credential.getTokenSync(sdkRequest)
      credential.getToken(sdkRequest).block()
      seen.asScala.toSet
    }
    def cloud(resource: String, mfa: Boolean = false) =
      Mono.just(
        new CloudInfo(
          mfa,
          CloudInfo.DEFAULT_PUBLIC_LOGIN_URL,
          "app",
          "redirect",
          resource,
          "fpa"))

    // Metadata resource kept as is (no ".kustomfa." rewrite, no slash trimming), like .NET.
    scopesSeen(
      endpoint(
        "aud-meta",
        cloud("https://kusto.example.kusto.windows.net/", mfa = true))) shouldBe
      Set("https://kusto.example.kusto.windows.net//.default")
    // Empty resource, or an error status from the metadata endpoint: public cloud default.
    val defaultScope = Set("https://kusto.kusto.windows.net/.default")
    scopesSeen(endpoint("aud-empty", cloud(""))) shouldBe defaultScope
    scopesSeen(
      endpoint(
        "aud-status",
        Mono.error(
          new DataServiceException(
            "x",
            "Error in metadata endpoint, got code: 500",
            true)))) shouldBe defaultScope
    // A 429 that persists after retries, as CloudInfo reports it.
    val throttled = ExceptionUtils.unwrapCloudInfoException(
      "x",
      new DataServiceException(
        "x",
        "Error in metadata endpoint, got code: 429\nWith error: ",
        false))
    throttled shouldBe a[DataClientException]
    scopesSeen(endpoint("aud-429", Mono.error(throttled))) shouldBe defaultScope
    // Unreachable metadata endpoint: fail, like .NET.
    val unreachable = endpoint(
      "aud-down",
      Mono.error(
        new DataServiceException("x", "IOException", new java.io.IOException("down"), false)))
    a[DataServiceException] should be thrownBy scopesSeen(unreachable)
    val down = ExceptionUtils.unwrapCloudInfoException("x", new java.net.ConnectException("down"))
    an[Exception] should be thrownBy scopesSeen(endpoint("aud-down2", Mono.error(down)))
    // User tokens keep the requested scope but still need a trusted endpoint.
    val requested = endpoint("aud-raw", cloud("https://kusto.kusto.windows.net"))
    scopesSeen(requested, rescope = false) shouldBe Set(s"$requested/.default")
    val untrusted = "https://ingest-aud.not-kusto.example.com"
    CloudInfo.manuallyAddToCache(untrusted, cloud("https://kusto.kusto.windows.net"))
    a[KustoClientInvalidConnectionStringException] should be thrownBy scopesSeen(
      untrusted,
      rescope = false)
  }

  "LakeFolderLocation" should "split the URL like the .NET LakeFolder" in {
    val location = LakeFolderLocation.parse(
      "https://ws1.z6.dfs.fabric.microsoft.com/workspace-id/item/Files/my%20folder")
    location.containerUrl shouldBe "https://ws1.z6.blob.fabric.microsoft.com/workspace-id"
    location.folder shouldBe "item/Files/my folder"
    location.rawFolder shouldBe "item/Files/my%20folder"
    location.blobPath("a.csv.gz") shouldBe "item/Files/my folder/a.csv.gz"
    LakeFolderLocation.parse("https://h.blob.x.com/ws").blobPath("a") shouldBe "a"
    location.blobUrl("a b+c.csv.gz") shouldBe
      "https://ws1.z6.blob.fabric.microsoft.com/workspace-id/item/Files/my%20folder/a%20b%2Bc.csv.gz"
    LakeFolderLocation
      .parse("https://h.blob.x.com/ws")
      .blobUrl("a") shouldBe "https://h.blob.x.com/ws/a"
    an[IllegalArgumentException] should be thrownBy LakeFolderLocation.parse(
      "http://h.dfs.x.com/ws/f")
    an[IllegalArgumentException] should be thrownBy LakeFolderLocation.parse(
      "https://h.dfs.x.com/")
  }

  "IngestV2ParquetWriter" should "stage under a unique folder and keep the shared Hadoop settings unchanged" in {
    val staging = IngestV2ParquetWriter.stagingLocation(lake, "req-1")
    staging.abfssDir should startWith(
      "abfss://11111111-2222-3333-4444-555555555555@ws1.z6.dfs.fabric.microsoft.com/item/Files/ingest/spark-ingest-v2/")
    staging.blobDirUrl should startWith(
      "https://ws1.z6.blob.fabric.microsoft.com/11111111-2222-3333-4444-555555555555/item/Files/ingest/spark-ingest-v2/")
    IngestV2ParquetWriter.stagingLocation(lake, "req-1") should not equal staging

    val key = IngestV2ParquetWriter.ValidEndpointsKey
    val conf = new Configuration(false)
    IngestV2ParquetWriter.hadoopOptions(conf, staging.abfssDir) shouldBe empty
    conf.set(key, "dfs.core.windows.net")
    IngestV2ParquetWriter.hadoopOptions(conf, staging.abfssDir) shouldBe
      Map(key -> "dfs.core.windows.net,ws1.z6.dfs.fabric.microsoft.com")
    conf.get(key) shouldBe "dfs.core.windows.net"
    conf.set(key, "WS1.z6.dfs.fabric.microsoft.com")
    IngestV2ParquetWriter.hadoopOptions(conf, staging.abfssDir) shouldBe empty
  }

  "IngestV2RequestProperties" should "convert the connector's CSV mapping and reject anything else" in {
    val columns = IngestV2RequestProperties.parseCsvMapping(mapping).get.asScala
    columns.map(_.getColumnName) shouldBe Seq("Id", "Name")
    IngestV2RequestProperties.parseCsvMapping(
      """[{"Name":"Id","DataType":"int"}]""") shouldBe None
    IngestV2RequestProperties.parseCsvMapping(
      """[{"columnName":"Id","columnType":"int","extra":1}]""") shouldBe None
    IngestV2RequestProperties.parseCsvMapping("[]") shouldBe None
    IngestV2RequestProperties.parseCsvMapping("not json") shouldBe None
  }

  it should "report settings that ingest-v2 cannot honour" in {
    def reason(p: SparkIngestionProperties, f: IngestionFormat.Value = IngestionFormat.CSV) =
      IngestV2RequestProperties.unsupportedReason(Some(p), f)
    reason(new SparkIngestionProperties(csvMapping = mapping)) shouldBe None
    reason(
      new SparkIngestionProperties(
        csvMapping = mapping,
        csvMappingNameReference = "m")) should not be empty
    reason(
      new SparkIngestionProperties(csvMappingNameReference = "m"),
      IngestionFormat.Parquet) should not be empty
    reason(new SparkIngestionProperties(csvMapping = """{"x":1}""")) should not be empty
    IngestV2RequestProperties.unsupportedReason(None, IngestionFormat.Parquet) shouldBe None
  }

  it should "carry the user's ingestion properties" in {
    val created = Instant.parse("2021-07-01T00:00:00Z")
    val props = new SparkIngestionProperties(
      dropByTags = List("d").asJava,
      ingestByTags = List("i").asJava,
      additionalTags = List("a").asJava,
      ingestIfNotExists = List("i").asJava,
      creationTime = created,
      csvMappingNameReference = "map")
    val built = IngestV2RequestProperties.build(Some(props), IngestionFormat.CSV)
    built.getEnableTracking shouldBe true
    Option(built.getSkipBatching).exists(_.booleanValue()) shouldBe false
    built.getTags.asScala should contain allOf ("drop-by:d", "ingest-by:i", "a")
    built.getIngestIfNotExists.asScala shouldBe Seq("i")
    built.getCreationTime shouldBe OffsetDateTime.ofInstant(created, ZoneOffset.UTC)
    built.getIngestionMappingReference shouldBe "map"
    IngestV2RequestProperties
      .build(None, IngestionFormat.CSV, forceSkipBatching = true)
      .getSkipBatching shouldBe true
    IngestV2RequestProperties
      .build(None, IngestionFormat.CSV, enableTracking = false)
      .getEnableTracking shouldBe false
  }

  "IngestV2StatusTracker" should "succeed only when every blob succeeded" in {
    import IngestV2StatusTracker._
    def status(succeeded: Long, failed: Long, inProgress: Long, canceled: Long) =
      new Status(succeeded, failed, inProgress, canceled)
    val done = Counts.of(status(2, 0, 0, 0))
    isDone(done, 2) shouldBe true
    isSuccess(done, 2) shouldBe true
    isSuccess(done, 3) shouldBe false
    val failed = Counts.of(status(1, 1, 0, 0))
    isDone(failed, 2) shouldBe true
    isSuccess(failed, 2) shouldBe false
    isDone(Counts.of(status(1, 0, 1, 0)), 2) shouldBe false
    isSuccess(Counts.of(status(2, 0, 0, 1)), 2) shouldBe false
    Counts.of(null) shouldBe Counts(0, 0, 0, 0)
  }

  "IngestV2WriterOrchestrator" should "derive the DM URL like the existing client" in {
    IngestV2WriterOrchestrator.dmUrl(coordinates) shouldBe
      "https://ingest-trd-abc.z6.kusto.fabric.microsoft.com"
    IngestV2WriterOrchestrator.dmUrl(coordinates.copy(ingestionUrl = Some("https://dm"))) shouldBe
      "https://dm"
  }

  it should "keep the existing path for settings ingest-v2 does not support" in {
    def plan(o: WriteOptions, c: IngestionConfig = config(ingestionMethod = Some("Rest"))) =
      IngestV2WriterOrchestrator.plan(coordinates, auth, o, c)
    val lakeConfig =
      config(uploadMethod = Some("Lake"), blobs = Seq.empty, lakes = Seq(workspaceLake))
    val userStorage = Some(Array(new IngestionStorageParameters("https://s", "c", "", "sas")))

    plan(options(writeMode = WriteMode.KustoStreaming)).isLeft shouldBe true
    plan(options(userStorage = userStorage)).isLeft shouldBe true
    plan(
      options(debug =
        KustoCustomDebugWriteOptions(ensureNoDuplicatedBlobs = true))).isLeft shouldBe true
    plan(
      options(),
      config(ingestionMethod = Some("Rest"), blobs = Seq.empty)).isLeft shouldBe true
    plan(options(), lakeConfig).left.get should include("storage token provider")
    plan(
      options(
        format = IngestionFormat.Parquet,
        adjustSchema = SchemaAdjustmentMode.GenerateDynamicCsvMapping),
      lakeConfig).isLeft shouldBe true
    plan(
      options(props = Some(
        new SparkIngestionProperties(
          csvMapping = mapping,
          csvMappingNameReference = "m")))).isLeft shouldBe true
  }

  it should "write CSV when parquet is asked for but only storage containers exist" in {
    IngestV2WriterOrchestrator.effectiveFormat(
      IngestionFormat.Parquet,
      UploadTargets(Seq(storage), isLake = false)) shouldBe IngestionFormat.CSV
    IngestV2WriterOrchestrator.effectiveFormat(
      IngestionFormat.Parquet,
      UploadTargets(Seq(lake), isLake = true)) shouldBe IngestionFormat.Parquet
  }

  "IngestV2SubmittedOperation" should "be serializable, since it is sent from executors to the driver" in {
    val op = IngestV2SubmittedOperation("id", "db", "t", "QUEUED", 3)
    val bytes = new java.io.ByteArrayOutputStream()
    new java.io.ObjectOutputStream(bytes).writeObject(op)
    new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
      .readObject() shouldBe op
  }

  private val privateLinkLake =
    "https://0a1b.z87.daily-onelake.fabric.microsoft.com/0a1b/item/Ingestions/20261103-lakedata"

  it should "send storage tokens only to OneLake hosts" in {
    import IngestV2WriterOrchestrator.isTrustedLakeFolder
    Seq(
      privateLinkLake,
      workspaceLake,
      workspaceLake.replace(".dfs.", ".blob."),
      "https://87e63f193aeb40a99407b9cdb8315ea9.z87.daily-dfs.fabric.microsoft.com/ws/item",
      "https://onelake.dfs.fabric.microsoft.com/ws/item/Files",
      "https://onelake.blob.fabric.microsoft.com/ws/item/Files",
      "https://daily-api.onelake.fabric.microsoft.com/ws/item/Ingestions").foreach { url =>
      withClue(url)(isTrustedLakeFolder(url) shouldBe true)
    }
    Seq(
      "http://onelake.dfs.fabric.microsoft.com/ws/item",
      "https://onelake.dfs.fabric.microsoft.com.evil.com/ws/item",
      "https://evil.com/onelake.dfs.fabric.microsoft.com/ws",
      "https://acct.blob.core.windows.net/c",
      "https://ws1.z6.dfs.fabric.microsoft.com/ws/item",
      "https://87e63f193aeb40a99407b9cdb8315ea9.z87.dfs.fabric.microsoft.com.evil.com/ws",
      "not a url").foreach { url =>
      withClue(url)(isTrustedLakeFolder(url) shouldBe false)
    }
    val untrusted = config(uploadMethod = Some("Lake"), lakes = Seq("https://evil.com/ws/item"))
    IngestV2WriterOrchestrator
      .plan(coordinates, auth, options(tokenProvider = Some(new TestTokenProvider)), untrusted)
      .left
      .get should include("evil.com")
    IngestV2WriterOrchestrator
      .plan(
        coordinates,
        auth,
        options(tokenProvider = Some(new TestTokenProvider)),
        config(uploadMethod = Some("Lake"), lakes = Seq(privateLinkLake)))
      .isRight shouldBe true
  }

  "IngestV2ParquetWriter.tooLarge" should "reject a file above the service limit before submitting" in {
    IngestV2ParquetWriter.tooLarge(Seq("a" -> 10L, "b" -> 100L), 100L) shouldBe None
    IngestV2ParquetWriter.tooLarge(Seq("a" -> 10L, "b" -> 101L), 100L).get should
      (include("'b'") and include("repartition"))
  }

  "IngestV2QueuedWriter" should "resubmit only when the service cannot have accepted the request" in {
    import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
    import IngestV2QueuedWriter.isSafeToResubmit
    def ingest(message: String, code: Integer = null) =
      new IngestException(message, null, code, null, null)
    isSafeToResubmit(ingest("Failed. Status: 429, Body: {}")) shouldBe true
    isSafeToResubmit(ingest("Failed. Status: 503, Body: {}")) shouldBe true
    isSafeToResubmit(ingest("x", 503)) shouldBe true
    isSafeToResubmit(ingest("Failed. Status: 503, Body: x. OperationId N/A")) shouldBe true
    // The service answered with an operation id, so it may have accepted the blobs.
    isSafeToResubmit(ingest("Failed. Status: 503, Body: x. OperationId 0f1e-77")) shouldBe false
    isSafeToResubmit(ingest("Failed. Status: 429, Body: x. OperationId 0f1e-77")) shouldBe false
    isSafeToResubmit(ingest("Failed. Status: 500, Body: {}")) shouldBe false
    isSafeToResubmit(ingest("Failed. Status: 504, Body: {}")) shouldBe false
    isSafeToResubmit(ingest("no status")) shouldBe false
    // The SDK reports 404 with a ConnectException cause; it must not be retried as a connect error.
    isSafeToResubmit(
      new IngestException(
        "not found",
        new java.net.ConnectException(),
        404,
        null,
        false)) shouldBe
      false
    isSafeToResubmit(new RuntimeException(new java.net.ConnectException("refused"))) shouldBe true
    isSafeToResubmit(new java.net.UnknownHostException("dm")) shouldBe true
    isSafeToResubmit(new java.net.SocketTimeoutException("read")) shouldBe false
    isSafeToResubmit(new java.util.concurrent.TimeoutException()) shouldBe false
  }

  it should "retry within one deadline and a bounded number of attempts" in {
    import IngestV2QueuedWriter.{withSubmitRetries, MaxSubmitAttempts}
    val retryable = new java.net.ConnectException("refused")
    def deadline(seconds: Long) = System.nanoTime() + Duration.ofSeconds(seconds).toNanos

    var calls = 0
    val sleeps = scala.collection.mutable.ArrayBuffer[Long]()
    withSubmitRetries(deadline(600), "t", sleeps += _) { _ =>
      calls += 1
      if (calls < 3) throw retryable else "ok"
    } shouldBe "ok"
    calls shouldBe 3
    sleeps should have size 2
    sleeps.head should (be >= 1000L and be <= 2000L)
    sleeps(1) should (be >= 2000L and be <= 4000L)

    calls = 0
    the[java.net.ConnectException] thrownBy withSubmitRetries(deadline(600), "t", _ => ()) { _ =>
      calls += 1
      throw retryable
    }
    calls shouldBe MaxSubmitAttempts

    calls = 0
    an[IllegalStateException] should be thrownBy withSubmitRetries(deadline(600), "t", _ => ()) {
      _ =>
        calls += 1
        throw new IllegalStateException("bad request")
    }
    calls shouldBe 1

    calls = 0
    a[java.net.ConnectException] should be thrownBy withSubmitRetries(deadline(1), "t", _ => ()) {
      _ =>
        calls += 1
        throw retryable
    }
    calls shouldBe 1
  }

  "IngestV2StatusTracker.waitForCompletion" should "poll until every blob succeeded" in {
    val ops = (1 to 70).map(i => IngestV2SubmittedOperation(s"op$i", "db", "t", "QUEUED", 2))
    val reads = new java.util.concurrent.atomic.AtomicInteger()
    var sleeps = 0
    IngestV2StatusTracker.waitForCompletion(
      ops,
      Duration.ofMinutes(5),
      "req",
      _ => {
        val round = reads.incrementAndGet() > ops.size
        completed(if (round) status(2, 0, 0, 0) else status(1, 0, 1, 0))
      },
      _ => sleeps += 1)
    reads.get shouldBe 140
    sleeps shouldBe 1
  }

  it should "fail at once when a blob failed, was canceled or went missing" in {
    def run(s: Status) = {
      var sleeps = 0
      val e = the[IllegalStateException] thrownBy IngestV2StatusTracker.waitForCompletion(
        Seq(IngestV2SubmittedOperation("op", "db", "t", "QUEUED", 3)),
        Duration.ofMinutes(5),
        "req",
        _ => completed(s),
        _ => sleeps += 1)
      sleeps shouldBe 0
      e.getMessage should include(".show ingestion failures")
    }
    run(status(2, 1, 0, 0))
    run(status(2, 0, 0, 1))

    // A failure in the first group is reported before the next group is read.
    val ops = (1 to IngestV2StatusTracker.MaxParallelStatusCalls + 1)
      .map(i => IngestV2SubmittedOperation(s"op$i", "db", "t", "QUEUED", 1))
    var reads = 0
    an[IllegalStateException] should be thrownBy IngestV2StatusTracker.waitForCompletion(
      ops,
      Duration.ofMinutes(5),
      "req",
      _ => { reads += 1; completed(status(0, 1, 0, 0)) },
      _ => ())
    reads shouldBe IngestV2StatusTracker.MaxParallelStatusCalls
    run(status(2, 0, 0, 0))
    run(status(1, 1, 1, 0))
  }

  it should "stop on permanent status errors and retry other errors" in {
    import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
    val op = Seq(IngestV2SubmittedOperation("op", "db", "t", "QUEUED", 1))
    val forbidden = new IngestException("forbidden", null, 403, null, null)
    the[IngestException] thrownBy IngestV2StatusTracker.waitForCompletion(
      op,
      Duration.ofMinutes(5),
      "req",
      _ => failed(forbidden),
      _ => ()) shouldBe forbidden

    var reads = 0
    IngestV2StatusTracker.waitForCompletion(
      op,
      Duration.ofMinutes(5),
      "req",
      _ => {
        reads += 1
        reads match {
          case 1 => failed(new IngestException("busy", null, 503, null, null))
          case 2 => failed(new IngestException("not found", null, 404, null, null))
          case 3 => completed(status(0, 0, 0, 0))
          case _ => completed(status(1, 0, 0, 0))
        }
      },
      _ => ())
    reads shouldBe 4
  }

  it should "give up at the deadline with the last error" in {
    val op = Seq(IngestV2SubmittedOperation("op", "db", "t", "QUEUED", 1))
    a[java.util.concurrent.TimeoutException] should be thrownBy IngestV2StatusTracker
      .waitForCompletion(
        op,
        Duration.ofMillis(200),
        "req",
        _ => completed(status(0, 0, 1, 0)),
        ms => Thread.sleep(ms))
    val busy = new RuntimeException("busy")
    the[RuntimeException] thrownBy IngestV2StatusTracker.waitForCompletion(
      op,
      Duration.ofMillis(200),
      "req",
      _ => failed(busy),
      ms => Thread.sleep(ms)) shouldBe busy
  }

  "IngestV2ConfigurationProvider.withRetries" should "retry transient errors only" in {
    import scala.util.{Failure, Success}
    var calls = 0
    IngestV2ConfigurationProvider.withRetries(3, Duration.ZERO) {
      calls += 1
      if (calls < 3) Failure(new RuntimeException("503")) else Success("ok")
    } shouldBe Success("ok")
    calls = 0
    IngestV2ConfigurationProvider
      .withRetries(3, Duration.ZERO) {
        calls += 1
        Failure(new RuntimeException("503"))
      }
      .isFailure shouldBe true
    calls shouldBe 3
    calls = 0
    IngestV2ConfigurationProvider
      .withRetries(3, Duration.ZERO) {
        calls += 1
        Failure(new java.util.concurrent.TimeoutException())
      }
      .isFailure shouldBe true
    calls shouldBe 1
  }

  "IngestV2ConfigurationCache" should "give the SDK the connector's cached configuration" in {
    val dm = "https://ingest-cache-test.kusto.windows.net"
    val response = new ConfigurationResponse(
      new ContainerSettings(List(new ContainerInfo(storage)).asJava, null, null, null),
      new IngestionSettings(5L, 1L, "Rest"))
    IngestV2ConfigurationProvider.seed(
      dm,
      auth,
      response,
      IngestionConfig.fromConfigurationResponse(response).get)
    try {
      val cache = new IngestV2ConfigurationCache(dm, auth)
      val data = cache
        .getConfiguration(null)
        .asInstanceOf[com.microsoft.azure.kusto.ingest.v2.common.CachedConfigurationData]
      data.getResponse shouldBe response
      IngestV2ConfigurationProvider.getConfiguration(dm, auth).get.maxBlobsPerBatch shouldBe 5
    } finally IngestV2ConfigurationProvider.clearCache()
  }

  private def status(succeeded: Long, failed: Long, inProgress: Long, canceled: Long) =
    new Status(succeeded, failed, inProgress, canceled)
  private def completed(s: Status) = java.util.concurrent.CompletableFuture.completedFuture(s)
  private def failed(e: Throwable) = {
    val f = new java.util.concurrent.CompletableFuture[Status]()
    f.completeExceptionally(e)
    f
  }

  private class TestTokenProvider extends StorageTokenProvider {
    override def getToken: String = "token"
  }
}

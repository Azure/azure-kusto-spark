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
    cacheTtl(Some(Duration.ofSeconds(5))) shouldBe MinCacheTtl
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

  "LakeFolderLocation" should "split the URL like the .NET LakeFolder" in {
    val location = LakeFolderLocation.parse(
      "https://ws1.z6.dfs.fabric.microsoft.com/workspace-id/item/Files/my%20folder")
    location.containerUrl shouldBe "https://ws1.z6.blob.fabric.microsoft.com/workspace-id"
    location.folder shouldBe "item/Files/my folder"
    location.rawFolder shouldBe "item/Files/my%20folder"
    location.blobPath("a.csv.gz") shouldBe "item/Files/my folder/a.csv.gz"
    LakeFolderLocation.parse("https://h.blob.x.com/ws").blobPath("a") shouldBe "a"
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
    val lakeConfig = config(uploadMethod = Some("Lake"), blobs = Seq.empty, lakes = Seq(lake))
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
}

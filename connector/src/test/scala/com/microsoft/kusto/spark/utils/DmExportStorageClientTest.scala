// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class DmExportStorageClientTest extends AnyFlatSpec with Matchers {
  private val objectMapper = new ObjectMapper()

  private def parse(json: String): ExportStorageTargets =
    DmExportStorageClient.parseExportStorageResponse(objectMapper.readTree(json))

  "parseExportStorageResponse" should "read blob containers with their SAS" in {
    val targets = parse("""
        |{
        |  "exportStorage": {
        |    "containers": [
        |      {"path": "https://acc1.blob.core.windows.net/exportcontainer?sv=2018-03-28&sr=c&sp=rwl"},
        |      {"path": "https://acc2.blob.core.windows.net/exportcontainer?sv=2018-03-28&sr=c&sp=rwl"}
        |    ],
        |    "lakeFolders": []
        |  }
        |}""".stripMargin)

    targets.containers should have size 2
    targets.containers.head should startWith(
      "https://acc1.blob.core.windows.net/exportcontainer?")
    targets.lakeFolders shouldBe empty
    targets.isEmpty shouldBe false
  }

  it should "read OneLake lake folders" in {
    val targets = parse("""
        |{
        |  "exportStorage": {
        |    "containers": [],
        |    "lakeFolders": [
        |      {"path": "https://daily-onelake.dfs.fabric.microsoft.com/ws-guid/artifact-guid/Ingestions/export/20260806/principalhash"}
        |    ]
        |  }
        |}""".stripMargin)

    targets.containers shouldBe empty
    targets.lakeFolders should have size 1
    targets.lakeFolders.head should endWith("/Ingestions/export/20260806/principalhash")
  }

  it should "parse a real DM response that returns both blob containers and a lake folder" in {
    // Matches the live response shape, with synthetic storage credentials.
    val targets = parse("""
        |{
        |  "exportStorage": {
        |    "containers": [
        |      {"path": "https://syntheticaccount1.blob.core.windows.net/exportcontainer-1?sv=test&sr=c&sp=rwdl&sig=synthetic-signature-1"},
        |      {"path": "https://syntheticaccount2.blob.core.windows.net/exportcontainer-2?sv=test&sr=c&sp=rwdl&sig=synthetic-signature-2"}
        |    ],
        |    "lakeFolders": [
        |      {"path": "https://453f06078e38475d808c5451f2c0e516.z45.daily-onelake.fabric.microsoft.com/453f0607-8e38-475d-808c-5451f2c0e516/893bf5ee-77c1-437a-9b3a-2731c0576aef/Ingestions/export/20260817-lakedata/b879c86e7c71b6dfc48ffea1bb09c4a2"}
        |    ]
        |  }
        |}""".stripMargin)

    targets.containers should have size 2
    targets.lakeFolders should have size 1
    targets.lakeFolders.head shouldBe
      "https://453f06078e38475d808c5451f2c0e516.z45.daily-onelake.fabric.microsoft.com/" +
      "453f0607-8e38-475d-808c-5451f2c0e516/893bf5ee-77c1-437a-9b3a-2731c0576aef/" +
      "Ingestions/export/20260817-lakedata/b879c86e7c71b6dfc48ffea1bb09c4a2"
    // The lake folder must carry no SAS - access is by caller impersonation.
    targets.lakeFolders.head should not include "?"
  }

  it should "tolerate different property casing" in {
    val targets = parse("""
        |{
        |  "ExportStorage": {
        |    "Containers": [{"Path": "https://acc.blob.core.windows.net/c?sv=x"}],
        |    "LakeFolders": null
        |  }
        |}""".stripMargin)

    targets.containers shouldBe Seq("https://acc.blob.core.windows.net/c?sv=x")
    targets.lakeFolders shouldBe empty
  }

  it should "return empty targets when both arrays are missing" in {
    val targets = parse("""{"exportStorage": {}}""")
    targets.isEmpty shouldBe true
  }

  it should "skip entries without a usable path" in {
    val targets = parse("""
        |{"exportStorage": {"containers": [{"path": ""}, {"other": "x"}, {"path": "https://a.blob.core.windows.net/c?sv=x"}]}}
        |""".stripMargin)

    targets.containers shouldBe Seq("https://a.blob.core.windows.net/c?sv=x")
  }

  it should "fail when the response has no exportStorage object" in {
    a[RuntimeException] should be thrownBy parse("""{"somethingElse": {}}""")
  }

  it should "fail when exportStorage is not an object" in {
    a[RuntimeException] should be thrownBy parse("""{"exportStorage": []}""")
    a[RuntimeException] should be thrownBy parse("""{"exportStorage": "invalid"}""")
  }

  "redactSecrets" should "strip query strings so SAS tokens never reach the log" in {
    val body = """{"error":"denied for https://acc.blob.core.windows.net/c?sv=2021&sig=SECRET"}"""
    val redacted = DmExportStorageClient.redactSecrets(body)
    redacted should not include "SECRET"
    redacted should not include "sv=2021"
    redacted should include("https://acc.blob.core.windows.net/c?<redacted>")
  }

  it should "leave text without a query string untouched" in {
    DmExportStorageClient.redactSecrets("Bad gateway") shouldBe "Bad gateway"
  }

  "isPermanentFailure" should "not retry client errors other than throttling and timeouts" in {
    DmExportStorageClient.isPermanentFailure(401) shouldBe true
    DmExportStorageClient.isPermanentFailure(403) shouldBe true
    DmExportStorageClient.isPermanentFailure(404) shouldBe true
    DmExportStorageClient.isPermanentFailure(301) shouldBe true
    DmExportStorageClient.isPermanentFailure(302) shouldBe true
    DmExportStorageClient.isPermanentFailure(307) shouldBe true
    DmExportStorageClient.isPermanentFailure(408) shouldBe false
    DmExportStorageClient.isPermanentFailure(429) shouldBe false
    DmExportStorageClient.isPermanentFailure(500) shouldBe false
    DmExportStorageClient.isPermanentFailure(503) shouldBe false
  }

  "isAuthorizationFailure" should "single out credential rejections from other client errors" in {
    DmExportStorageClient.isAuthorizationFailure(401) shouldBe true
    DmExportStorageClient.isAuthorizationFailure(403) shouldBe true
    DmExportStorageClient.isAuthorizationFailure(404) shouldBe false
    DmExportStorageClient.isAuthorizationFailure(429) shouldBe false
    DmExportStorageClient.isAuthorizationFailure(500) shouldBe false
  }

  "retryAfterMillis" should "honour delta-seconds and ignore anything it cannot trust" in {
    DmExportStorageClient.retryAfterMillis(Some("5")) shouldBe Some(5000L)
    DmExportStorageClient.retryAfterMillis(Some("  3 ")) shouldBe Some(3000L)
    DmExportStorageClient.retryAfterMillis(Some("7")) shouldBe Some(5000L)
    DmExportStorageClient.retryAfterMillis(Some("100000")) shouldBe Some(5000L)
    DmExportStorageClient.retryAfterMillis(Some("Wed, 21 Oct 2015 07:28:00 GMT")) shouldBe None
    DmExportStorageClient.retryAfterMillis(
      Some("Wed, 02 Sep 2026 07:30:04 GMT"),
      Instant.parse("2026-09-02T07:30:00Z")) shouldBe Some(4000L)
    DmExportStorageClient.retryAfterMillis(Some("0")) shouldBe None
    DmExportStorageClient.retryAfterMillis(Some("-3")) shouldBe None
    DmExportStorageClient.retryAfterMillis(Some("")) shouldBe None
    DmExportStorageClient.retryAfterMillis(None) shouldBe None
  }

  private def config(json: String): IngestionConfiguration =
    DmExportStorageClient.parseIngestionConfiguration(objectMapper.readTree(json))

  /** Live Fabric ingestion-configuration response shape with SAS values replaced. */
  private val privateLinkConfiguration =
    """
      |{
      |  "containerSettings": {
      |    "containers": [
      |      {"path": "https://acc1.blob.core.windows.net/ingestdata-0?sv=2018-03-28&sr=c&sp=rw"},
      |      {"path": "https://acc2.blob.core.windows.net/ingestdata-0?sv=2018-03-28&sr=c&sp=rw"}
      |    ],
      |    "lakeFolders": [
      |      {"path": "https://ws.z45.onelake.fabric.microsoft.com/ws/artifact/Ingestions/20260825-lakedata"}
      |    ],
      |    "refreshInterval": 3600,
      |    "preferredUploadMethod": "Lake"
      |  },
      |  "ingestionSettings": {
      |    "maxBlobsPerBatch": 20,
      |    "maxDataSize": 6442450944,
      |    "preferredIngestionMethod": "Rest"
      |  }
      |}
      |""".stripMargin

  "parseIngestionConfiguration" should "read a live Fabric private link payload" in {
    val parsed = config(privateLinkConfiguration)
    parsed.lakeFolders shouldBe Seq(
      "https://ws.z45.onelake.fabric.microsoft.com/ws/artifact/Ingestions/20260825-lakedata")
    parsed.preferredUploadMethod shouldBe Some("Lake")
    parsed.preferredIngestionMethod shouldBe Some("Rest")
    parsed.refreshInterval shouldBe Some(java.time.Duration.ofHours(1))
    parsed.invalidRefreshInterval shouldBe false
  }

  it should "parse documented TimeSpan refresh intervals" in {
    config(
      """{"containerSettings":{"refreshInterval":"01:00:00"}}""").refreshInterval shouldBe Some(
      java.time.Duration.ofHours(1))
    config(
      """{"containerSettings":{"refreshInterval":"1.01:00:00"}}""").refreshInterval shouldBe Some(
      java.time.Duration.ofHours(25))
  }

  it should "parse integral numeric refresh intervals as seconds" in {
    config("""{"containerSettings":{"refreshInterval":3600}}""").refreshInterval shouldBe Some(
      java.time.Duration.ofHours(1))
  }

  it should "fall back when the refresh interval is invalid" in {
    val malformed =
      config("""{"containerSettings":{"refreshInterval":"not-a-duration"}}""")
    malformed.refreshInterval shouldBe None
    malformed.invalidRefreshInterval shouldBe true

    val zero = config("""{"containerSettings":{"refreshInterval":"00:00:00"}}""")
    zero.refreshInterval shouldBe None
    zero.invalidRefreshInterval shouldBe true

    val negative = config("""{"containerSettings":{"refreshInterval":-60}}""")
    negative.refreshInterval shouldBe None
    negative.invalidRefreshInterval shouldBe true

    val fractional = config("""{"containerSettings":{"refreshInterval":60.5}}""")
    fractional.refreshInterval shouldBe None
    fractional.invalidRefreshInterval shouldBe true
  }

  "effectiveRefreshInterval" should "bound service values and preserve the default" in {
    DmExportStorageClient.effectiveRefreshInterval(None) shouldBe
      DmExportStorageClient.DefaultRefreshInterval
    DmExportStorageClient.effectiveRefreshInterval(
      Some(java.time.Duration.ofSeconds(10))) shouldBe java.time.Duration.ofMinutes(10)
    DmExportStorageClient.effectiveRefreshInterval(
      Some(java.time.Duration.ofMinutes(10))) shouldBe java.time.Duration.ofMinutes(10)
    DmExportStorageClient.effectiveRefreshInterval(
      Some(java.time.Duration.ofHours(1))) shouldBe java.time.Duration.ofHours(1)
    DmExportStorageClient.effectiveRefreshInterval(
      Some(java.time.Duration.ofHours(2))) shouldBe java.time.Duration.ofHours(2)
    DmExportStorageClient.effectiveRefreshInterval(Some(java.time.Duration.ofHours(25))) shouldBe
      DmExportStorageClient.DefaultRefreshInterval
  }

  it should "report no lake folders for a cluster that is not behind private link" in {
    val parsed = config("""
        |{
        |  "containerSettings": {
        |    "containers": [{"path": "https://acc1.blob.core.windows.net/ingestdata-0?sv=x"}],
        |    "lakeFolders": [],
        |    "preferredUploadMethod": "Storage"
        |  },
        |  "ingestionSettings": {"preferredIngestionMethod": "Queue"}
        |}
        |""".stripMargin)
    parsed.lakeFolders shouldBe empty
    parsed.preferredUploadMethod shouldBe Some("Storage")
  }

  it should "treat an absent lakeFolders array as no lake folders" in {
    config("""{"containerSettings": {"containers": []}}""").lakeFolders shouldBe empty
  }

  it should "tolerate an absent or null preferredIngestionMethod" in {
    // The service contract marks this field nullable, so the gate must never require it.
    config(
      """{"containerSettings": {"lakeFolders": []}}""").preferredIngestionMethod shouldBe None
    config("""
        |{"containerSettings": {"lakeFolders": []},
        | "ingestionSettings": {"preferredIngestionMethod": null}}
        |""".stripMargin).preferredIngestionMethod shouldBe None
  }

  it should "not be tied to the casing of the service serializer" in {
    val parsed = config("""
        |{
        |  "ContainerSettings": {
        |    "LakeFolders": [{"Path": "https://ws.z45.onelake.fabric.microsoft.com/ws/a/f"}],
        |    "PreferredUploadMethod": "Lake"
        |  },
        |  "IngestionSettings": {"PreferredIngestionMethod": "Rest"}
        |}
        |""".stripMargin)
    parsed.lakeFolders should have size 1
    parsed.preferredUploadMethod shouldBe Some("Lake")
    parsed.preferredIngestionMethod shouldBe Some("Rest")
  }

  it should "drop blank folder paths rather than emit an unusable target" in {
    config("""
        |{"containerSettings": {"lakeFolders": [
        |  {"path": ""}, {"path": "   "}, {"path": null}, {"nopath": "x"},
        |  {"path": "https://ws.z45.onelake.fabric.microsoft.com/ws/a/f"}
        |]}}
        |""".stripMargin).lakeFolders shouldBe
      Seq("https://ws.z45.onelake.fabric.microsoft.com/ws/a/f")
  }

  it should "reject a payload with no containerSettings" in {
    // An empty or foreign body must fail loudly here so the caller falls back, rather than
    // silently reporting "not private link" for a cluster that is.
    a[RuntimeException] should be thrownBy config("""{"ingestionSettings": {}}""")
    a[RuntimeException] should be thrownBy config("""{}""")
  }

  it should "reject a payload whose containerSettings is not an object" in {
    a[RuntimeException] should be thrownBy config("""{"containerSettings": []}""")
    a[RuntimeException] should be thrownBy config("""{"containerSettings": "invalid"}""")
  }

  "exportStorageMode" should "select lake folders only for Rest and Lake" in {
    config(privateLinkConfiguration).exportStorageMode shouldBe ExportStorageMode.LakeFolders
    configuration(uploadMethod = "Lake", ingestionMethod = "Rest").exportStorageMode shouldBe
      ExportStorageMode.LakeFolders
  }

  it should "select blob containers only for Rest and Storage" in {
    configuration(uploadMethod = "Storage", ingestionMethod = "Rest").exportStorageMode shouldBe
      ExportStorageMode.BlobContainers
  }

  it should "use the legacy command for every other combination" in {
    configuration(uploadMethod = "Lake", ingestionMethod = "Queue").exportStorageMode shouldBe
      ExportStorageMode.LegacyCommand
    configuration(uploadMethod = "Storage", ingestionMethod = "Queue").exportStorageMode shouldBe
      ExportStorageMode.LegacyCommand
    configuration(uploadMethod = "Something", ingestionMethod = "Rest").exportStorageMode shouldBe
      ExportStorageMode.LegacyCommand
    configuration(uploadMethod = "Lake", ingestionMethod = "Something").exportStorageMode shouldBe
      ExportStorageMode.LegacyCommand
  }

  it should "ignore lake folders when the preference pair selects the legacy command" in {
    val ordinaryFabric = config("""
        |{
        |  "containerSettings": {
        |    "containers": [{"path": "https://acc1.blob.core.windows.net/ingestdata-0?sv=x"}],
        |    "lakeFolders": [
        |      {"path": "https://ws.z45.onelake.fabric.microsoft.com/ws/artifact/Ingestions/d"}
        |    ],
        |    "preferredUploadMethod": "Storage"
        |  },
        |  "ingestionSettings": {"preferredIngestionMethod": "Queue"}
        |}
        |""".stripMargin)
    ordinaryFabric.lakeFolders should have size 1
    ordinaryFabric.exportStorageMode shouldBe ExportStorageMode.LegacyCommand
  }

  it should "use the legacy command when either preference is absent" in {
    IngestionConfiguration(Seq.empty, None, None).exportStorageMode shouldBe
      ExportStorageMode.LegacyCommand
    IngestionConfiguration(Seq.empty, Some("Lake"), None).exportStorageMode shouldBe
      ExportStorageMode.LegacyCommand
    IngestionConfiguration(Seq.empty, None, Some("Rest")).exportStorageMode shouldBe
      ExportStorageMode.LegacyCommand
  }

  it should "not depend on the casing the service happens to use" in {
    configuration(
      uploadMethod = "lake",
      ingestionMethod = "REST").exportStorageMode shouldBe ExportStorageMode.LakeFolders
    configuration(
      uploadMethod = "storage",
      ingestionMethod = "REST").exportStorageMode shouldBe ExportStorageMode.BlobContainers
  }

  private def configuration(uploadMethod: String, ingestionMethod: String) =
    IngestionConfiguration(Seq.empty, Some(uploadMethod), Some(ingestionMethod))
}

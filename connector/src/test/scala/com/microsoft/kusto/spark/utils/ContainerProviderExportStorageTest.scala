// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{ClientRequestProperties, KustoOperationResult}
import io.github.resilience4j.retry.RetryConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration

class ContainerProviderExportStorageTest extends AnyFlatSpec with Matchers {
  private val clusterAlias = "somecluster"
  private val dmUrl = "https://ingest-somecluster.kusto.fabric.microsoft.com"
  private val exportContainersCommand = ".show export containers"
  private val commandContainer =
    "https://commandacc.blob.core.windows.net/exportcontainer?sv=2018-03-28&sr=c&sp=rwl"
  private val apiContainer =
    "https://apiacc.blob.core.windows.net/exportcontainer?sv=2018-03-28&sr=c&sp=rwl"
  private val lakeFolder =
    "https://daily-onelake.dfs.fabric.microsoft.com/ws-guid/artifact-guid/Ingestions/export/20260806/hash"
  // Shape observed from a real DM ExportStorage response: workspace-specific FQDN
  // ({workspaceIdNoDashes}.z{first2}.{fqdn}) and NO '.dfs.' label in the host.
  private val realWorkspaceFqdnLakeFolder =
    "https://453f06078e38475d808c5451f2c0e516.z45.daily-onelake.fabric.microsoft.com/" +
      "453f0607-8e38-475d-808c-5451f2c0e516/893bf5ee-77c1-437a-9b3a-2731c0576aef/" +
      "Ingestions/export/20260817-lakedata/b879c86e7c71b6dfc48ffea1bb09c4a2"

  private class StubExportStorageClient(
      result: Option[ExportStorageTargets],
      refreshInterval: Duration = DmExportStorageClient.DefaultRefreshInterval)
      extends DmExportStorageClient(new ConnectionStringBuilder(dmUrl), clusterAlias) {
    var callCount = 0
    override private[kusto] def resolveExportStorage: ExportStorageResolution = {
      callCount += 1
      ExportStorageResolution(result, refreshInterval)
    }
  }

  private class StubKustoClient
      extends ExtendedKustoClient(
        new ConnectionStringBuilder("https://somecluster.kusto.fabric.microsoft.com"),
        new ConnectionStringBuilder(dmUrl),
        clusterAlias) {
    var executeDmCallCount = 0
    var returnedContainer: String = commandContainer
    var failRefresh = false
    override def executeDM(
        command: String,
        maybeCrp: Option[ClientRequestProperties],
        activityName: String,
        retryConfig: Option[RetryConfig]): KustoOperationResult = {
      executeDmCallCount += 1
      command shouldBe exportContainersCommand
      if (failRefresh) throw new RuntimeException("temporary command failure")
      new KustoOperationResult(
        s"""{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"StorageRoot","DataType":"String","ColumnType":"string"}],
           |"Rows":[["$returnedContainer"]]}]}""".stripMargin,
        "v1")
    }
  }

  private def provider(
      kustoClient: ExtendedKustoClient,
      exportStorageClient: Option[DmExportStorageClient]) =
    new ContainerProvider(
      kustoClient,
      clusterAlias,
      exportContainersCommand,
      KustoConstants.StorageExpirySeconds,
      exportStorageClient)

  "getExportContainers" should "use lake folders selected by the export storage client" in {
    val kustoClient = new StubKustoClient
    val exportStorageClient = new StubExportStorageClient(
      Some(ExportStorageTargets(containers = Seq.empty, lakeFolders = Seq(lakeFolder))))

    val containers = provider(kustoClient, Some(exportStorageClient)).getExportContainers

    containers should have size 1
    containers.head.containerUrl shouldBe lakeFolder
    containers.head.sas shouldBe ""
    ExtendedKustoClient.toTransientStorageCredentials(containers.head).isOneLake shouldBe true
    exportStorageClient.callCount shouldBe 1
    kustoClient.executeDmCallCount shouldBe 0
  }

  it should "discard invalid lake folders when at least one valid target remains" in {
    val kustoClient = new StubKustoClient
    val exportStorageClient = new StubExportStorageClient(
      Some(
        ExportStorageTargets(
          containers = Seq.empty,
          lakeFolders = Seq("not-a-url", s"$lakeFolder?sv=x", lakeFolder))))

    val containers = provider(kustoClient, Some(exportStorageClient)).getExportContainers

    containers should have size 1
    containers.head.containerUrl shouldBe lakeFolder
    containers.head.sas shouldBe ""
    kustoClient.executeDmCallCount shouldBe 0
  }

  it should "fall back when every lake folder is invalid" in {
    val kustoClient = new StubKustoClient
    val exportStorageClient = new StubExportStorageClient(
      Some(
        ExportStorageTargets(
          containers = Seq.empty,
          lakeFolders = Seq(
            "not-a-url",
            "https://daily-onelake.dfs.fabric.microsoft.com/workspace/artifact",
            "https://onelake.attacker.example/workspace/artifact/path",
            s"$lakeFolder?sv=x"))))

    val containers = provider(kustoClient, Some(exportStorageClient)).getExportContainers

    containers should have size 1
    containers.head.containerUrl shouldBe "https://commandacc.blob.core.windows.net/exportcontainer"
    containers.head.sas shouldBe "?sv=2018-03-28&sr=c&sp=rwl"
    kustoClient.executeDmCallCount shouldBe 1
  }

  it should "use blob containers when the API returns no lake folders" in {
    val kustoClient = new StubKustoClient
    val exportStorageClient = new StubExportStorageClient(
      Some(ExportStorageTargets(containers = Seq(apiContainer), lakeFolders = Seq.empty)))

    val containers = provider(kustoClient, Some(exportStorageClient)).getExportContainers

    containers should have size 1
    containers.head.containerUrl shouldBe "https://apiacc.blob.core.windows.net/exportcontainer"
    containers.head.sas shouldBe "?sv=2018-03-28&sr=c&sp=rwl"
    ExtendedKustoClient.toTransientStorageCredentials(containers.head).isOneLake shouldBe false
    exportStorageClient.callCount shouldBe 1
    kustoClient.executeDmCallCount shouldBe 0
  }

  it should "discard invalid API blob targets when at least one valid target remains" in {
    val kustoClient = new StubKustoClient
    val exportStorageClient = new StubExportStorageClient(
      Some(
        ExportStorageTargets(
          containers = Seq(
            "not-a-url",
            "https://apiacc.blob.core.windows.net/exportcontainer",
            "https://apiacc.blob.core.windows.net/exportcontainer;impersonate",
            apiContainer),
          lakeFolders = Seq.empty)))

    val containers = provider(kustoClient, Some(exportStorageClient)).getExportContainers

    containers should have size 1
    containers.head.containerUrl shouldBe "https://apiacc.blob.core.windows.net/exportcontainer"
    containers.head.sas shouldBe "?sv=2018-03-28&sr=c&sp=rwl"
    kustoClient.executeDmCallCount shouldBe 0
  }

  it should "fall back when every API blob target is invalid or SAS-less" in {
    val kustoClient = new StubKustoClient
    val exportStorageClient = new StubExportStorageClient(
      Some(
        ExportStorageTargets(
          containers = Seq(
            "not-a-url",
            "https://apiacc.blob.core.windows.net/exportcontainer",
            "https://apiacc.blob.core.windows.net/exportcontainer;impersonate",
            "http://apiacc.blob.core.windows.net/exportcontainer?sv=x"),
          lakeFolders = Seq.empty)),
      Duration.ofMillis(500))
    val containerProvider = provider(kustoClient, Some(exportStorageClient))

    val containers = containerProvider.getExportContainers

    containers should have size 1
    containers.head.containerUrl shouldBe "https://commandacc.blob.core.windows.net/exportcontainer"
    containers.head.sas shouldBe "?sv=2018-03-28&sr=c&sp=rwl"
    kustoClient.executeDmCallCount shouldBe 1

    Thread.sleep(650)

    containerProvider.getExportContainers should not be empty
    exportStorageClient.callCount shouldBe 2
    kustoClient.executeDmCallCount shouldBe 2
  }

  it should "use lake folders when the API returns no blob containers" in {
    val kustoClient = new StubKustoClient
    val exportStorageClient = new StubExportStorageClient(
      Some(ExportStorageTargets(containers = Seq.empty, lakeFolders = Seq(lakeFolder))))

    val containers = provider(kustoClient, Some(exportStorageClient)).getExportContainers

    containers should have size 1
    containers.head.containerUrl shouldBe lakeFolder
    containers.head.sas shouldBe ""
    ExtendedKustoClient.toTransientStorageCredentials(containers.head).isOneLake shouldBe true
    kustoClient.executeDmCallCount shouldBe 0
  }

  it should "fall back to the export containers command when the API is unavailable" in {
    val kustoClient = new StubKustoClient
    val exportStorageClient = new StubExportStorageClient(None)

    val containers = provider(kustoClient, Some(exportStorageClient)).getExportContainers

    containers should have size 1
    containers.head.containerUrl shouldBe "https://commandacc.blob.core.windows.net/exportcontainer"
    containers.head.sas shouldBe "?sv=2018-03-28&sr=c&sp=rwl"
    ExtendedKustoClient.toTransientStorageCredentials(containers.head).isOneLake shouldBe false
    exportStorageClient.callCount shouldBe 1
    kustoClient.executeDmCallCount shouldBe 1
  }

  it should "fall back to the export containers command when no API client is configured" in {
    val kustoClient = new StubKustoClient

    val containers = provider(kustoClient, None).getExportContainers

    containers should have size 1
    containers.head.containerUrl shouldBe "https://commandacc.blob.core.windows.net/exportcontainer"
    kustoClient.executeDmCallCount shouldBe 1
  }

  it should "keep legacy results cached until the legacy expiry" in {
    val kustoClient = new StubKustoClient
    val containerProvider = provider(kustoClient, None)

    containerProvider.getExportContainers shouldBe containerProvider.getExportContainers
    kustoClient.executeDmCallCount shouldBe 1
  }

  it should "preserve legacy command parsing when no API client is configured" in {
    val kustoClient = new StubKustoClient
    kustoClient.returnedContainer = s"$commandContainer?extra"

    provider(kustoClient, None).getExportContainers.head.sas shouldBe
      "?sv=2018-03-28&sr=c&sp=rwl"

    kustoClient.returnedContainer = "https://commandacc.blob.core.windows.net/exportcontainer"
    intercept[ArrayIndexOutOfBoundsException] {
      provider(kustoClient, None).getExportContainers
    }
  }

  it should "retry each expired legacy refresh without the API backoff" in {
    val kustoClient = new StubKustoClient
    val containerProvider =
      new ContainerProvider(kustoClient, clusterAlias, exportContainersCommand, -1)
    val original = containerProvider.getExportContainers
    kustoClient.failRefresh = true

    containerProvider.getExportContainers shouldBe original
    containerProvider.getExportContainers shouldBe original
    kustoClient.executeDmCallCount shouldBe 3
  }

  it should "propagate a cold legacy command failure" in {
    val kustoClient = new StubKustoClient
    kustoClient.failRefresh = true

    intercept[RuntimeException] {
      provider(kustoClient, None).getExportContainers
    }.getMessage shouldBe "temporary command failure"
    kustoClient.executeDmCallCount shouldBe 1
  }

  "getTempBlobsForExport" should "use legacy blob credentials by default" in {
    val kustoClient = new StubKustoClient

    kustoClient.exportStorageApiEnabled shouldBe false
    val credentials = kustoClient.getTempBlobsForExport.storageCredentials
    credentials should have length 1
    credentials.head.isOneLake shouldBe false
    credentials.head.storageAccountName shouldBe "commandacc"
    credentials.head.sasKey shouldBe "?sv=2018-03-28&sr=c&sp=rwl"
    kustoClient.getTempBlobsForExport.storageCredentials.head.sasUrl shouldBe commandContainer
    kustoClient.executeDmCallCount shouldBe 1
  }

  "getExportContainers" should "refresh legacy results after the legacy expiry" in {
    val kustoClient = new StubKustoClient
    val containerProvider =
      new ContainerProvider(kustoClient, clusterAlias, exportContainersCommand, 0)

    containerProvider.getExportContainers should not be empty
    Thread.sleep(1100)
    containerProvider.getExportContainers should not be empty
    kustoClient.executeDmCallCount shouldBe 2
  }

  it should "refresh API targets after the service interval" in {
    val kustoClient = new StubKustoClient
    val exportStorageClient = new StubExportStorageClient(
      Some(ExportStorageTargets(containers = Seq.empty, lakeFolders = Seq(lakeFolder))),
      Duration.ofMillis(500))
    val containerProvider = provider(kustoClient, Some(exportStorageClient))

    containerProvider.getExportContainers should not be empty
    containerProvider.getExportContainers should not be empty
    exportStorageClient.callCount shouldBe 1

    Thread.sleep(650)

    containerProvider.getExportContainers should not be empty
    exportStorageClient.callCount shouldBe 2
  }

  it should "refresh a legacy fallback after the service interval" in {
    val kustoClient = new StubKustoClient
    val exportStorageClient =
      new StubExportStorageClient(None, Duration.ofMillis(500))
    val containerProvider = provider(kustoClient, Some(exportStorageClient))

    containerProvider.getExportContainers should not be empty
    containerProvider.getExportContainers should not be empty
    kustoClient.executeDmCallCount shouldBe 1

    Thread.sleep(650)

    containerProvider.getExportContainers should not be empty
    exportStorageClient.callCount shouldBe 2
    kustoClient.executeDmCallCount shouldBe 2
  }

  "parseContainerWithSas" should "split the container url from its SAS" in {
    val parsed = ContainerProvider.parseContainerWithSas(
      "https://acc.blob.core.windows.net/c?sv=2018-03-28&sr=c")
    parsed.containerUrl shouldBe "https://acc.blob.core.windows.net/c"
    parsed.sas shouldBe "?sv=2018-03-28&sr=c"
    parsed.productArity shouldBe 2
  }

  it should "return an empty SAS when the url has no query string" in {
    val parsed = ContainerProvider.parseContainerWithSas("https://acc.blob.core.windows.net/c")
    parsed.containerUrl shouldBe "https://acc.blob.core.windows.net/c"
    parsed.sas shouldBe ""
  }

  "parseValidApiContainerWithSas" should "use the existing transient-storage SAS validation" in {
    ContainerProvider.parseValidApiContainerWithSas(apiContainer) shouldBe defined
    ContainerProvider.parseValidApiContainerWithSas(
      "https://apiacc.blob.core.windows.net/exportcontainer") shouldBe empty
    ContainerProvider.parseValidApiContainerWithSas(
      "https://apiacc.blob.core.windows.net/exportcontainer;impersonate") shouldBe empty
    ContainerProvider.parseValidApiContainerWithSas(
      "https://apiacc.blob.core.windows.net/exportcontainer/?sv=x") shouldBe defined
    ContainerProvider.parseValidApiContainerWithSas(
      "http://apiacc.blob.core.windows.net/exportcontainer?sv=x") shouldBe empty
    ContainerProvider.parseValidApiContainerWithSas("not-a-url") shouldBe empty
  }

  "parseValidApiLakeFolder" should "accept only valid OneLake folder paths" in {
    ContainerProvider.parseValidApiLakeFolder(lakeFolder) shouldBe defined
    ContainerProvider.parseValidApiLakeFolder(realWorkspaceFqdnLakeFolder) shouldBe defined
    ContainerProvider.parseValidApiLakeFolder(
      "https://onelake-int-edog.dfs.pbidedicated.windows-int.net/ws/artifact/path") shouldBe defined
    ContainerProvider.parseValidApiLakeFolder(s"$lakeFolder?sv=x") shouldBe empty
    ContainerProvider.parseValidApiLakeFolder(
      "https://daily-onelake.dfs.fabric.microsoft.com/workspace/artifact") shouldBe empty
    ContainerProvider.parseValidApiLakeFolder(
      "https://onelake.attacker.example/workspace/artifact/path") shouldBe empty
    ContainerProvider.parseValidApiLakeFolder(
      "https://storage.fabric.microsoft.com/workspace/artifact/path") shouldBe empty
    ContainerProvider.parseValidApiLakeFolder(
      "https://onelake.dfs.fabric.microsoft.com.evil.example/workspace/artifact/path") shouldBe empty
    ContainerProvider.parseValidApiLakeFolder("not-a-url") shouldBe empty
  }

  "toTransientStorageCredentials" should "build a OneLake credential for lake folders" in {
    val credentials = ExtendedKustoClient.toTransientStorageCredentials(
      ContainerProvider.parseValidApiLakeFolder(lakeFolder).get)

    credentials.isOneLake shouldBe true
    credentials.oneLakeEndpoint shouldBe "daily-onelake.dfs.fabric.microsoft.com"
    credentials.oneLakeWorkspace shouldBe "ws-guid"
    credentials.oneLakeArtifactPath shouldBe "artifact-guid/Ingestions/export/20260806/hash"
    credentials.oneLakeAbfssBase shouldBe
      "abfss://ws-guid@daily-onelake.dfs.fabric.microsoft.com/artifact-guid/Ingestions/export/20260806/hash"
  }

  it should "build a SAS credential for blob containers" in {
    val credentials = ExtendedKustoClient.toTransientStorageCredentials(
      ContainerAndSas(
        "https://apiacc.blob.core.windows.net/exportcontainer",
        "?sv=2018-03-28&sr=c&sp=rwl"))

    credentials.isOneLake shouldBe false
    credentials.storageAccountName shouldBe "apiacc"
    credentials.blobContainer shouldBe "exportcontainer"
    credentials.domainSuffix shouldBe "core.windows.net"
  }

  it should "accept a workspace-specific FQDN that has no '.dfs.' label" in {
    val credentials = ExtendedKustoClient.toTransientStorageCredentials(
      ContainerProvider.parseValidApiLakeFolder(realWorkspaceFqdnLakeFolder).get)

    credentials.isOneLake shouldBe true
    credentials.oneLakeEndpoint shouldBe
      "453f06078e38475d808c5451f2c0e516.z45.daily-onelake.fabric.microsoft.com"
    credentials.oneLakeWorkspace shouldBe "453f0607-8e38-475d-808c-5451f2c0e516"
    credentials.oneLakeArtifactPath shouldBe
      "893bf5ee-77c1-437a-9b3a-2731c0576aef/Ingestions/export/20260817-lakedata/b879c86e7c71b6dfc48ffea1bb09c4a2"
    credentials.oneLakeAbfssBase shouldBe
      "abfss://453f0607-8e38-475d-808c-5451f2c0e516@453f06078e38475d808c5451f2c0e516.z45." +
      "daily-onelake.fabric.microsoft.com/893bf5ee-77c1-437a-9b3a-2731c0576aef/Ingestions/" +
      "export/20260817-lakedata/b879c86e7c71b6dfc48ffea1bb09c4a2"
  }

  it should "accept the shorter lake folder shape returned by the live service" in {
    val credentials = ExtendedKustoClient.toTransientStorageCredentials(
      ContainerProvider
        .parseValidApiLakeFolder(
          "https://453f06078e38475d808c5451f2c0e516.z45.daily-onelake.fabric.microsoft.com/" +
            "453f0607-8e38-475d-808c-5451f2c0e516/893bf5ee-77c1-437a-9b3a-2731c0576aef/" +
            "Ingestions/20260822-lakedata")
        .get)

    credentials.isOneLake shouldBe true
    credentials.oneLakeArtifactPath shouldBe
      "893bf5ee-77c1-437a-9b3a-2731c0576aef/Ingestions/20260822-lakedata"
  }

  "public compatibility" should "retain the existing JVM constructor and product shape" in {
    classOf[ContainerProvider].getConstructors.map(_.getParameterCount) should contain(4)

    val container = ContainerAndSas("https://account.blob.core.windows.net/container", "?sv=x")
    container.productArity shouldBe 2
    ContainerAndSas.unapply(container) shouldBe Some((container.containerUrl, container.sas))
  }
}

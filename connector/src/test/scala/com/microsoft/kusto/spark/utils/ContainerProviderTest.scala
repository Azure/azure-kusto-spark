// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.azure.storage.blob.BlobContainerClient
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{Client, KustoOperationResult}
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas
import com.microsoft.azure.kusto.ingest.{IngestionResourceManager, QueuedIngestClient}
import com.microsoft.kusto.spark.datasink.IngestionStorageParameters
import com.microsoft.kusto.spark.exceptions.NoStorageContainersException
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyLong}
import org.mockito.Mockito
import org.mockito.Mockito.{doAnswer, doReturn, spy, times}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.time.Instant
import java.util.Collections
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source

class ContainerProviderTest extends AnyFlatSpec with Matchers with MockFactory {
  private val CACHE_EXPIRY_SEC = 30
  private val SLEEP_TIME_SEC = 10
  private val clusterAlias = "ingest-cluster"

  private def createExtendedKustoMockClient(
      hasEmptyResults: Boolean = false,
      mockDmClient: Client,
      maybeExceptionThrown: Option[Throwable] = None,
      getRMOccurances: Int = 1): ExtendedKustoClient = {
    val mockIngestClient: QueuedIngestClient = mock[QueuedIngestClient]
    val mockIngestionResourceManager: IngestionResourceManager =
      Mockito.mock[IngestionResourceManager](classOf[IngestionResourceManager])

    maybeExceptionThrown match {
      case Some(exception) =>
        Mockito
          .when(mockIngestionResourceManager.getShuffledContainers)
          .thenThrow(
            exception,
            exception,
            exception,
            exception,
            exception,
            exception,
            exception,
            exception)
          . // throws exception 8 times due to retry
          thenAnswer(_ => List(getMockContainerWithSas(1), getMockContainerWithSas(2)).asJava)
      case None =>
        if (hasEmptyResults) {
          Mockito
            .when(mockIngestionResourceManager.getShuffledContainers)
            .thenAnswer(_ => Collections.EMPTY_LIST)
        } else {
          Mockito
            .when(mockIngestionResourceManager.getShuffledContainers)
            .thenAnswer(_ => List(getMockContainerWithSas(1), getMockContainerWithSas(2)).asJava)
        }
    }
    // Expecting getResourceManager to be called maxCommandsRetryAttempts i.e. 8 times.
    mockIngestClient.getResourceManager _ expects () repeated getRMOccurances times () returning mockIngestionResourceManager
    // Unfortunately we cannot Mock this class as there is a member variable that is a val and cannot be mocked
    new ExtendedKustoClient(
      new ConnectionStringBuilder("https://somecluster.eastus.kusto.windows.net/"),
      new ConnectionStringBuilder("https://ingest-somecluster.eastus.kusto.windows.net"),
      "somecluster") {
      override lazy val ingestClient: QueuedIngestClient = mockIngestClient
      override lazy val dmClient: Client = mockDmClient
    }
  }

  private def getMockContainerWithSas(index: Int): ContainerWithSas = {
    val mockResultsOne: ContainerWithSas =
      Mockito.mock[ContainerWithSas](classOf[ContainerWithSas])
    val blobResultsOne: BlobContainerClient =
      Mockito.mock[BlobContainerClient](classOf[BlobContainerClient])
    Mockito
      .when(blobResultsOne.getBlobContainerUrl)
      .thenAnswer(_ =>
        s"https://sacc$index.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0")
    Mockito.when(mockResultsOne.getSas).thenAnswer(_ => "?sv=2018-03-28&sr=c&sp=rw")
    Mockito.when(mockResultsOne.getContainer).thenAnswer(_ => blobResultsOne)
    mockResultsOne
  }
  // happy path
  "ContainerProvider returns a container" should "from RM" in {
    val mockDmClient = mock[Client]

    val command = ".create tempstorage"
    /*
      Invoke and test
     */
    val extendedMockClient = createExtendedKustoMockClient(mockDmClient = mockDmClient)
    val containerProvider =
      new ContainerProvider(extendedMockClient, clusterAlias, command, CACHE_EXPIRY_SEC)
    containerProvider.getContainer().containerUrl should (not be "")
    val ingestionContainer1 = "https://sacc1.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0"
    val ingestionContainer2 = "https://sacc2.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0"
    Some(containerProvider.getContainer().containerUrl) should contain oneOf
      (ingestionContainer1,
      ingestionContainer2)
    containerProvider.getContainer().sas should (not be "")
    /* Second test that returns from cache. The test will fail if the client is invoked again as expectation is to call once */
    containerProvider.getContainer().containerUrl should (not be "")
    Some(containerProvider.getContainer().containerUrl) should contain oneOf
      (ingestionContainer1,
      ingestionContainer2)
    containerProvider.getContainer().sas should (not be "")
    /* Third test where the cache expires and the invocation throws an exception */
    Thread.sleep(SLEEP_TIME_SEC * 1000) // Milliseconds
    containerProvider.getContainer().containerUrl should (not be "")
    Some(containerProvider.getContainer().containerUrl) should contain oneOf
      (ingestionContainer1,
      ingestionContainer2)
    containerProvider.getContainer().sas should (not be "")

    // The case where storageUris.nonEmpty is false. This will throw the exception as there is nothing to give from the cache
    Thread.sleep((SLEEP_TIME_SEC * 2) * 1000) // Milliseconds

    val mockDmFailClient = mock[Client]
    val extendedMockClientEmptyFail = createExtendedKustoMockClient(
      hasEmptyResults = true,
      mockDmClient = mockDmFailClient,
      getRMOccurances = 8)
    val emptyStorageContainerProvider =
      new ContainerProvider(extendedMockClientEmptyFail, clusterAlias, command, CACHE_EXPIRY_SEC)
    val caught =
      intercept[NoStorageContainersException] { // Result type: Assertion
        emptyStorageContainerProvider.getContainer()
      }
    assert(
      caught.getMessage.indexOf(
        "No storage containers received. Failed to allocate temporary storage") != -1)
  }

  "ContainerProvider" should "fail in the case when call succeeds but returns no storage" in {
    val clusterAlias = "ingest-cluster"
    val command = ".create tempstorage"

    val kustoOperationResult =
      new KustoOperationResult(readTestSource("storage-result-empty.json"), "v1")
    val mockDmClient = mock[Client]
    /*
      Invoke and test
     */
    val extendedMockClient = createExtendedKustoMockClient(
      hasEmptyResults = true,
      mockDmClient = mockDmClient,
      getRMOccurances = 8)
    /*
      Invoke and test. In this case the call succeeds but returns no storage. This will hit the empty storage block
     */
    val containerProvider =
      new ContainerProvider(extendedMockClient, clusterAlias, command, CACHE_EXPIRY_SEC)
    the[NoStorageContainersException] thrownBy containerProvider
      .getContainer() should have message "No storage containers received. Failed to allocate temporary storage"
  }

  "ContainerProvider" should "retry and return a container in case of a temporary HTTPException" in {
    val clusterAlias = "ingest-cluster"
    val command = ".get ingestion resources"
    /*
      Invoke and test
     */
    val mockDmClient = mock[Client]
    val extendedMockClient = createExtendedKustoMockClient(
      mockDmClient = mockDmClient,
      maybeExceptionThrown =
        Some(new IngestionServiceException("IOError when trying to retrieve CloudInfo")),
      getRMOccurances = 8)
    val containerProvider =
      new ContainerProvider(extendedMockClient, clusterAlias, command, CACHE_EXPIRY_SEC)
    the[IngestionServiceException] thrownBy containerProvider
      .getContainer() should have message "IOError when trying to retrieve CloudInfo"
  }

  it should "generate new SAS token when cache is expired" in {
    val ingestionContainer1 = "https://custom.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0"
    val ingestionStorageParam = new IngestionStorageParameters(ingestionContainer1, "container","msi","")
    val arrIngestionStorageParams = Array(ingestionStorageParam)
    val mockDmClient = mock[Client]
    val command = ".create tempstorage"
    /*
      Invoke and test
     */
    val extendedMockClient = createExtendedKustoMockClient(
      hasEmptyResults = true,
      mockDmClient = mockDmClient,
      getRMOccurances = 0)
    val cacheTimeoutSec = 3
    val containerProvider =
      new ContainerProvider(extendedMockClient, clusterAlias, command, cacheTimeoutSec)
   val mockContainerProvider = spy(containerProvider)
   val mockedSasKey = "?mockedSasToken"
    var count = 0
   doAnswer(answer(_ => {
     count = count + 1
     s"$mockedSasKey-$count"
   })).when(mockContainerProvider)
      .generateSasKey(anyLong(), anyBoolean(),any[IngestionStorageParameters]())
    // This will fail if it is not returned from the cache!
    mockContainerProvider.getContainer(Some(arrIngestionStorageParams)).sas should equal(s"$mockedSasKey-1")
    mockContainerProvider.getContainer(Some(arrIngestionStorageParams)).sas should equal(s"$mockedSasKey-1")
    // this is greater by 1s
    Thread.sleep((cacheTimeoutSec + 1) * 1000  )
    mockContainerProvider.getContainer(Some(arrIngestionStorageParams)).sas should equal(s"$mockedSasKey-2")
  }

  def answer[T](f: InvocationOnMock => T): Answer[T] = {
    new Answer[T] {
      override def answer(invocation: InvocationOnMock): T = f(invocation)
    }
  }

  private def readTestSource(fileName: String): String = {
    val queryResultsSource =
      Source.fromFile(this.getClass.getResource(s"/TestData/json/$fileName").getPath)
    val queryResults = queryResultsSource.getLines().mkString
    queryResultsSource.close()
    queryResults
  }
}

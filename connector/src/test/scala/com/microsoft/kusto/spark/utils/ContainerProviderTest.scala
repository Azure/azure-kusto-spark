package com.microsoft.kusto.spark.utils

import com.azure.storage.blob.BlobContainerClient
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.exceptions.DataServiceException
import com.microsoft.azure.kusto.data.{Client, ClientRequestProperties, KustoOperationResult}
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas
import com.microsoft.azure.kusto.ingest.{IngestionResourceManager, QueuedIngestClient}
import org.apache.http.HttpHost
import org.apache.http.conn.HttpHostConnectException
import org.mockito.Mockito
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.net.{ConnectException, InetAddress}
import java.util.Collections
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source


class ContainerProviderTest extends AnyFlatSpec with Matchers with MockFactory {
  val CACHE_EXPIRY_SEC = 2
  val SLEEP_TIME_SEC = 10

  private def createExtendedKustoMockClient(hasEmptyResults: Boolean = false, mockDmClient: Client,
                                            mayBeExceptionThrown: Option[Throwable] = None): ExtendedKustoClient = {
    val mockIngestClient: QueuedIngestClient = mock[QueuedIngestClient]
    val mockIngestionResourceManager: IngestionResourceManager = Mockito.mock[IngestionResourceManager](classOf[IngestionResourceManager])

    mayBeExceptionThrown match {
      case Some(exception) => Mockito.when(mockIngestionResourceManager.getShuffledContainers)
        .thenThrow(exception).thenThrow(exception).
        thenAnswer(_ => List(getMockContainerWithSas(1), getMockContainerWithSas(2)).asJava)
      case None => if (hasEmptyResults) {
        Mockito.when(mockIngestionResourceManager.getShuffledContainers).
          thenAnswer(_ => Collections.EMPTY_LIST)
      } else {
        Mockito.when(mockIngestionResourceManager.getShuffledContainers).
          thenAnswer(_ => List(getMockContainerWithSas(1), getMockContainerWithSas(2)).asJava)
      }
    }
    // noMoreThanTwice()
    mockIngestClient.getResourceManager _ expects() returning mockIngestionResourceManager
    // Unfortunately we cannot Mock this class as there is a member variable that is a val and cannot be mocked
    new ExtendedKustoClient(new ConnectionStringBuilder("https://somecluster.eastus.kusto.windows.net/"),
      new ConnectionStringBuilder("https://ingest-somecluster.eastus.kusto.windows.net"), "somecluster") {
      override lazy val ingestClient: QueuedIngestClient = mockIngestClient
      override lazy val dmClient: Client = mockDmClient
    }
  }

  private def getMockContainerWithSas(index: Int): ContainerWithSas = {
    val mockResultsOne: ContainerWithSas = Mockito.mock[ContainerWithSas](classOf[ContainerWithSas])
    val blobResultsOne: BlobContainerClient = Mockito.mock[BlobContainerClient](classOf[BlobContainerClient])
    Mockito.when(blobResultsOne.getBlobContainerUrl).thenAnswer(_ => s"https://sacc$index.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0")
    Mockito.when(mockResultsOne.getSas).thenAnswer(_ => "?sv=2018-03-28&sr=c&sp=rw")
    Mockito.when(mockResultsOne.getContainer).thenAnswer(_ => blobResultsOne)
    mockResultsOne
  }
  // happy path
  "ContainerProvider" should "return a container" in {
    val kustoOperationResult = new KustoOperationResult(readTestSource("storage-result.json"), "v1")
    val mockDmClient = mock[Client]
    (mockDmClient.execute(_: String, _: String, _: ClientRequestProperties)).expects(*, *, *).
      noMoreThanOnce().returning(kustoOperationResult)

    val clusterAlias = "ingest-cluster"
    val command = ".create tempstorage"
    /*
      Invoke and test
     */
    val extendedMockClient = createExtendedKustoMockClient(mockDmClient = mockDmClient)
    val containerProvider = new ContainerProvider(extendedMockClient, clusterAlias, command, CACHE_EXPIRY_SEC)
    containerProvider.getContainer.containerUrl should (not be "")
    Some(containerProvider.getContainer.containerUrl) should contain oneOf
      ("https://sacc1.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0",
        "https://sacc2.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0")
    containerProvider.getContainer.sas should (not be "")

    /* Second test that returns from cache. The test will fail if the client is invoked again as expectation is to call once */
    containerProvider.getContainer.containerUrl should (not be "")
    Some(containerProvider.getContainer.containerUrl) should contain oneOf
      ("https://sacc1.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0",
        "https://sacc2.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0")
    containerProvider.getContainer.sas should (not be "")
    /* Third test where the cache expires and the invocation throws an exception */
    Thread.sleep(SLEEP_TIME_SEC * 1000) // Milliseconds
    containerProvider.getContainer.containerUrl should (not be "")
    Some(containerProvider.getContainer.containerUrl) should contain oneOf
      ("https://sacc1.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0",
        "https://sacc2.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0")
    containerProvider.getContainer.sas should (not be "")

    // The case where storageUris.nonEmpty is false. This will throw the exception as there is nothing to give from the cache
    Thread.sleep((SLEEP_TIME_SEC * 2) * 1000) // Milliseconds

    val mockDmFailClient = mock[Client]
    val extendedMockClientEmptyFail = createExtendedKustoMockClient(hasEmptyResults = true, mockDmClient = mockDmFailClient)
    val emptyStorageContainerProvider = new ContainerProvider(extendedMockClientEmptyFail, clusterAlias, command, CACHE_EXPIRY_SEC)
    val caught =
      intercept[RuntimeException] { // Result type: Assertion
        emptyStorageContainerProvider.getContainer
      }
    assert(caught.getMessage.indexOf("Failed to allocate temporary storage") != -1)
  }

  "ContainerProvider" should "fail in the case when call succeeds but returns no storage" in {
    val clusterAlias = "ingest-cluster"
    val command = ".create tempstorage"

    val kustoOperationResult = new KustoOperationResult(readTestSource("storage-result-empty.json"), "v1")
    val mockDmClient = mock[Client]
    (mockDmClient.execute(_: String, _: String, _: ClientRequestProperties)).expects(*, *, *).
      noMoreThanOnce() returning (kustoOperationResult)
    /*
      Invoke and test
     */
    val extendedMockClient = createExtendedKustoMockClient(hasEmptyResults = true, mockDmClient = mockDmClient)
    /*
      Invoke and test. In this case the call succeeds but returns no storage. This will hit the empty storage block
     */
    val containerProvider = new ContainerProvider(extendedMockClient, clusterAlias, command, CACHE_EXPIRY_SEC)
    the[RuntimeException] thrownBy containerProvider.getContainer should have message "Failed to allocate temporary storage"
  }

  "ContainerProvider" should "retry and return a container in case of a temporary HTTPException" in {
    val clusterAlias = "ingest-cluster"
    val command = ".get ingestion resources"
    /*
      Invoke and test
     */
    val mockDmClient = mock[Client]
    val extendedMockClient = createExtendedKustoMockClient(mockDmClient = mockDmClient,
      mayBeExceptionThrown = Some(new IngestionServiceException("IOError when trying to retrieve CloudInfo")))
    val containerProvider = new ContainerProvider(extendedMockClient, clusterAlias, command, CACHE_EXPIRY_SEC)
    the[IngestionServiceException] thrownBy containerProvider.getContainer should have message "IOError when trying to retrieve CloudInfo"
  }

  private def readTestSource(fileName: String): String = {
    val queryResultsSource = Source.fromFile(this.getClass.getResource(s"/TestData/json/$fileName").getPath)
    val queryResults = queryResultsSource.getLines().mkString
    queryResultsSource.close()
    queryResults
  }
}

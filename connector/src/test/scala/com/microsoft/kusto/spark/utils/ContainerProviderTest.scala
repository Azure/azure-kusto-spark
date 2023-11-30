package com.microsoft.kusto.spark.utils

import com.microsoft.azure.kusto.data.KustoOperationResult
import com.microsoft.azure.kusto.data.exceptions.DataServiceException
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.http.HttpHost
import org.apache.http.conn.HttpHostConnectException
import org.scalamock.scalatest.MockFactory
import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{ConnectException, InetAddress}
import scala.io.Source
import java.io.IOException

@Ignore
class ContainerProviderTest extends AnyFlatSpec with Matchers with MockFactory {
  val CACHE_EXPIRY_SEC = 2
  val SLEEP_TIME_SEC = 10
  // happy path
  "ContainerProvider" should "return a container" in {
    val extendedMockClient = mock[ExtendedKustoClient]
    val extendedMockClientEmptyFail = mock[ExtendedKustoClient]
    val kustoOperationResult = new KustoOperationResult(readTestSource("storage-result.json"), "v1")
    val clusterAlias = "ingest-cluster"
    val command = ".create tempstorage"
    val ingestProviderEntryCreator = (c: ContainerAndSas) => c
    /*
      Invoke and test
     */
    val containerProvider = new ContainerProvider(extendedMockClient, clusterAlias, command,CACHE_EXPIRY_SEC)
    extendedMockClient.executeDM _ expects(command, None, *) noMoreThanOnce() returning kustoOperationResult
    containerProvider.getContainer.containerUrl should(not be "")
    Some(containerProvider.getContainer.containerUrl) should contain oneOf
      ("https://sacc1.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0",
        "https://sacc2.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0")
    containerProvider.getContainer.sas should(not be "")

    /* Second test that returns from cache. The test will fail if the client is invoked again as expectation is to call once */
    containerProvider.getContainer.containerUrl should (not be "")
    Some(containerProvider.getContainer.containerUrl) should contain oneOf
      ("https://sacc1.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0",
        "https://sacc2.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0")
    containerProvider.getContainer.sas should (not be "")


    /* Third test where the cache expires and the invocation throws an exception */
    Thread.sleep(SLEEP_TIME_SEC * 1000) // Milliseconds
    extendedMockClient.executeDM _ expects(command, None, *) throws new DataServiceException(clusterAlias,"Cannot create temp storage",false)
    containerProvider.getContainer.containerUrl should (not be "")
    Some(containerProvider.getContainer.containerUrl) should contain oneOf
      ("https://sacc1.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0",
        "https://sacc2.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0")
    containerProvider.getContainer.sas should (not be "")
    // The case where storageUris.nonEmpty is false. This will throw the exception as there is nothing to give from the cache
    Thread.sleep(SLEEP_TIME_SEC * 1000) // Milliseconds

    extendedMockClientEmptyFail.executeDM _ expects(command, None, *) throws new DataServiceException(clusterAlias, "Cannot create temp storage", false)
    val emptyStorageContainerProvider = new ContainerProvider(extendedMockClientEmptyFail, clusterAlias, command,CACHE_EXPIRY_SEC)
    val caught =
      intercept[DataServiceException] { // Result type: Assertion
        emptyStorageContainerProvider.getContainer
    }
    assert(caught.getMessage.indexOf("Cannot create temp storage") != -1)
  }

  "ContainerProvider" should "fail in the case when call succeeds but returns no storage" in {
    val extendedMockClient = mock[ExtendedKustoClient]
    val kustoOperationResult = new KustoOperationResult(readTestSource("storage-result-empty.json"), "v1")
    val clusterAlias = "ingest-cluster"
    val command = ".create tempstorage"
    val ingestProviderEntryCreator = (c: ContainerAndSas) => c
    /*
      Invoke and test. In this case the call succeeds but returns no storage. This will hit the empty storage block
     */
    val containerProvider = new ContainerProvider(extendedMockClient, clusterAlias, command, CACHE_EXPIRY_SEC)
    extendedMockClient.executeDM _ expects(command, None, *) noMoreThanOnce() returning kustoOperationResult
    the[RuntimeException] thrownBy containerProvider.getContainer should have message "Failed to allocate temporary storage"
  }

  "ContainerProvider" should "retry and return a container in case of a temporary HTTPException" in {
    val extendedMockClient = mock[ExtendedKustoClient]
    val extendedMockNoRootExceptionFail = mock[ExtendedKustoClient]
    val extendedMockClientIOExceptionFail = mock[ExtendedKustoClient]
    val kustoOperationResult = new KustoOperationResult(readTestSource("storage-result.json"), "v1")
    val clusterAlias = "ingest-cluster"
    val command = ".get ingestion resources"
    /*
      Invoke and test
     */
    val containerProvider = new ContainerProvider(extendedMockClient, clusterAlias, command, CACHE_EXPIRY_SEC)

    extendedMockClient.executeDM _ expects(command, None, *) throws new DataServiceException(clusterAlias,
      "IOError when trying to retrieve CloudInfo",new HttpHostConnectException(
      new IOException(new ConnectException("Connection timed out")),
      HttpHost.create("kustocluster.centralus.kusto.windows.net"),
      InetAddress.getLoopbackAddress),true) once() returning kustoOperationResult
    // The first call will fail with a HttpHostConnectException. The second call will succeed
    containerProvider.getContainer.containerUrl should (not be "")
    Some(containerProvider.getContainer.containerUrl) should contain oneOf
      ("https://sacc1.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0",
        "https://sacc2.blob.core.windows.net/20230430-ingestdata-e5c334ee145d4b4-0")
    containerProvider.getContainer.sas should (not be "")

    // Test where the root exception below it is empty, It will fail
    extendedMockNoRootExceptionFail.executeDM _ expects(command, None, *) throws new DataServiceException(clusterAlias,
      "No root exception", false)
    val noRootExceptionContainerProvider = new ContainerProvider(extendedMockNoRootExceptionFail, clusterAlias, command,
      CACHE_EXPIRY_SEC)
    val caught =
      intercept[DataServiceException] { // Result type: Assertion
        noRootExceptionContainerProvider.getContainer
      }
    assert(caught.getMessage.indexOf("No root exception") != -1)


    // Test where the root exception below it is not a HttpHostConnectException. It will fail
    extendedMockClientIOExceptionFail.executeDM _ expects(command, None, *) throws new DataServiceException(clusterAlias,
      "No root exception", new IOException(new ConnectException("IOError when trying to retrieve CloudInfo")), true)
    val ioExceptionContainerProvider = new ContainerProvider(extendedMockClientIOExceptionFail, clusterAlias, command,
      CACHE_EXPIRY_SEC)
    val ioErrorCaught =
      intercept[DataServiceException] { // Result type: Assertion
        ioExceptionContainerProvider.getContainer
      }
    assert(ioErrorCaught.getMessage.indexOf("No root exception") != -1)
    assert(ExceptionUtils.getRootCause(ioErrorCaught).getMessage.indexOf("IOError when trying to retrieve CloudInfo") != -1)
  }

  private def readTestSource(fileName: String): String = {
    val queryResultsSource = Source.fromFile(this.getClass.getResource(s"/TestData/json/$fileName").getPath)
    val queryResults = queryResultsSource.getLines().mkString
    queryResultsSource.close()
    queryResults
  }
}

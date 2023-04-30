package com.microsoft.kusto.spark.utils

import com.microsoft.azure.kusto.data.{KustoOperationResult, KustoResultSetTable}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, FunSuite, Matchers}

import java.util.Collections
import scala.collection.JavaConverters._
import scala.io.Source

class ContainerProviderTest extends FlatSpec with Matchers with MockFactory {

  "ContainerProvider" should "return a container" in {
    val extendedMockClient = mock[ExtendedKustoClient]
    val kustoOperationResult = new KustoOperationResult(readTestSource("storage-result.json"), "v1")


    val clusterAlias = "ingest-cluster"
    val command = ".create tempstorage"
    val ingestProviderEntryCreator = (c: ContainerAndSas) => c
    /*
    (val client: ExtendedKustoClient, val clusterAlias: String, val command: String, cacheEntryCreator: ContainerAndSas => A,
                               cacheExpirySeconds:Int=KustoConstants.StorageExpirySeconds)
     */
    val containerProvider = new ContainerProvider(extendedMockClient, clusterAlias, command,
      ingestProviderEntryCreator, 10)
    extendedMockClient.executeDM _ expects(command, None, *) returning kustoOperationResult
    containerProvider.getContainer.containerUrl should(not be "")
    containerProvider.getContainer.containerUrl should contain("https://")
    containerProvider.getContainer.sas should(not be "")
  }

  private def readTestSource(fileName: String): String = {
    val queryResultsSource = Source.fromFile(this.getClass.getResource(s"/TestData/json/$fileName").getPath)
    val queryResults = queryResultsSource.getLines().mkString
    queryResultsSource.close()
    queryResults
  }
}

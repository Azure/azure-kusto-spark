package com.microsoft.kusto.spark

import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{Client, ClientRequestProperties, KustoOperationResult, KustoResultSetTable}
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.{SparkIngestionProperties, WriteOptions}
import com.microsoft.kusto.spark.utils.ExtendedKustoClient
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, times, verify}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import java.util
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ExtendedKustoClientTests extends FlatSpec with Matchers {
  private val kustoCoordinates = KustoCoordinates("", "", "database", Some("table"))
  class ExtendedKustoClientStub(override val engineKcsb: ConnectionStringBuilder,
                                override val ingestKcsb: ConnectionStringBuilder,
                                override val clusterAlias: String,
                                var tagsToReturn: util.ArrayList[String]) extends ExtendedKustoClient(engineKcsb, ingestKcsb,
    clusterAlias) {
    override lazy val engineClient = mock(classOf[Client])
    override def fetchTableExtentsTags(database: String, table: String, crp: ClientRequestProperties)
    : KustoResultSetTable = {
      val response =
        s"""{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"Tags","DataType":"Object","ColumnType":"dynamic"}],
           "Rows":[[${if (tagsToReturn.isEmpty) "" else tagsToReturn.asScala.map(t => "\"" + t + "\"").asJava}]]}]}"""
      new KustoOperationResult(response, "v1").getPrimaryResults
    }
  }

  "IngestIfNotExists tags shouldIngest" should "return true if no extents given" in {
    val emptyTags = new util.ArrayList[String]
    val stubbedClient = new ExtendedKustoClientStub(null, null, "", null)
    stubbedClient.tagsToReturn = emptyTags
    val props = new SparkIngestionProperties
    val shouldIngestWhenNoTags = stubbedClient.shouldIngestData(kustoCoordinates,
      Some(props.toString), tableExists = true, null)
    shouldIngestWhenNoTags shouldEqual true

    val tags = new util.ArrayList[String]
    tags.add("tag")
    stubbedClient.tagsToReturn = tags
    props.ingestIfNotExists = util.Collections.singletonList("otherTag")
    val shouldIngestWhenNoOverlap = stubbedClient.shouldIngestData(kustoCoordinates,
      Some(props.toString), tableExists = true, null)
    shouldIngestWhenNoOverlap shouldEqual true

    tags.add("otherTag")
    val shouldIngestWhenOverlap = stubbedClient.shouldIngestData(kustoCoordinates,
      Some(props.toString), tableExists = true, null)
    shouldIngestWhenOverlap shouldEqual false
  }

  "initializeTablesBySchema" should "Not create table if queued mode" in {
    val tempTable = "temp"
    val stubbedClient = new ExtendedKustoClientStub(null, null, "", null)
    val struct = StructType(Array(StructField("colA", StringType, nullable = true)))
    stubbedClient.initializeTablesBySchema(kustoCoordinates, tempTable, struct,  Array(new ObjectMapper().readTree("""{"Type":"System.String",
      "CslType":"string", "Name":"name"}""")), WriteOptions(isTransactionalMode = false), null, true)
    verify(stubbedClient.engineClient, times(0)).execute(any(), any(), any())
  }
}

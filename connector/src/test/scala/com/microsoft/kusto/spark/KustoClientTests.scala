package com.microsoft.kusto.spark

import java.util

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{Client, ClientRequestProperties, KustoOperationResult, KustoResultSetTable}
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.{SparkIngestionProperties, WriteOptions}
import com.microsoft.kusto.spark.utils.KustoClient
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.json.JSONObject
import org.junit.runner.RunWith
import org.mockito.Mockito.mock
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class KustoClientTests extends FlatSpec  with Matchers {
  val coords = KustoCoordinates("", "", "database", Some("table"))
  class KustoClientStub (override  val clusterAlias: String, override  val engineKcsb: ConnectionStringBuilder, override val ingestKcsb: ConnectionStringBuilder, var tagsToReturn: util.ArrayList[String]) extends KustoClient(clusterAlias, engineKcsb, ingestKcsb){
    override val engineClient = mock(classOf[Client])
    override def fetchTableExtentsTags(database: String, table: String, crp: ClientRequestProperties)
    : KustoResultSetTable = {
      val response =
        s"""{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"Tags","DataType":"Object","ColumnType":"dynamic"}],
           "Rows":[[${if (tagsToReturn.isEmpty) "" else tagsToReturn.asScala.map(t=>"\""+t+"\"").asJava}]]}]}"""
      new KustoOperationResult(response, "v1").getPrimaryResults
    }
  }

  "IngestIfNotExists tags shouldIngest" should "return true if no extents given" in {
    val emptyTags = new util.ArrayList[String]
    val stubbedClient = new KustoClientStub("", null, null, null)
    stubbedClient.tagsToReturn = emptyTags
    val props = new SparkIngestionProperties
    val shouldIngestWhenNoTags = stubbedClient.shouldIngestData(coords,
      Some(props.toString), tableExists = true, null)
    shouldIngestWhenNoTags shouldEqual true

    val tags = new util.ArrayList[String]
    tags.add("tag")
    stubbedClient.tagsToReturn = tags
    props.ingestIfNotExists = new util.ArrayList[String](){{add("otherTag")}}
    val shouldIngestWhenNoOverlap = stubbedClient.shouldIngestData(coords,
      Some(props.toString), tableExists = true, null)
    shouldIngestWhenNoOverlap shouldEqual true

    tags.add("otherTag")
    val shouldIngestWhenOverlap = stubbedClient.shouldIngestData(coords,
      Some(props.toString), tableExists = true, null)
    shouldIngestWhenOverlap shouldEqual false
  }

  "initializeTablesBySchema" should "Not create table if queued mode" in {
    val stubbedClient = new KustoClientStub("", null, null, null)
    val struct = StructType(Array(StructField("colA", StringType, nullable = true)))
    stubbedClient.initializeTablesBySchema(coords, "temp", struct,  Array(new JSONObject("""{"Type":"System.String",
      "CslType":"string", "Name":"name"}""")), WriteOptions(isTransactionalMode = true), null, true)
    stubbedClient.initializeTablesBySchema(coords, "temp", struct,  Array(new JSONObject("""{"Type":"System.String",
      "CslType":"string", "Name":"name"}""")), WriteOptions(isTransactionalMode = false), null, true)
  }
}

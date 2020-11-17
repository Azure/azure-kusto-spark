package com.microsoft.kusto.spark

import java.util

import com.microsoft.azure.kusto.data.{ConnectionStringBuilder, KustoOperationResult, KustoResultSetTable}
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.SparkIngestionProperties
import com.microsoft.kusto.spark.utils.KustoClient
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class KustoClientTests extends FlatSpec  with Matchers{
  class KustoClientStub (override  val clusterAlias: String, override  val engineKcsb: ConnectionStringBuilder, override val ingestKcsb: ConnectionStringBuilder, var tagsToReturn: util.ArrayList[String]) extends KustoClient(clusterAlias, engineKcsb, ingestKcsb){
    override def fetchTableExtentsTags(database: String, table: String): KustoResultSetTable = {
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
    val shouldIngestWhenNoTags = stubbedClient.shouldIngestData(KustoCoordinates("", "", "database", Some("table")), Some(props.toString), tableExists = true)

    val tags = new util.ArrayList[String]
    tags.add("tag")
    stubbedClient.tagsToReturn = tags
    props.ingestIfNotExists = new util.ArrayList[String](){{add("otherTag")}}
    val shouldIngestWhenNoOverlap = stubbedClient.shouldIngestData(KustoCoordinates("", "","database", Some("table")), Some(props.toString), tableExists = true)
    shouldIngestWhenNoOverlap shouldEqual true

    tags.add("otherTag")
    val shouldIngestWhenOverlap = stubbedClient.shouldIngestData(KustoCoordinates("", "","database", Some("table")), Some(props.toString), tableExists = true)
    shouldIngestWhenOverlap shouldEqual false
  }
}

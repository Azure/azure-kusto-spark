package com.microsoft.kusto.spark.datasource

import com.microsoft.azure.kusto.data.KustoOperationResult
import org.scalatest.FlatSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import java.sql.Timestamp
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class KustoResponseDeserializerTest extends FlatSpec {

  "Data types should get resolved and rows" should "get extracted - v1 queries" in {
    val resultSetTable = new KustoOperationResult(readTestSource("query-results-v1.json"), "v1")
    validateResults(resultSetTable)
  }

  "Data types should get resolved and rows" should "get extracted - v2 queries" in {
    val resultSetTable = new KustoOperationResult(readTestSource("query-results-v2.json"), "v2")
    validateResults(resultSetTable)
  }

  private def readTestSource(fileName: String): String = {
    val queryResultsSource = Source.fromFile(this.getClass.getResource(s"/TestData/json/$fileName").getPath)
    val queryResults = queryResultsSource.getLines().mkString
    queryResultsSource.close()
    queryResults
  }

  private def validateResults(resultSetTable: KustoOperationResult): Unit = {
    assert(resultSetTable != null)
    assert(resultSetTable.getPrimaryResults != null)
    val primaryResults = resultSetTable.getPrimaryResults
    val deserializer = KustoResponseDeserializer.apply(primaryResults)
    assert(deserializer != null)
    val dataRead = deserializer.toRows.asScala
    // 5 rows selected in the JSON
    assert(dataRead.size == 5)
    val colsToRead = deserializer.schema.sparkSchema.fields.map(st => st.name).toList
    val actualRowsRead = deserializer.toRows.asScala
    actualRowsRead.foreach(actualRow => {
      primaryResults.next()
      val rowAsMap = actualRow.getValuesMap(colsToRead)
      val expectedRealResult = primaryResults.getBigDecimal("vreal").doubleValue()
      val actualRealResult = rowAsMap.getOrElse("vreal", 0.0d)
      val expectedLongResult = primaryResults.getLong("vlong")
      val actualLongResult = rowAsMap.getOrElse("vlong", 0L)
      val expectedDateResult = Timestamp.from(java.time.Instant.parse(primaryResults.getString("vdate")))
      val fallbackTime = Timestamp.from(java.time.Instant.now())
      val actualDateResult = rowAsMap.getOrElse("vdate", fallbackTime)
      assert(expectedRealResult == actualRealResult)
      assert(expectedLongResult == actualLongResult)
      assert(expectedDateResult == actualDateResult)
    })
  }
}
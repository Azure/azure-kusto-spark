package com.microsoft.kusto.spark.datasource

import com.microsoft.kusto.spark.utils.KustoQueryUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class KustoQueryUtilsTest extends FlatSpec with MockFactory with Matchers with BeforeAndAfterAll {

  "nomralizeQuery" should "add ; for query" in {
    val stardardQuery = "Table | where column1 == 'abc' "

    KustoQueryUtils.nomralizeQuery(stardardQuery) should be ("Table | where column1 == 'abc';")
  }

  "nomralizeQuery" should "add do nothing for normalized query" in {
    val stardardQuery = "Table | where column1 == 'abc';"

    KustoQueryUtils.nomralizeQuery(stardardQuery) should be ("Table | where column1 == 'abc';")
  }

  "getQuerySchemaQuery" should "add suffix" in {
    val query = "Table | where column1 = 'abc' | summarize by count()"

    KustoQueryUtils.getQuerySchemaQuery(query) should be ("Table | where column1 = 'abc' | summarize by count()| take 1;")
  }
}

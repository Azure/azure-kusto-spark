package com.microsoft.kusto.spark

import com.microsoft.kusto.spark.utils.KustoQueryUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class KustoQueryUtilsTest extends FlatSpec with MockFactory with Matchers with BeforeAndAfterAll {

  "normalizeQuery" should "remove redundant query separator" in {
    val standardQuery = "Table | where column1 == 'abc';"

    KustoQueryUtils.normalizeQuery(standardQuery) should be ("Table | where column1 == 'abc'")
  }

  "getQuerySchemaQuery" should "add suffix" in {
    val query = "Table | where column1 = 'abc' | summarize by count()"

    KustoQueryUtils.getQuerySchemaQuery(query) should be ("Table | where column1 = 'abc' | summarize by count()| take 0")
  }

  "limitQuery" should "add limit" in {
    val query = "Table | where column1 = 'abc' | summarize by count()"

    KustoQueryUtils.limitQuery(query, 5) should be ("Table | where column1 = 'abc' | summarize by count()| take 5")
  }
}

//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark

import com.microsoft.kusto.spark.datasource.{KustoFilter, KustoFiltering, KustoSchema}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Date, Timestamp}

class KustoFilterTests extends AnyFlatSpec with MockFactory with Matchers {

  private val schema: StructType = StructType(
    Seq(
      StructField("string", StringType),
      StructField("bool", BooleanType),
      StructField("int", IntegerType),
      StructField("byte", ByteType),
      StructField("double", DoubleType),
      StructField("float", FloatType),
      StructField("long", LongType),
      StructField("short", ShortType),
      StructField("date", DateType),
      StructField("timestamp", TimestampType)))

  "Filter clause" should "be empty if filters list is empty" in {
    assert(KustoFilter.buildFiltersClause(StructType(Nil), Seq.empty) === "")
  }

  "EqualTo expression" should "construct equality filter correctly for string type" in {
    val filter = KustoFilter.buildFilterExpression(schema, EqualTo("string", "abc"))
    filter shouldBe Some("""['string'] == 'abc'""")
  }

  "EqualTo expression" should "construct equality filter correctly for string with tags" in {
    val filter = KustoFilter.buildFilterExpression(schema, EqualTo("string", "'abc'"))
    filter shouldBe Some("""['string'] == '\'abc\''""")
  }

  "EqualTo expression" should "construct equality filter correctly for date type" in {
    // Java.sql.date  year is 1900-based, month is 0-based
    val filter =
      KustoFilter.buildFilterExpression(schema, EqualTo("date", Date.valueOf("2019-02-21")))
    filter shouldBe Some("""['date'] == datetime('2019-02-21')""")
  }

  "EqualTo expression" should "construct equality filter correctly for timestamp type" in {
    // Java.sql.date  year is 1900-based, month is 0-based
    val filter = KustoFilter.buildFilterExpression(
      schema,
      EqualTo("timestamp", Timestamp.valueOf("2019-02-21 12:30:02.000000123")))
    filter shouldBe Some("""['timestamp'] == datetime('2019-02-21 12:30:02.000000123')""")
  }

  "EqualTo expression" should "construct equality filter correctly for double type" in {
    val filter = KustoFilter.buildFilterExpression(schema, EqualTo("double", 0.13))
    filter shouldBe Some("""['double'] == 0.13""")
  }

  "EqualNullSafe expression" should "translate to isnull when value is null" in {
    val filter = KustoFilter.buildFilterExpression(schema, EqualNullSafe("string", null))
    filter shouldBe Some("""isnull(['string'])""")
  }

  "EqualNullSafe expression" should "translate to equality when value is not null" in {
    val filter = KustoFilter.buildFilterExpression(schema, EqualNullSafe("string", "abc"))
    filter shouldBe Some("""['string'] == 'abc'""")
  }

  "GreaterThan expression" should "construct filter expression correctly for byte type" in {
    val filter = KustoFilter.buildFilterExpression(schema, GreaterThan("byte", 5))
    filter shouldBe Some("""['byte'] > 5""")
  }

  "GreaterThanOrEqual expression" should "construct filter expression correctly for float type" in {
    val filter = KustoFilter.buildFilterExpression(schema, GreaterThanOrEqual("float", 123.456))
    filter shouldBe Some("""['float'] >= 123.456""")
  }

  "LessThan expression" should "construct filter expression correctly for byte type" in {
    val filter = KustoFilter.buildFilterExpression(schema, LessThan("byte", 5))
    filter shouldBe Some("""['byte'] < 5""")
  }

  "LessThanOrEqual expression" should "construct filter expression correctly for float type" in {
    val filter = KustoFilter.buildFilterExpression(schema, LessThanOrEqual("float", 123.456))
    filter shouldBe Some("""['float'] <= 123.456""")
  }

  "In expression" should "construct filter expression correctly for a set of values" in {
    val stringArray = Array("One Mississippi", "Two Mississippi", "Hippo")
    val filter = KustoFilter.buildFilterExpression(
      schema,
      In("string", stringArray.asInstanceOf[Array[Any]]))
    filter shouldBe Some("""['string'] in ('One Mississippi', 'Two Mississippi', 'Hippo')""")
  }

  "IsNull expression" should "construct filter expression correctly" in {
    val filter = KustoFilter.buildFilterExpression(schema, IsNull("byte"))
    filter shouldBe Some("""isnull(['byte'])""")
  }

  "IsNotNull expression" should "construct filter expression correctly" in {
    val filter = KustoFilter.buildFilterExpression(schema, IsNotNull("byte"))
    filter shouldBe Some("""isnotnull(['byte'])""")
  }

  "And expression" should "construct inner filters and than construct the and expression" in {
    val leftFilter = IsNotNull("byte")
    val rightFilter = LessThan("float", 5)

    val filter = KustoFilter.buildFilterExpression(schema, And(leftFilter, rightFilter))
    filter shouldBe Some("""(isnotnull(['byte'])) and (['float'] < 5)""")
  }

  "Or expression" should "construct inner filters and than construct the or expression" in {
    val leftFilter = IsNotNull("byte")
    val rightFilter = LessThan("float", 5)

    val filter = KustoFilter.buildFilterExpression(schema, Or(leftFilter, rightFilter))
    filter shouldBe Some("""(isnotnull(['byte'])) or (['float'] < 5)""")
  }

  "Not expression" should "construct the child filter and than construct the not expression" in {
    val childFilter = IsNotNull("byte")

    val filter = KustoFilter.buildFilterExpression(schema, Not(childFilter))
    filter shouldBe Some("""not(isnotnull(['byte']))""")
  }

  "StringStartsWith expression" should "construct the correct expression" in {
    val filter =
      KustoFilter.buildFilterExpression(schema, StringStartsWith("string", "StartingString"))
    filter shouldBe Some("""['string'] startswith_cs 'StartingString'""")
  }

  "StringEndsWith expression" should "construct the correct expression" in {
    val filter =
      KustoFilter.buildFilterExpression(schema, StringEndsWith("string", "EndingString"))
    filter shouldBe Some("""['string'] endswith_cs 'EndingString'""")
  }

  "StringContains expression" should "construct the correct expression" in {
    val filter =
      KustoFilter.buildFilterExpression(schema, StringContains("string", "ContainedString"))
    filter shouldBe Some("""['string'] contains_cs 'ContainedString'""")
  }

  "Empty columns filter" should "construct an empty string" in {
    val expr = KustoFilter.buildColumnsClause(Array.empty, Set())
    expr shouldBe empty
  }

  "Non-empty columns filter" should "construct a project statement" in {
    val expr = KustoFilter.buildColumnsClause(Array("ColA", "ColB"), Set("ColB"))
    expr shouldBe " | project ['ColA'], tostring(['ColB'])"
  }

  "Providing multiple filters" should "lead to and-concatenation of these filters" in {
    val testSchema =
      StructType(Seq(StructField("ColA", StringType), StructField("ColB", IntegerType)))
    val filters: Array[Filter] =
      Array(StringEndsWith("ColA", "EndingString"), LessThanOrEqual("ColB", 5))
    val expr = KustoFilter.buildFiltersClause(testSchema, filters)

    expr shouldBe " | where ['ColA'] endswith_cs 'EndingString' and ['ColB'] <= 5"
  }

  "Providing two filters when one is resolved to NONE" should "only apply the second filter" in {
    val testSchema =
      StructType(Seq(StructField("ColA", StringType), StructField("ColB", IntegerType)))
    val filters: Array[Filter] =
      Array(StringEndsWith("ColA", "EndingString"), LessThanOrEqual("ColNotInTheSchema", 5))
    val expr = KustoFilter.buildFiltersClause(testSchema, filters)

    expr shouldBe " | where ['ColA'] endswith_cs 'EndingString'"
  }

  "Requesting only column pruning" should "adjust the query with prune expression" in {
    val testSchema =
      StructType(Seq(StructField("ColA", StringType), StructField("ColB", IntegerType)))
    val originalQuery = "MyTable | take 100"
    val columns = Array("ColA", "ColB")
    val filters: Array[Filter] =
      Array(StringEndsWith("ColA", "EndingString"), LessThanOrEqual("ColB", 5))
    val query = KustoFilter.pruneAndFilter(
      KustoSchema(testSchema, Set("ColA")),
      originalQuery,
      KustoFiltering(columns, filters))

    query shouldBe "MyTable | take 100 | where ['ColA'] endswith_cs 'EndingString' and ['ColB'] <= 5 | project tostring(['ColA']), ['ColB']"
  }
}

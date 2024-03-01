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

import com.microsoft.kusto.spark.utils.KustoQueryUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KustoQueryUtilsTest
    extends AnyFlatSpec
    with MockFactory
    with Matchers
    with BeforeAndAfterAll {

  "normalizeQuery" should "remove redundant query separator" in {
    val standardQuery = "Table | where column1 == 'abc';"

    KustoQueryUtils.normalizeQuery(standardQuery) should be("Table | where column1 == 'abc'")
  }

  "getQuerySchemaQuery" should "add suffix" in {
    val query = "Table | where column1 = 'abc' | summarize by count()"

    KustoQueryUtils.getQuerySchemaQuery(query) should be(
      "Table | where column1 = 'abc' | summarize by count()| take 0")
  }

  "limitQuery" should "add limit" in {
    val query = "Table | where column1 = 'abc' | summarize by count()"

    KustoQueryUtils.limitQuery(query, 5) should be(
      "Table | where column1 = 'abc' | summarize by count()| take 5")
  }
}

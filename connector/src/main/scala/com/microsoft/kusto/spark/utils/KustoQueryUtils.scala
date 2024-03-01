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

package com.microsoft.kusto.spark.utils

object KustoQueryUtils {

  def normalizeQuery(query: String): String = {
    val trimmedQuery = query.trim
    // We don't use concatenation of query statements, so no need in the semicolon separator
    if (trimmedQuery.endsWith(";")) trimmedQuery.dropRight(1) else trimmedQuery
  }

  def limitQuery(query: String, limit: Int): String = {
    query + s"| take $limit"
  }

  def getQuerySchemaQuery(query: String): String = {
    limitQuery(query, 0)
  }

  def isCommand(query: String): Boolean = query.trim.startsWith(".")

  def isQuery(query: String): Boolean = !isCommand(query)

  def simplifyName(name: String): String = {
    name.replaceAll("[^0-9a-zA-Z]", "_")
  }

  def normalizeTableName(table: String): String = {
    val tableName = table.replace("-", "_")

    if (tableName.startsWith("[")) {
      tableName
    } else if (!tableName.contains("'")) {
      "['" + tableName + "']"
    } else {
      "[\"" + tableName + "\"]"
    }
  }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

object KustoQueryUtils {

  def normalizeQuery(query: String): String = {
    val trimmedQuery = query.trim
    // We don't use concatenation of query statements, so no need in the semicolon separator
    if (trimmedQuery.endsWith(";")) trimmedQuery.dropRight(1) else trimmedQuery
  }

  def limitQuery(query: String, limit: Int): String = {
    // Append the operator on a new line so that a query ending with a single-line
    // comment (// ...) does not swallow the appended '| take' operator (issue #267).
    query + s"\n| take $limit"
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
      s"['$tableName']"
    } else {
      s"""["$tableName"]"""
    }
  }
}

package com.microsoft.kusto.spark.utils

object KustoQueryUtils {

  def normalizeQuery(query: String): String = {
    val trimedQuery = query.trim
    // We don't use concatenation of query statements, so no need in the semicolon separator
    if (trimedQuery.endsWith(";")) trimedQuery.dropRight(1) else trimedQuery
  }

  def limitQuery(query: String, limit: Int): String = {
    query + s"| take $limit"
  }

  def getQuerySchemaQuery(query: String): String = {
    limitQuery(query, 1)
  }

  def isCommand(query: String): Boolean = query.trim.startsWith(".")

  def isQuery(query: String): Boolean = !isCommand(query)

  def simplifyName(name: String): String = {
    name.replaceAll("-", "_").replaceAll("\\s", "")
  }

  def normalizeTableName(table: String): String = {
    val tableName = table.replace("-", "_")

    if (tableName.startsWith("[")) {
      tableName
    }
    else if (!tableName.contains("'")) {
      "['" + tableName + "']"
    }
    else {
      "[\"" + tableName + "\"]"
    }
  }
}

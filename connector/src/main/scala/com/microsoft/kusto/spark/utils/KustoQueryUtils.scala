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
    name.replaceAll("-", "_").replaceAll("\\.", "_").replaceAll("\\s", "_")
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

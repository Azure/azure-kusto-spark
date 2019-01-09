package com.microsoft.kusto.spark.utils

object KustoQueryUtils {
  private val getSchemaQuerySuffix = "| take 1;"

  def nomralizeQuery(query: String): String = {
    val trimedQuery = query.trim
    if (trimedQuery.endsWith(";")) trimedQuery else trimedQuery + ";"
  }

  def getQuerySchemaQuery(query: String): String = {
    nomralizeQuery(query).dropRight(1) + getSchemaQuerySuffix
  }

  def isCommand(query: String): Boolean = query.trim.startsWith(".")

  def isQuery(query: String): Boolean = !isCommand(query)

  def simplifyTableName(table: String): String = {
    table.replaceAll("-", "_").replaceAll("\\s", "")
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

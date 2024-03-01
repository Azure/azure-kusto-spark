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

package com.microsoft.kusto.spark.datasource

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

object KustoFilter {
  // Augment the original query to include column pruning and filtering
  def pruneAndFilter(
      kustoSchema: KustoSchema,
      originalQuery: String,
      filtering: KustoFiltering): String = {
    originalQuery + KustoFilter.buildFiltersClause(
      kustoSchema.sparkSchema,
      filtering.filters) + KustoFilter.buildColumnsClause(
      filtering.columns,
      kustoSchema.toStringCastedColumns)
  }

  def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = schema.fields.map(x => x.name -> x).toMap
    new StructType(columns.map(name => fieldMap(name)))
  }

  def buildColumnsClause(columns: Array[String], timespanColumns: Set[String]): String = {
    if (columns.isEmpty) ""
    else {
      " | project " + columns
        .map(col => if (timespanColumns.contains(col)) s"tostring(['$col'])" else s"['$col']")
        .mkString(", ")
    }
  }

  def buildFiltersClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterExpressions =
      filters.flatMap(f => buildFilterExpression(schema, f)).mkString(" and ")
    if (filterExpressions.isEmpty) "" else " | where " + filterExpressions
  }

  def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {

    filter match {
      case EqualTo(attr, value) => binaryScalarOperatorFilter(schema, attr, value, "==")
      case EqualNullSafe(attr, value) if value == null =>
        unaryScalarOperatorFilter(attr, "isnull")
      case EqualNullSafe(attr, value) => binaryScalarOperatorFilter(schema, attr, value, "==")
      case GreaterThan(attr, value) => binaryScalarOperatorFilter(schema, attr, value, ">")
      case GreaterThanOrEqual(attr, value) =>
        binaryScalarOperatorFilter(schema, attr, value, ">=")
      case LessThan(attr, value) => binaryScalarOperatorFilter(schema, attr, value, "<")
      case LessThanOrEqual(attr, value) => binaryScalarOperatorFilter(schema, attr, value, "<=")
      case In(attr, values) => unaryOperatorOnValueSetFilter(schema, attr, values, "in")
      case IsNull(attr) => unaryScalarOperatorFilter(attr, "isnull")
      case IsNotNull(attr) => unaryScalarOperatorFilter(attr, "isnotnull")
      case And(left, right) => binaryLogicalOperatorFilter(schema, left, right, "and")
      case Or(left, right) => binaryLogicalOperatorFilter(schema, left, right, "or")
      case Not(child) => unaryLogicalOperatorFilter(schema, child, "not")
      case StringStartsWith(attr, value) =>
        stringOperatorFilter(schema, attr, value, "startswith_cs")
      case StringEndsWith(attr, value) => stringOperatorFilter(schema, attr, value, "endswith_cs")
      case StringContains(attr, value) => stringOperatorFilter(schema, attr, value, "contains_cs")
      case _ => None
    }
  }

  private def binaryScalarOperatorFilter(
      schema: StructType,
      attr: String,
      value: Any,
      operator: String): Option[String] = {
    getType(schema, attr).map { dataType =>
      s"['$attr'] $operator ${format(value, dataType)}"
    }
  }

  private def unaryScalarOperatorFilter(attr: String, function: String): Option[String] = {
    Some(s"$function(['$attr'])")
  }

  private def binaryLogicalOperatorFilter(
      schema: StructType,
      leftFilter: Filter,
      rightFilter: Filter,
      operator: String): Option[String] = {
    buildFilterExpression(schema, leftFilter).flatMap { left =>
      buildFilterExpression(schema, rightFilter).map { right =>
        s"($left) $operator ($right)"
      }
    }
  }

  private def unaryLogicalOperatorFilter(
      schema: StructType,
      childFilter: Filter,
      operator: String): Option[String] = {
    buildFilterExpression(schema, childFilter).map(child => s"$operator($child)")
  }

  private def stringOperatorFilter(
      schema: StructType,
      attr: String,
      value: String,
      operator: String): Option[String] = {
    // Will return 'None' if 'attr' is not part of the 'schema'
    getType(schema, attr).map { _ =>
      s"""['$attr'] $operator '$value'"""
    }
  }

  private def toStringList(values: Array[Any], dataType: DataType): String = {
    values.map(value => format(value, dataType)).mkString(", ")
  }

  private def unaryOperatorOnValueSetFilter(
      schema: StructType,
      attr: String,
      value: Array[Any],
      operator: String): Option[String] = {
    getType(schema, attr).map { dataType =>
      s"['$attr'] $operator (${toStringList(value, dataType)})"
    }
  }

  private def format(value: Any, dataType: DataType): String = {
    dataType match {
      case StringType => s"'${value.toString.replace("\'", "\\'")}'"
      case DateType => s"datetime('${value.asInstanceOf[Date]}')"
      case TimestampType => s"datetime('${value.asInstanceOf[Timestamp]}')"
      case _ => value.toString
    }
  }

  private def getType(schema: StructType, attr: String): Option[DataType] = {
    if (schema.fieldNames.contains(attr)) {
      Some(schema(attr).dataType)
    } else None
  }
}

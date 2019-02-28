package com.microsoft.kusto.spark.datasource

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

private[kusto] object KustoFilter {
  // Augment the original query to include column pruning and filtering
  def pruneAndFilter(originalSchema: StructType, originalQuery: String, filtering: KustoFiltering): String = {
    var query = originalQuery

    if (!filtering.filters.isEmpty) {
      query += KustoFilter.buildFiltersClause(originalSchema, filtering.filters)
    }

    if (!filtering.columns.isEmpty) {
      query += KustoFilter.buildColumnsClause(filtering.columns)
    }

    query
  }

  def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

  def buildColumnsClause(columns: Array[String]): String = {
    if(columns.isEmpty) "" else {
      " | project " + columns.mkString(", ")
    }
  }

  def buildFiltersClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(schema, f)).mkString(" and ")
    if (filterExpressions.isEmpty) "" else " | where " + filterExpressions
  }

  def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {

    filter match {
      case EqualTo(attr, value) => binaryScalarOperatorFilter(schema, attr, value, "==")
      case EqualNullSafe(attr, value) if value == null => unaryScalarOperatorFilter(attr, "isnull")
      case EqualNullSafe(attr, value) => binaryScalarOperatorFilter(schema, attr, value, "==")
      case GreaterThan(attr, value) => binaryScalarOperatorFilter(schema, attr, value, ">")
      case GreaterThanOrEqual(attr, value) => binaryScalarOperatorFilter(schema, attr, value, ">=")
      case LessThan(attr, value) => binaryScalarOperatorFilter(schema, attr, value, "<")
      case LessThanOrEqual(attr, value) => binaryScalarOperatorFilter(schema, attr, value, "<=")
      case In(attr, values) => unaryOperatorOnValueSetFilter(schema, attr, values, "in")
      case IsNull(attr) => unaryScalarOperatorFilter(attr, "isnull")
      case IsNotNull(attr) => unaryScalarOperatorFilter(attr, "isnotnull")
      case And(left, right) => binaryLogicalOperatorFilter(schema, left, right, "and")
      case Or(left, right) => binaryLogicalOperatorFilter(schema, left, right, "or")
      case Not(child) => unaryLogicalOperatorFilter(schema, child, "not")
      case StringStartsWith(attr, value) => stringOperatorFilter(schema, attr, value, "startswith_cs")
      case StringEndsWith(attr, value) => stringOperatorFilter(schema, attr, value, "endswith_cs")
      case StringContains(attr, value) => stringOperatorFilter(schema, attr, value, "contains_cs")
      case _ => None
    }
  }

  private def binaryScalarOperatorFilter(schema: StructType, attr: String, value: Any, operator: String): Option[String] = {
    getType(schema, attr).map {
      dataType => s"$attr $operator ${format(value, dataType)}"
    }
  }

  private def unaryScalarOperatorFilter(attr: String, function: String): Option[String] = {
    Some(s"$function($attr)")
  }

  private def binaryLogicalOperatorFilter(schema: StructType, leftFilter: Filter, rightFilter: Filter, operator: String): Option[String] = {
    val left = buildFilterExpression(schema, leftFilter)
    val right = if(left.isEmpty) None else buildFilterExpression(schema, rightFilter)

    if (left.isEmpty || right.isEmpty) None else {
      Some(s"(${left.get}) $operator (${right.get})")
    }
  }

  private def unaryLogicalOperatorFilter(schema: StructType, childFilter: Filter, operator: String): Option[String] = {
    val child = buildFilterExpression(schema, childFilter)

    if (child.isEmpty) None else {
      Some(s"$operator(${child.get})")
    }
  }

  private  def stringOperatorFilter(schema: StructType, attr: String, value: String, operator: String): Option[String] = {
    // Will return 'None' if 'attr' is not part of the 'schema'
    getType(schema, attr).map {
      _ => s"""$attr $operator '$value'"""
    }
  }

  private def toStringList(values: Array[Any], dataType: DataType): String = {
    val combined = values.map(value => format(value, dataType)).mkString(", ")
    if (combined.isEmpty) "" else combined
  }

  private def unaryOperatorOnValueSetFilter(schema: StructType, attr: String, value: Array[Any], operator: String): Option[String] = {
    getType(schema, attr).map {
      dataType => s"$attr $operator (${toStringList(value, dataType)})"
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

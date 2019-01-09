package com.microsoft.kusto.spark.utils

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes._

object DataTypeMapping {
  val kustoTypeToSparkTypeMap: Map[String, DataType] = Map(
    "string" -> StringType,
    "int64" -> LongType,
    "datetime" -> DateType,
    "sbyte" -> BooleanType,
    "guid" -> StringType,
    "long" -> LongType,
    "real" -> DoubleType,
    "decimal" -> IntegerType,
    "int" -> IntegerType,
    "int32" -> IntegerType
  )

  val sparkTypeToKustoTypeMap: Map[DataType, String] = Map(
    StringType ->  "string",
    BooleanType -> "bool",
    DateType -> "datetime",
    TimestampType -> "datetime",
    CalendarIntervalType -> "timespan",
    DoubleType -> "real",
    FloatType -> "real",
    ByteType -> "int",
    IntegerType -> "int",
    LongType ->  "long",
    ShortType ->  "int"
  )
}

package com.microsoft.kusto.spark.utils

import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType, MapType, StructType}

object DataTypeMapping {

  val kustoTypeToSparkTypeMap: Map[String, DataType] = Map(
    "string" -> StringType,
    "long" -> LongType,
    "datetime" -> TimestampType,// Kusto datetime is equivalent to TimestampType
    "timespan" -> StringType,
    "bool" -> BooleanType,
    "real" -> DoubleType,
    // Can be partitioned differently between precision and scale, total must be 34 to match .Net SqlDecimal
    "decimal" -> DataTypes.createDecimalType(20,14),
    "guid" -> StringType,
    "int" -> IntegerType,
    "dynamic" -> StringType
  )

  val kustoJavaTypeToSparkTypeMap: Map[String, DataType] = Map(
    "string" -> StringType,
    "int64" -> LongType,
    "datetime" -> TimestampType,
    "timespan" -> StringType,
    "sbyte" -> BooleanType,
    "double" -> DoubleType,
    "sqldecimal" -> DataTypes.createDecimalType(20,14),
    "guid" -> StringType,
    "int32" -> IntegerType,
    "object" -> StringType
  )

  val sparkTypeToKustoTypeMap: Map[DataType, String] = Map(
    StringType ->  "string",
    BooleanType -> "bool",
    DateType -> "datetime",
    TimestampType -> "datetime",
    DataTypes.createDecimalType() -> "decimal",
    DoubleType -> "real",
    FloatType -> "real",
    ByteType -> "int",
    IntegerType -> "int",
    LongType ->  "long",
    ShortType ->  "int"
  )

  def getSparkTypeToKustoTypeMap(fieldType: DataType): String ={
      if(fieldType.isInstanceOf[DecimalType]) "decimal"
      else if (fieldType.isInstanceOf[ArrayType] || fieldType.isInstanceOf[StructType] || fieldType.isInstanceOf[MapType]) "dynamic"
      else DataTypeMapping.sparkTypeToKustoTypeMap.getOrElse(fieldType, "string")
  }
}

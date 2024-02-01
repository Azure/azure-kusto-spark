package com.microsoft.kusto.spark.utils

import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  DataTypes,
  DecimalType,
  MapType,
  StructType
}

object DataTypeMapping {

  val KustoTypeToSparkTypeMap: Map[String, DataType] = Map(
    "string" -> StringType,
    "long" -> LongType,
    "datetime" -> TimestampType, // Kusto datetime is equivalent to TimestampType
    "timespan" -> StringType,
    "bool" -> BooleanType,
    "real" -> DoubleType,

    /*
    Kusto uses floating decimal points and spark uses fixed decimal points. The compromise scenario is to use the system
    default of 38,18 used in the spark framework.https://github.com/apache/spark/blob
    /1439d9b275e844b5b595126bc97d2b44f6e859ed/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L131
     */
    "decimal" -> DecimalType.SYSTEM_DEFAULT,
    "guid" -> StringType,
    "int" -> IntegerType,
    "dynamic" -> StringType)

  val KustoJavaTypeToSparkTypeMap: Map[String, DataType] = Map(
    "string" -> StringType,
    "int64" -> LongType,
    "datetime" -> TimestampType,
    "timespan" -> StringType,
    "sbyte" -> BooleanType,
    "double" -> DoubleType,
    "sqldecimal" -> DecimalType.SYSTEM_DEFAULT,
    "guid" -> StringType,
    "int32" -> IntegerType,
    "object" -> StringType)

  val SparkTypeToKustoTypeMap: Map[DataType, String] = Map(
    StringType -> "string",
    BooleanType -> "bool",
    DateType -> "datetime",
    TimestampType -> "datetime",
    DataTypes.createDecimalType() -> "decimal",
    DoubleType -> "real",
    FloatType -> "real",
    ByteType -> "int",
    IntegerType -> "int",
    LongType -> "long",
    ShortType -> "int")

  def getSparkTypeToKustoTypeMap(fieldType: DataType): String = {
    if (fieldType.isInstanceOf[DecimalType]) "decimal"
    else if (fieldType.isInstanceOf[ArrayType] || fieldType.isInstanceOf[StructType] || fieldType
        .isInstanceOf[MapType]) "dynamic"
    else DataTypeMapping.SparkTypeToKustoTypeMap.getOrElse(fieldType, "string")
  }
}

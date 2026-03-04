// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  ByteType,
  DataType,
  DataTypes,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  ShortType,
  StringType,
  StructType,
  TimestampType
}

object DataTypeMapping {

  // Kusto type name constants
  private val KustoString = "string"
  private val KustoLong = "long"
  private val KustoDatetime = "datetime"
  private val KustoTimespan = "timespan"
  private val KustoBool = "bool"
  private val KustoReal = "real"
  private val KustoDecimal = "decimal"
  private val KustoGuid = "guid"
  private val KustoInt = "int"
  private val KustoDynamic = "dynamic"

  val KustoTypeToSparkTypeMap: Map[String, DataType] = Map(
    KustoString -> StringType,
    KustoLong -> LongType,
    KustoDatetime -> TimestampType, // Kusto datetime is equivalent to TimestampType
    KustoTimespan -> StringType,
    KustoBool -> BooleanType,
    KustoReal -> DoubleType,

    /*
    Kusto uses floating decimal points and spark uses fixed decimal points. The compromise scenario is to use the system
    default of 38,18 used in the spark framework.https://github.com/apache/spark/blob
    /1439d9b275e844b5b595126bc97d2b44f6e859ed/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L131
     */
    KustoDecimal -> DecimalType.SYSTEM_DEFAULT,
    KustoGuid -> StringType,
    KustoInt -> IntegerType,
    KustoDynamic -> StringType)

  val KustoJavaTypeToSparkTypeMap: Map[String, DataType] = Map(
    KustoString -> StringType,
    "int64" -> LongType,
    KustoDatetime -> TimestampType,
    KustoTimespan -> StringType,
    "sbyte" -> BooleanType,
    "double" -> DoubleType,
    "sqldecimal" -> DecimalType.SYSTEM_DEFAULT,
    KustoGuid -> StringType,
    "int32" -> IntegerType,
    "object" -> StringType)

  val SparkTypeToKustoTypeMap: Map[DataType, String] = Map(
    StringType -> KustoString,
    BooleanType -> KustoBool,
    DateType -> KustoDatetime,
    TimestampType -> KustoDatetime,
    DataTypes.createDecimalType() -> KustoDecimal,
    DoubleType -> KustoReal,
    FloatType -> KustoReal,
    ByteType -> KustoInt,
    IntegerType -> KustoInt,
    LongType -> KustoLong,
    ShortType -> KustoInt)

  def getSparkTypeToKustoTypeMap(fieldType: DataType): String = {
    if (fieldType.isInstanceOf[DecimalType]) {
      KustoDecimal
    } else if (fieldType.isInstanceOf[ArrayType] || fieldType
        .isInstanceOf[StructType] || fieldType
        .isInstanceOf[MapType]) {
      KustoDynamic
    } else {
      DataTypeMapping.SparkTypeToKustoTypeMap.getOrElse(fieldType, KustoString)
    }
  }
}

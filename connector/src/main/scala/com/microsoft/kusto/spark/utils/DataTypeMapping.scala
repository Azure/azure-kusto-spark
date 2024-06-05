// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DataType,
  DataTypes, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType,
  MapType, ShortType, StringType, StructType, TimestampType}

object DataTypeMapping {

  private val STRING_TYPE = "string"
  private val DT_TM_TYPE = "datetime"
  private val REAL_TYPE = "real"
  private val DECIMAL_TYPE = "decimal"
  private val INT_TYPE = "int"
  private val DYNAMIC_TYPE = "dynamic"
  private val LONG_TYPE = "long"
  private val BOOLEAN_TYPE = "bool"

  val KustoTypeToSparkTypeMap: Map[String, DataType] = Map(
    STRING_TYPE -> StringType,
    LONG_TYPE -> LongType,
    DT_TM_TYPE -> TimestampType, // Kusto datetime is equivalent to TimestampType
    "timespan" -> StringType,
    BOOLEAN_TYPE -> BooleanType,
    REAL_TYPE -> DoubleType,

    /*
    Kusto uses floating decimal points and spark uses fixed decimal points. The compromise scenario is to use the system
    default of 38,18 used in the spark framework.https://github.com/apache/spark/blob
    /1439d9b275e844b5b595126bc97d2b44f6e859ed/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L131
     */
    DECIMAL_TYPE -> DecimalType.SYSTEM_DEFAULT,
    "guid" -> StringType,
    INT_TYPE -> IntegerType,
    DYNAMIC_TYPE -> StringType)

  val SparkTypeToKustoTypeMap: Map[DataType, String] = Map(
    StringType -> STRING_TYPE,
    BooleanType -> BOOLEAN_TYPE,
    DateType -> DT_TM_TYPE,
    TimestampType -> DT_TM_TYPE,
    DataTypes.createDecimalType() -> DECIMAL_TYPE,
    DoubleType -> REAL_TYPE,
    FloatType -> REAL_TYPE,
    ByteType -> INT_TYPE,
    IntegerType -> INT_TYPE,
    LongType -> LONG_TYPE,
    ShortType -> INT_TYPE)

  def getSparkTypeToKustoTypeMap(fieldType: DataType): String = {
    if (fieldType.isInstanceOf[DecimalType]) {
      DECIMAL_TYPE
    }
    else if (fieldType.isInstanceOf[ArrayType] || fieldType.isInstanceOf[StructType] || fieldType
        .isInstanceOf[MapType]) {
      DYNAMIC_TYPE
    }
    else {
      DataTypeMapping.SparkTypeToKustoTypeMap.getOrElse(fieldType, STRING_TYPE)
    }
  }
}

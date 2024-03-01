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

import com.microsoft.azure.kusto.data.KustoResultSetTable
import com.microsoft.kusto.spark.utils.DataTypeMapping
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructType, _}

import java.sql.Timestamp
import java.time.Instant
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

object KustoResponseDeserializer {
  def apply(kustoResult: KustoResultSetTable): KustoResponseDeserializer =
    new KustoResponseDeserializer(kustoResult)
}

// Timespan columns are casted to strings in kusto side. A simple test to compare the translation to a Duration string
// in the format of timespan resulted in less performance. One way was using a new expression that extends UnaryExpression,
// second was by a udf function, both were less performant.
case class KustoSchema(sparkSchema: StructType, toStringCastedColumns: Set[String])

class KustoResponseDeserializer(val kustoResult: KustoResultSetTable) {
  val schema: KustoSchema = getSchemaFromKustoResult

  private def getValueTransformer(valueType: String): Any => Any = {

    valueType.toLowerCase() match {
      case "string" => value: Any => value
      case "int64" => value: Any => value
      case "datetime" => value: Any => Timestamp.from(Instant.parse(value.toString))
      case "timespan" => value: Any => value
      case "sbyte" => value: Any => value
      case "long" => {
        case i: Int => i.toLong
        case value => value.asInstanceOf[Long]
      }
      case "double" => value: Any => value
      case "decimal" => value: Any => BigDecimal(value.asInstanceOf[String])
      case "int" => value: Any => value
      case "int32" => value: Any => value
      case "bool" => value: Any => value
      case "real" => {
        case v: Int => v.toDouble
        case v: Long => v.toDouble
        case v: java.math.BigDecimal => v.doubleValue()
        case v => v.asInstanceOf[Double]
      }
      case _ => value: Any => value.toString
    }
  }

  private def getSchemaFromKustoResult: KustoSchema = {
    if (kustoResult.getColumns.isEmpty) {
      KustoSchema(StructType(List()), Set())
    } else {
      val columns = kustoResult.getColumns

      KustoSchema(
        StructType(
          columns.map(col =>
            StructField(
              col.getColumnName,
              DataTypeMapping.KustoTypeToSparkTypeMap
                .getOrElse(col.getColumnType.toLowerCase, StringType)))),
        columns
          .filter(c => c.getColumnType.equalsIgnoreCase("TimeSpan"))
          .map(c => c.getColumnName)
          .toSet)
    }
  }

  def getSchema: KustoSchema = { schema }

  def toRows: java.util.List[Row] = {
    val columnInOrder = kustoResult.getColumns
    val value: util.ArrayList[Row] = new util.ArrayList[Row](kustoResult.count())

//     Calculate the transformer function for each column to use later by order
    val valueTransformers: mutable.Seq[Any => Any] =
      columnInOrder.map(col => getValueTransformer(col.getColumnType))
    kustoResult.getData.asScala.foreach(row => {
      val genericRow = row
        .toArray()
        .zipWithIndex
        .map(column => {
          if (column._1 == null) null else valueTransformers(column._2)(column._1)
        })
      value.add(new GenericRowWithSchema(genericRow, schema.sparkSchema))
    })

    value
  }

//  private def getOrderedColumnName = {
//    val columnInOrder = ArrayBuffer.fill(kustoResult.getColumnNameToIndex.size()){ "" }
//    kustoResult.getColumns.foreach((columnIndexPair: KustoResultColumn) => columnInOrder(columnIndexPair.) = columnIndexPair._1)
//    columnInOrder
//  }
}

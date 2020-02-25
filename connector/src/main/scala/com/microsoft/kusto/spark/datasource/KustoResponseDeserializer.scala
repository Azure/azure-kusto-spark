package com.microsoft.kusto.spark.datasource

import java.sql.Timestamp
import java.util

import com.microsoft.azure.kusto.data.Results
import com.microsoft.kusto.spark.utils.DataTypeMapping
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructType, _}
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object KustoResponseDeserializer {
  def apply(kustoResult: Results): KustoResponseDeserializer = new KustoResponseDeserializer(kustoResult)
}

// Timespan columns are casted to strings in kusto side. A simple test to compare the translation to a Duration string
// in the format of timespan resulted in less performance. One way was using a new expression that extends UnaryExpression,
// second was by a udf function, both were less performant.
case class KustoSchema(sparkSchema: StructType, timespanColumns: Set[String])

class KustoResponseDeserializer(val kustoResult: Results) {
  val schema: KustoSchema = getSchemaFromKustoResult

  private def getValueTransformer(valueType: String): String => Any = {

    valueType.toLowerCase() match {
      case "string" => value: String => value
      case "int64" => value: String => value.toLong
      case "datetime" =>value: String => new Timestamp(new DateTime(value).getMillis)
      case "timespan" => value: String => value
      case "sbyte" => value: String => value.toBoolean
      case "long" => value: String => value.toLong
      case "double" => value: String => value.toDouble
      case "sqldecimal" => value: String => BigDecimal(value)
      case "int" => value: String => value.toInt
      case "int32" => value: String => value.toInt
      case _ => value: String => value.toString
      }
  }

   private def getSchemaFromKustoResult: KustoSchema = {
    if (kustoResult.getColumnNameToType.isEmpty) {
      KustoSchema(StructType(List()), Set())
    } else {
      val columnInOrder = this.getOrderedColumnName

      val columnNameToType = kustoResult.getColumnNameToType

      KustoSchema(StructType(columnInOrder.map(key => StructField(key,
            DataTypeMapping.kustoJavaTypeToSparkTypeMap.getOrElse(columnNameToType.get(key).toLowerCase, StringType)))),
        columnNameToType.asScala.filter(c => c._2 == "TimeSpan").keys.toSet)
    }
  }

  def getSchema: KustoSchema = { schema }

  def toRows: java.util.List[Row] = {
    val columnInOrder = this.getOrderedColumnName
    val value: util.ArrayList[Row] = new util.ArrayList[Row](kustoResult.getValues.size)

    // Calculate the transformer function for each column to use later by order
    val valueTransformers: mutable.Seq[String => Any] = columnInOrder.map(col => getValueTransformer(kustoResult.getTypeByColumnName(col)))
    kustoResult.getValues.toArray().foreach(row => {
      val genericRow = row.asInstanceOf[util.ArrayList[String]].toArray().zipWithIndex.map(
        column => if (column._1== null) null else valueTransformers(column._2)(column._1.asInstanceOf[String])
      )
      value.add(new GenericRowWithSchema(genericRow, schema.sparkSchema))
    })

    value
  }

  private def getOrderedColumnName = {
    val columnInOrder = ArrayBuffer.fill(kustoResult.getColumnNameToIndex.size()){ "" }
    kustoResult.getColumnNameToIndex.asScala.foreach(columnIndexPair => columnInOrder(columnIndexPair._2) = columnIndexPair._1)
    columnInOrder
  }
}

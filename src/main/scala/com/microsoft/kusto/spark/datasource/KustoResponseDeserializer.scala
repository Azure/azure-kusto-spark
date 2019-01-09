package com.microsoft.kusto.spark.datasource

import java.util

import com.microsoft.azure.kusto.data.Results
import com.microsoft.kusto.spark.utils.DataTypeMapping
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructType, _}
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object KustoResponseDeserializer {
  def apply(kustoResult: Results): KustoResponseDeserializer = new KustoResponseDeserializer(kustoResult)
}

class KustoResponseDeserializer(val kustoResult: Results) {
  val schema: StructType = getSchemaFromKustoResult

  private def valueTransformer(value: String, valueType: String): Any = {
    valueType.toLowerCase() match {
      case "string" => value
      case "int64" => value.toLong
      case "datetime" => new DateTime(value)
      case "sbyte" => value.toBoolean
      case "long" => value.toLong
      case "real" => value.toDouble
      case "decimal" => value.toInt
      case "int" => value.toInt
      case "int32" => value.toInt
      case _ => value.toString
    }
  }

   private def getSchemaFromKustoResult: StructType = {
    if (kustoResult.getColumnNameToType.isEmpty) {
      StructType(List())
    } else {
      val columnInOrder = this.getOrderedColumnName

      val columnNameToType = kustoResult.getColumnNameToType
      StructType(
        columnInOrder
          .map(key => StructField(key, DataTypeMapping.kustoTypeToSparkTypeMap.getOrElse(columnNameToType.get(key).toLowerCase, StringType))))
    }
  }

  def getSchema: StructType = { schema }

  def toRows: java.util.List[Row] = {
    val columnInOrder = this.getOrderedColumnName
    var value: List[Row] = List()
    kustoResult.getValues.toArray().foreach(row => {
      val genericRow = row.asInstanceOf[util.ArrayList[String]].toArray().zipWithIndex.map(
        column => this.valueTransformer(column._1.asInstanceOf[String], kustoResult.getTypeByColumnName(columnInOrder(column._2)))
      )
      value :+= new GenericRowWithSchema(genericRow, schema)
    })
    value.asJava
  }

  private def getOrderedColumnName = {
    val columnInOrder = ArrayBuffer.fill(kustoResult.getColumnNameToIndex.size()){ "" }
    kustoResult.getColumnNameToIndex.asScala.foreach(columnIndexPair => columnInOrder(columnIndexPair._2) = columnIndexPair._1)
    columnInOrder
  }
}

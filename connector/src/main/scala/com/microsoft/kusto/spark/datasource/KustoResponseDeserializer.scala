package com.microsoft.kusto.spark.datasource

import java.sql.Timestamp
import java.util

import com.microsoft.azure.kusto.data.{KustoResultColumn, KustoResultSetTable, Results}
import com.microsoft.kusto.spark.utils.DataTypeMapping
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructType, _}
import java.time.Instant

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object KustoResponseDeserializer {
  def apply(kustoResult: KustoResultSetTable): KustoResponseDeserializer = new KustoResponseDeserializer(kustoResult)
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
      case "long" => value: Any => value match {
        case i: Int => i.toLong
        case _ => value.asInstanceOf[Long]
      }
      case "double" => value: Any => value
      case "decimal" => value: Any => BigDecimal(value.asInstanceOf[String])
      case "int" => value: Any => value
      case "int32" => value: Any => value
      case "bool" => value: Any => value
      case "real" => value: Any => value
      case _ => value: Any => value.toString
      }
  }

   private def getSchemaFromKustoResult: KustoSchema = {
    if (kustoResult.getColumns.isEmpty) {
      KustoSchema(StructType(List()), Set())
    } else {
      val columns = kustoResult.getColumns

      KustoSchema(StructType(columns.map(col => StructField(col.getColumnName,
            DataTypeMapping.KustoTypeToSparkTypeMap.getOrElse(col.getColumnType.toLowerCase, StringType)))),
        columns.filter(c => c.getColumnType.equalsIgnoreCase("TimeSpan")).map(c => c.getColumnName).toSet)
    }
  }

  def getSchema: KustoSchema = { schema }

  def toRows: java.util.List[Row] = {
    val columnInOrder = kustoResult.getColumns
    val value: util.ArrayList[Row] = new util.ArrayList[Row](kustoResult.count())

//     Calculate the transformer function for each column to use later by order
    val valueTransformers: mutable.Seq[Any => Any] = columnInOrder.map(col => getValueTransformer(col.getColumnType))
    kustoResult.getData.asScala.foreach(row => {
      val genericRow = row.toArray().zipWithIndex.map(
        column => {
          println(s"------------->>>>>>>>>> column : ${column._1} : ${column._2}")
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

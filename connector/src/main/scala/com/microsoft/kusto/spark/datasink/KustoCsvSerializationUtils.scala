package com.microsoft.kusto.spark.datasink

import com.fasterxml.jackson.databind.ObjectMapper
import java.util.TimeZone

import com.microsoft.kusto.spark.utils.DataTypeMapping
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.StructType

private[kusto] class KustoCsvSerializationUtils(val schema: StructType, timeZone: String) {
  private[kusto] val DateFormat =
    FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", TimeZone.getTimeZone(timeZone))
  private[kusto] def convertRow(row: InternalRow) = {
    val values = new Array[String](row.numFields)
    for (i <- 0 until row.numFields if !row.isNullAt(i)) {
      val dataType = schema.fields(i).dataType
      values(i) = dataType match {
        case DateType => DateTimeUtils.toJavaDate(row.getInt(i)).toString
        case TimestampType => DateFormat.format(DateTimeUtils.toJavaTimestamp(row.getLong(i)))
        case _ => row.get(i, dataType).toString
      }
    }

    values
  }
}

private[kusto] object KustoCsvMapper {
  import org.apache.spark.sql.types.StructType

  def createCsvMapping(schema: StructType): String = {
    val objectMapper = new ObjectMapper();
    val csvMapping = objectMapper.createArrayNode()

    for (i <- 0 until schema.length) {
      val field = schema.apply(i)
      val dataType = field.dataType
      val mapping = objectMapper.createObjectNode
      mapping.put("Name", field.name)
      mapping.put("Ordinal", i)
      mapping.put(
        "DataType",
        DataTypeMapping.SparkTypeToKustoTypeMap.getOrElse(dataType, StringType).toString)
      csvMapping.add(mapping)
    }

    csvMapping.toString
  }
}

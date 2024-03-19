package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.datasink.KustoWriter.className

import java.io.{ByteArrayOutputStream, CharArrayWriter, PrintStream}
import java.math.BigInteger
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDateTime, ZoneId}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import com.microsoft.kusto.spark.utils.{KustoConstants => KCONST, KustoDataSourceUtils => KDSU}

object RowCSVWriterUtils {
  def writeRowAsCSV(
      row: InternalRow,
      schema: StructType,
      timeZone: ZoneId,
      writer: Writer): Unit = {
    val schemaFields: Array[StructField] = schema.fields

    if (!row.isNullAt(0)) {
      writeField(
        row,
        fieldIndexInRow = 0,
        schemaFields(0).dataType,
        timeZone,
        writer,
        nested = false)
    }

    for (i <- 1 until row.numFields) {
      writer.write(',')
      if (!row.isNullAt(i)) {
        writeField(row, i, schemaFields(i).dataType, timeZone, writer, nested = false)
      }
    }

    writer.newLine()
  }

  private def writeJsonField(json: String, writer: Writer, nested: Boolean): Unit = {
    if (nested) {
      writer.writeUnescaped(json)
    } else {
      writer.writeStringField(json)
    }
  }

  private def getLocalDateTimeFromTimestampWithZone(timestamp: Long, timeZone: ZoneId) = {
    LocalDateTime.ofInstant(Instant.EPOCH.plus(timestamp, ChronoUnit.MICROS), timeZone)
  }

  // This method does not check for null at the current row idx and should be checked before !
  private def writeField(
      row: SpecializedGetters,
      fieldIndexInRow: Int,
      dataType: DataType,
      timeZone: ZoneId,
      writer: Writer,
      nested: Boolean): Unit = {
    dataType match {
      case StringType => writeStringFromUTF8(row.get(fieldIndexInRow, StringType), writer)
      case DateType =>
        writer.writeStringField(DateTimeUtils.toJavaDate(row.getInt(fieldIndexInRow)).toString)
      case TimestampType =>
        writer.writeStringField(
          getLocalDateTimeFromTimestampWithZone(row.getLong(fieldIndexInRow), timeZone).toString)
      case BooleanType => writer.write(row.getBoolean(fieldIndexInRow).toString)
      case structType: StructType =>
        writeJsonField(
          convertStructToJson(
            row.getStruct(fieldIndexInRow, structType.length),
            structType,
            timeZone),
          writer,
          nested)
      case arrType: ArrayType =>
        writeJsonField(
          convertArrayToJson(row.getArray(fieldIndexInRow), arrType.elementType, timeZone),
          writer,
          nested)
      case mapType: MapType =>
        writeJsonField(
          convertMapToJson(row.getMap(fieldIndexInRow), mapType, timeZone),
          writer,
          nested)
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
        writer.write(row.get(fieldIndexInRow, dataType).toString)
      case decimalType: DecimalType =>
        writeDecimalField(row, fieldIndexInRow, decimalType.precision, decimalType.scale, writer)
      case _ => writer.writeStringField(row.get(fieldIndexInRow, dataType).toString)
    }
  }

  private def convertStructToJson(
      row: InternalRow,
      schema: StructType,
      timeZone: ZoneId): String = {
    val fields = schema.fields
    if (fields.length != 0) {
      val writer = EscapedWriter(new CharArrayWriter())
      writer.write('{')

      var x = 0
      var isNullOrEmpty = true
      while (x < fields.length && isNullOrEmpty) {
        isNullOrEmpty = row.isNullAt(x)
        if (!isNullOrEmpty) {
          writeStructField(x)
        }
        x += 1
      }

      while (x < fields.length) {
        if (!row.isNullAt(x)) {
          writer.write(',')
          writeStructField(x)
        }
        x += 1
      }
      writer.write('}')

      def writeStructField(idx: Int): Unit = {
        writer.writeStringField(fields(idx).name)
        writer.write(':')
        writeField(row, idx, fields(idx).dataType, timeZone, writer, nested = true)
      }

      writer.out.toString
    } else {
      "{}"
    }
  }

  private def convertArrayToJson(
      ar: ArrayData,
      fieldsType: DataType,
      timeZone: ZoneId): String = {
    if (ar.numElements() == 0) "[]"
    else {
      val writer = EscapedWriter(new CharArrayWriter())

      writer.write('[')
      if (ar.isNullAt(0)) writer.write("null")
      else writeField(ar, fieldIndexInRow = 0, fieldsType, timeZone, writer, nested = true)
      for (x <- 1 until ar.numElements()) {
        writer.write(',')
        if (ar.isNullAt(x)) writer.write("null")
        else writeField(ar, x, fieldsType, timeZone, writer, nested = true)
      }
      writer.write(']')

      writer.out.toString
    }
  }

  private def convertMapToJson(map: MapData, fieldsType: MapType, timeZone: ZoneId): String = {
    val keys = map.keyArray()
    val values = map.valueArray()

    val writer = EscapedWriter(new CharArrayWriter())

    writer.write('{')

    var x = 0
    var isNull = true
    while (x < map.keyArray().numElements() && isNull) {
      isNull = values.isNullAt(x)
      if (!isNull) {
        writeMapField(x)
      }
      x += 1
    }

    while (x < map.keyArray().numElements()) {
      if (!values.isNullAt(x)) {
        writer.write(',')
        writeMapField(x)
      }
      x += 1
    }

    writer.write('}')

    def writeMapField(idx: Int): Unit = {
      writeField(
        keys,
        fieldIndexInRow = idx,
        dataType = fieldsType.keyType,
        timeZone = timeZone,
        writer,
        nested = true)
      writer.write(':')
      writeField(
        values,
        fieldIndexInRow = idx,
        dataType = fieldsType.valueType,
        timeZone = timeZone,
        writer = writer,
        nested = true)
    }
    writer.out.toString
  }

  private def writeDecimalField(
      row: SpecializedGetters,
      fieldIndexInRow: Int,
      precision: Int,
      scale: Int,
      writer: Writer): Unit = {
    writer.write('"')
    val (numStr: String, negative: Boolean) = if (precision <= Decimal.MAX_LONG_DIGITS) {
      val num: Long = row.getLong(fieldIndexInRow)
      (num.abs.toString, num < 0)
    } else {
      val bytes = row.getBinary(fieldIndexInRow)
      val num = new BigInteger(bytes)
      (num.abs.toString, num.signum() < 0)
    }

    // Get string representation without scientific notation
    var point = numStr.length - scale
    if (negative) {
      writer.write("-")
    }
    if (point <= 0) {
      writer.write('0')
      writer.write('.')
      while (point < 0) {
        writer.write('0')
        point += 1
      }
      writer.write(numStr)
    } else {
      for (i <- 0 until numStr.length) {
        if (point == i) {
          writer.write('.')
        }
        writer.write(numStr(i))
      }
    }

    writer.write('"')
  }

  private def writeStringFromUTF8(str: Object, writer: Writer): Unit = {
    writer.writeStringField(str.toString)
  }
}

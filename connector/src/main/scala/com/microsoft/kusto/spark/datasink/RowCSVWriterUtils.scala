package com.microsoft.kusto.spark.datasink

import java.io.CharArrayWriter
import java.math.BigInteger

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object RowCSVWriterUtils {
  def writeRowAsCSV(row: InternalRow, schema: StructType, dateFormat: FastDateFormat, writer: CountingWriter): Unit = {
    val schemaFields: Array[StructField] = schema.fields

    if (!row.isNullAt(0)) {
      writeField(row, fieldIndexInRow = 0, schemaFields(0).dataType, dateFormat, writer, nested = false)
    }

    for (i <- 1 until row.numFields) {
      writer.write(',')
      if (!row.isNullAt(i)) {
        writeField(row, i, schemaFields(i).dataType, dateFormat, writer, nested = false)
      }
    }

    writer.newLine()
  }

  def writeJsonField(json: String, writer: Writer, nested: Boolean): Unit = {
    if (nested) {
      writer.writeUnescaped(json)
    } else {
      writer.writeStringField(json)
    }
  }

  // This method does not check for null at the current row idx and should be checked before !
  private def writeField(row: SpecializedGetters, fieldIndexInRow: Int, dataType: DataType, dateFormat: FastDateFormat, writer: Writer, nested: Boolean): Unit = {
    dataType match {
      case StringType => writeStringFromUTF8(row.getUTF8String(fieldIndexInRow), writer)
      case DateType => writer.writeStringField(DateTimeUtils.toJavaDate(row.getInt(fieldIndexInRow)).toString)
      case TimestampType => writer.writeStringField(dateFormat.format(DateTimeUtils.toJavaTimestamp(row.getLong(fieldIndexInRow))))
      case BooleanType => writer.write(row.getBoolean(fieldIndexInRow).toString)
      case structType: StructType => writeJsonField(convertStructToJson(row.getStruct(fieldIndexInRow, structType.length), structType, dateFormat), writer, nested)
      case arrType: ArrayType => writeJsonField(convertArrayToJson(row.getArray(fieldIndexInRow), arrType.elementType, dateFormat), writer, nested)
      case mapType: MapType => writeJsonField(convertMapToJson(row.getMap(fieldIndexInRow), mapType, dateFormat), writer, nested)
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
        writer.write(row.get(fieldIndexInRow, dataType).toString)
      case decimalType: DecimalType => writeDecimalField(row, fieldIndexInRow, decimalType.precision, decimalType.scale, writer)
      case _ => writer.writeStringField(row.get(fieldIndexInRow, dataType).toString)
    }
  }

  private def convertStructToJson(row: InternalRow, schema: StructType, dateFormat: FastDateFormat): String = {
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
        if (!row.isNullAt(x)){
          writer.write(',')
          writeStructField(x)
        }
        x += 1
      }
      writer.write('}')

      def writeStructField(idx: Int): Unit = {
        writer.writeStringField(fields(idx).name)
        writer.write(':')
        writeField(row, idx, fields(idx).dataType, dateFormat, writer, nested = true)
      }

      writer.out.toString
    } else {
      "{}"
    }
  }

  private def convertArrayToJson(ar: ArrayData, fieldsType: DataType, dateFormat: FastDateFormat): String = {
    if (ar.numElements() == 0) "[]" else {
      val writer = EscapedWriter(new CharArrayWriter())

      writer.write('[')
      if (ar.isNullAt(0)) writer.write("null") else writeField(ar, fieldIndexInRow = 0, fieldsType, dateFormat, writer, nested = true)
      for (x <- 1 until ar.numElements()) {
        writer.write(',')
        if (ar.isNullAt(x)) writer.write("null") else writeField(ar, x, fieldsType, dateFormat, writer, nested = true)
      }
      writer.write(']')

      writer.out.toString
    }
  }

  private def convertMapToJson(map: MapData, fieldsType: MapType, dateFormat: FastDateFormat): String = {
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
      writeField(keys, fieldIndexInRow = idx, dataType = fieldsType.keyType, dateFormat = dateFormat, writer, nested = true)
      writer.write(':')
      writeField(values, fieldIndexInRow = idx, dataType = fieldsType.valueType, dateFormat = dateFormat, writer = writer, nested = true)
    }
    writer.out.toString
  }

  private def writeDecimalField(row: SpecializedGetters, fieldIndexInRow: Int, precision: Int,scale: Int, writer: Writer): Unit = {
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
    if (negative){
      writer.write("-")
    }
    if (point <= 0){
      writer.write('0')
      writer.write('.')
      while(point < 0){
        writer.write('0')
        point += 1
      }
      writer.write(numStr)
    } else {
      for (i <- 0 until numStr.length){
        if (point == i) {
          writer.write('.')
        }
        writer.write(numStr(i))
      }
    }

    writer.write('"')
  }

  private def writeStringFromUTF8(str: UTF8String, writer: Writer): Unit = {
    writer.writeStringField(str.toString)
  }
}

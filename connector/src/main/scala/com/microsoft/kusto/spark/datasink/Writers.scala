package com.microsoft.kusto.spark.datasink

trait Writer {
  val out: java.io.Writer
  def write(c: Char): Unit
  def write(str: String): Unit
  def writeStringField(str: String): Unit
  def writeUnescaped(str: String): Unit = {
    out.write(str)
  }
}

case class CountingWriter(out: java.io.Writer) extends Writer {
  private val newLineSep: String = System.lineSeparator()
//    java.security.AccessController.doPrivileged(
//    new sun.security.action.GetPropertyAction("line.separator"))
  private val newLineSepLength: Int = newLineSep.length
  private var bytesCounter: Long = 0L

  def newLine(): Unit = {
    out.write(newLineSep)
    bytesCounter += newLineSepLength
  }

  def write(c: Char): Unit ={
    out.write(c)
    bytesCounter += 1
  }
  def write(str: String): Unit = {
    out.write(str)
    bytesCounter += str.length
  }

  def writeStringField(str: String): Unit = {
    if (str.nonEmpty) {
      out.write('"')
      bytesCounter += 2
      for (c <- str) {
        if (c == '"') {
          out.write("\"\"")
          bytesCounter += 1
        } else {
          out.write(c)
        }
      }
      out.write('"')
      bytesCounter += str.length
    }
  }

  def getCounter: Long = bytesCounter

  def resetCounter(): Unit = {
    bytesCounter = 0
  }
}

case class EscapedWriter(out: java.io.Writer) extends Writer {
  def write(c: Char): Unit ={
    out.write(c)
  }

  def write(str: String): Unit ={
    for (c <- str) {
      val escaped =  if (c > 127) 0 else EscapedWriter.escapeTable(c)
      if (escaped != 0) {
        out.write('\\')
        out.write(escaped)
      } else {
        out.write(c)
      }
    }
  }

  def writeStringField(str: String): Unit = {
    out.write('"')
    write(str)
    out.write('"')
  }
}

object EscapedWriter {
  val escapeTable: Array[Int] = Array.fill[Int](128)(0)
  escapeTable('"') = '"'
  escapeTable('\\') = '\\'
  escapeTable('\n') = 'n'
  escapeTable('\r') = 'r'
  escapeTable('\b') = 'b'
  escapeTable('\t') = 't'
  escapeTable('\f') = 'f'
}

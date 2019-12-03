package com.microsoft.kusto.spark.datasink

import java.io.Writer

case class CountingCsvWriter(out: Writer) {
  val newLineSep: String = java.security.AccessController.doPrivileged(
    new sun.security.action.GetPropertyAction("line.separator"))
  val newLineSepLength: Int = newLineSep.length
  var bytesCounter: Long = 0L

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

  def writeStringField(str: String, nested: Boolean) {
    if (str.length > 0) {

      out.write('"')
      if(nested){
        out.write('"')
        bytesCounter += 2
      }

      bytesCounter += 2

      // Json does not allow special characters
      if(nested){
        for (c <- str) {
          if (c == '\n') {
            out.write("\\n")
            bytesCounter += 1
          } else if ( c == '\r'){
            out.write("\\r")
            bytesCounter += 1
          } else if (c == '\b'){
            out.write("\\b")
            bytesCounter += 1
          } else if ( c == '\f') {
            out.write("\\f")
            bytesCounter += 1
          } else if (c == '\t'){
            out.write("\\t")
            bytesCounter += 1
          }
          else if (c == '"') {
            out.write("\\\"\"")
            bytesCounter += 1
          } else {
            out.write(c)
          }
        }
      } else {
        for (c <- str) {
          if (c == '"') {
            out.write("\"\"")
            bytesCounter += 1
          } else {
            out.write(c)
          }
        }
      }

      out.write('"')
      if(nested){
        out.write('"')
      }

      bytesCounter += str.length
    }
  }

  def getCounter: Long = bytesCounter

  def resetCounter(): Unit = {
    bytesCounter = 0
  }

}

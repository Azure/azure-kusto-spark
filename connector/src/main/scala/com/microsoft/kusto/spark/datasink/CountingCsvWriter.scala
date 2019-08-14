package com.microsoft.kusto.spark.datasink

import java.io.Writer

case class CountingCsvWriter(out: Writer) {
  val newLineSep: String = java.security.AccessController.doPrivileged(
    new sun.security.action.GetPropertyAction("line.separator"))
  val newLineSepLength: Int = newLineSep.length
  var bytsCounter: Long = 0L

  def newLine(): Unit = {
    out.write(newLineSep)
    bytsCounter += newLineSepLength
  }

  def write(str: String): Unit = {
    out.write(str)
    bytsCounter += str.length
  }

  def writeStringField(str: String, nested: Boolean) {
    if (str.length > 0) {
      if(!nested) {
        out.write("\"")
        bytsCounter += 2
      }

      for (c <- str) {
        if (c == '"') {
          out.write("\"\"")
          bytsCounter += 1
        }
        else {
          out.write(c)
        }
      }
    }

    if(!nested) {
      out.write("\"")
    }

    bytsCounter += str.length
  }

  def getProgress: Long = bytsCounter

  def resetProgress(): Unit = {
    bytsCounter = 0
  }

}

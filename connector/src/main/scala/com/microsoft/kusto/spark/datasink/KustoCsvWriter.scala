package com.microsoft.kusto.spark.datasink

import java.io.Writer

case class KustoCsvWriter(out: Writer) {
  val newLineSep: String = java.security.AccessController.doPrivileged(
    new sun.security.action.GetPropertyAction("line.separator"))
  val newLineSepLength: Int = newLineSep.length
  var progress: Long = 0L

  def newLine(): Unit = {
    out.write(newLineSep)
    progress += newLineSepLength
  }

  def write(str: String): Unit = {
    out.write(str)
    progress += str.length
  }

  def writeStringField(str: String, nested: Boolean) {
    if (str.length > 0) {
      if(!nested) {
        out.write("\"")
        progress += 2
      }

      for (c <- str) {
        if (c == '"') {
          out.write("\"\"")
          progress += 1
        }
        else {
          out.write(c)
        }
      }
    }

    if(!nested) {
      out.write("\"")
    }

    progress += str.length
  }

  def getProgress: Long = progress

  def resetProgress(): Unit = {
    progress = 0
  }

}

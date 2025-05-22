// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import org.apache.commons.text.StringEscapeUtils

import java.io

trait Writer {
  val newLineSep: String = System.lineSeparator()

  val newLineSepLength: Int = newLineSep.length

  val out: java.io.Writer
  def write(c: Char): Unit
  def write(str: String): Unit
  def writeStringField(str: String): Unit
  def writeUnescaped(str: String): Unit = {
    out.write(str)
  }
  def newLine(): Unit = {
    out.write(System.lineSeparator())
  }
  def getCounter: Long = 0L
  def resetCounter(): Unit = {}
}

class CountingWriter(outwriter: java.io.Writer, var bytesCounter: Long = 0L) extends Writer {

  override def newLine(): Unit = {
    outwriter.write(newLineSep)
    bytesCounter += newLineSepLength
  }

  override def write(c: Char): Unit = {
    outwriter.write(c)
    bytesCounter += 1
  }
  override def write(str: String): Unit = {
    outwriter.write(str)
    bytesCounter += str.length
  }

  override def writeStringField(str: String): Unit = {
    if (str.nonEmpty) {
      outwriter.write('"')
      bytesCounter += 2
      for (c <- str) {
        if (c == '"') {
          outwriter.write("\"\"")
          bytesCounter += 1
        } else {
          outwriter.write(c)
        }
      }
      outwriter.write('"')
      bytesCounter += str.length
    }
  }

  override def getCounter: Long = bytesCounter

  override def resetCounter(): Unit = {
    bytesCounter = 0
  }

  override val out: io.Writer = outwriter
}

case class EscapedWriter(out: java.io.Writer) extends Writer { // stringEscapUtils
  override def write(c: Char): Unit = {
    out.write(c)
  }

  override def write(str: String): Unit = {
    for (c <- str) {
      val escaped = if (c > 127) 0 else EscapedWriter.escapeTable(c)
      if (escaped != 0) {
        out.write('\\')
        out.write(escaped)
      } else {
        out.write(c)
      }
    }
  }

  override def writeStringField(str: String): Unit = {
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

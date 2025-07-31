// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils
import java.io.ByteArrayOutputStream
import java.util

class ByteArrayOutputStreamWithOffset extends ByteArrayOutputStream {
  def this(bytes: Array[Byte], index: Int) {
    this()
    buf = bytes
    count = index
  }

  def this(size: Int) {
    this()
    buf = new Array[Byte](size)
  }

  def createNewFromOffset(offset: Int): ByteArrayOutputStreamWithOffset = {
    new ByteArrayOutputStreamWithOffset(
      util.Arrays.copyOfRange(buf, offset, count),
      count - offset)
  }

  // Use this if the byte array will not change anymore before reading it, use toByteArray if a copy is needed
  def getByteArrayOrCopy: Array[Byte] = {
    if (count == 0) buf else this.toByteArray
  }
}

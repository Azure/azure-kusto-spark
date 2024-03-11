// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils;
import java.io.ByteArrayOutputStream
import java.util

class ByteArrayOutputStreamWithOffset extends ByteArrayOutputStream {
  def this(bytes: Array[Byte]) {
    this()
    buf = bytes
  }

  def this(size: Int) {
    this()
    buf = new Array[Byte](size)
  }

  def createNewFromOffset(offset: Int): ByteArrayOutputStreamWithOffset = {
    new ByteArrayOutputStreamWithOffset(util.Arrays.copyOfRange(buf, offset, count));
  }
}

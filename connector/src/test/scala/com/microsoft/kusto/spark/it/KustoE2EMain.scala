// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.it

import scala.math.random

object KustoE2EMain {
  def main(args: Array[String]): Unit = {
    var count = 0
    for (i <- 1 to 100000) {
      val x = random() * 2 - 1
      val y = random() * 2 - 1
      if (x * x + y * y <= 1) count += 1
    }
    println(s"Pi is roughly ${4 * count / 100000.0}")
  }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.utils.KustoCustomDebugWriteOptions
import org.apache.spark.sql.SaveMode
import org.scalatest.flatspec.AnyFlatSpec

import java.security.InvalidParameterException

class KustoSinkTest extends AnyFlatSpec {
  "addBatch" should "reject overwrite for structured streaming writes" in {
    val sink = new KustoSink(
      null,
      null,
      WriteOptions(
        kustoCustomDebugWriteOptions = KustoCustomDebugWriteOptions(),
        saveMode = SaveMode.Overwrite),
      null)

    val exception = intercept[InvalidParameterException] {
      sink.addBatch(0, null)
    }

    assert(
      exception.getMessage ==
        "SaveMode.Overwrite is not supported for Spark structured streaming writes.")
  }
}

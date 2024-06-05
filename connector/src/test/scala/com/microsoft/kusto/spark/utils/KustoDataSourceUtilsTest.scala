// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.kusto.spark.datasource.ReadMode.ForceDistributedMode
import com.microsoft.kusto.spark.datasource.{
  KustoReadOptions,
  KustoSourceOptions,
  PartitionOptions,
  ReadMode
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class KustoDataSourceUtilsTest extends AnyFlatSpec with MockFactory {
  "ReadParameters" should "KustoReadOptions with passed in options" in {
    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceDistributedMode.toString,
      KustoSourceOptions.KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE -> true.toString,
      KustoSourceOptions.KUSTO_AAD_APP_ID -> "AppId",
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> "AppKey",
      KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID -> "Tenant",
      KustoSourceOptions.KUSTO_EXPORT_OPTIONS_JSON -> "{\"sizeLimit\":250,\"compressionType\":\"gzip\",\"async\":\"none\"}")
    // a no interaction mock only for test
    val actualReadOptions = KustoDataSourceUtils.getReadParameters(conf, null)
    val expectedResult = KustoReadOptions(
      Some(ForceDistributedMode),
      PartitionOptions(1, None, None),
      distributedReadModeTransientCacheEnabled = true,
      None,
      Map("sizeLimit" -> "250", "compressionType" -> "gzip", "async" -> "none"))
    assert(actualReadOptions != null)
    assert(actualReadOptions == expectedResult)
  }

  "ReadParameters" should "throw an exception when an invalid export options is passed" in {
    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_READ_MODE -> ReadMode.ForceDistributedMode.toString,
      KustoSourceOptions.KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE -> true.toString,
      KustoSourceOptions.KUSTO_AAD_APP_ID -> "AppId",
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> "AppKey",
      KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID -> "Tenant",
      KustoSourceOptions.KUSTO_EXPORT_OPTIONS_JSON -> "\"sizeLimit\":250,\"compressionType\":\"gzip\",\"async\":\"none\"}")
    val illegalArgumentException =
      intercept[IllegalArgumentException](KustoDataSourceUtils.getReadParameters(conf, null))
    assert(
      illegalArgumentException.getMessage == "The configuration for kustoExportOptionsJson has " +
        "a value \"sizeLimit\":250,\"compressionType\":\"gzip\",\"async\":\"none\"} that cannot be parsed as Map")
  }
}

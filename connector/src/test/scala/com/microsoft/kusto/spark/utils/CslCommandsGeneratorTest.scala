// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.kusto.spark.datasource.{
  TransientStorageCredentials,
  TransientStorageParameters
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

class CslCommandsGeneratorTest extends AnyFlatSpec {
  private val dataCombinations =
    Table(
      ("additionalExportOptions", "expectedOptions", "compressed", "iteration"),
      (
        Map("key1" -> "value1", "exportOption2" -> "eo2", "sizeLimit" -> "1000"),
        "with (sizeLimit=1048576000 , namePrefix=\"storms/data/part1\", " +
          "compressionType=\"snappy\",key1=\"value1\",exportOption2=\"eo2\")",
        "compressed",
        1),
      // size is not provided. Hence this will fallback to default without size
      (
        Map("key1" -> "value1", "exportOption2" -> "eo2", "compressionType" -> "gz"),
        "with ( namePrefix=\"storms/data/part1\", compressionType=\"gz\",key1=\"value1\",exportOption2=\"eo2\")",
        "compressed",
        2),
      // Though namePrefix is specified, we do not use this option and ignore this. This has downstream implications
      // where we read the exported data, better to lock this option atleast for now
      (
        Map(
          "key1" -> "value1",
          "exportOption2" -> "eo2",
          "compressionType" -> "gz",
          "namePrefix" -> "Np-2"),
        "with ( namePrefix=\"storms/data/part1\", compressionType=\"gz\",key1=\"value1\",exportOption2=\"eo2\")",
        "compressed",
        3),
      // when compressed is set as none, this should not appear in the command
      (
        Map(
          "key1" -> "value1",
          "exportOption2" -> "eo2",
          "compressionType" -> "gz",
          "compressed" -> "none"),
        "with ( namePrefix=\"storms/data/part1\", compressionType=\"gz\",key1=\"value1\",exportOption2=\"eo2\")",
        "",
        4),
      // when compressed is none , it should not be in the command
      (
        Map(
          "key1" -> "value1",
          "exportOption2" -> "eo2",
          "compressionType" -> "gz",
          "compressed" -> "none"),
        "with ( namePrefix=\"storms/data/part1\", compressionType=\"gz\",key1=\"value1\",exportOption2=\"eo2\")",
        "",
        5))

  forAll(dataCombinations) { (additionalExportOptions, expectedOptions, compressed, iteration) =>
    "TestGenerateExportDataCommand" should s"generate command with additional options-$iteration" in {
      val query = "Storms | take 100"
      val directory = "storms/data/"
      val partitionId = 1
      val transientStorageCredentials = new TransientStorageCredentials(
        "test-storage-account",
        "test-storage-account-key",
        "test-storage-account-container")
      val transientStorageParameters =
        new TransientStorageParameters(Array(transientStorageCredentials))
      val commandResult = CslCommandsGenerator.generateExportDataCommand(
        query,
        directory,
        partitionId,
        transientStorageParameters,
        Option.empty[String],
        additionalExportOptions = additionalExportOptions)
      assert(commandResult.nonEmpty)
      val expectedResult = s".export async $compressed to parquet " +
        "(\"https://test-storage-account.blob.core.windows.net/test-storage-account-container;\" h@\"test-storage-account-key\") " +
        s"$expectedOptions <| Storms | take 100"
      assert(commandResult == expectedResult)
    }
  }

  "TestGenerateExportDataCommand" should "emit OneLake URL with ;impersonate for OneLake credentials" in {
    val oneLakeUrl =
      "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/exports"
    val json =
      s"""{"storageCredentials": [{"oneLakeUrl": "$oneLakeUrl"}]}"""
    val params = TransientStorageParameters.fromString(json)

    val commandResult = CslCommandsGenerator.generateExportDataCommand(
      "Storms | take 100",
      "storms/data/",
      1,
      params,
      Option.empty[String],
      additionalExportOptions = Map.empty[String, String])

    val expectedResult = ".export async compressed to parquet " +
      s"""("$oneLakeUrl;impersonate") """ +
      "with ( namePrefix=\"storms/data/part1\", compressionType=\"snappy\",) " +
      "<| Storms | take 100"
    assert(commandResult == expectedResult)
  }

  it should "canonicalize OneLake abfss URL to https in the .export command" in {
    val abfssUrl =
      "abfss://myws@onelake.dfs.fabric.microsoft.com/mylake.Lakehouse/Files/exports"
    val canonicalHttpsUrl =
      "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/exports"
    val json =
      s"""{"storageCredentials": [{"oneLakeUrl": "$abfssUrl"}]}"""
    val params = TransientStorageParameters.fromString(json)

    val commandResult = CslCommandsGenerator.generateExportDataCommand(
      "Storms | take 100",
      "storms/data/",
      1,
      params,
      Option.empty[String],
      additionalExportOptions = Map.empty[String, String])

    // Kusto .export accepts the https form of the OneLake URL; the abfss input is
    // canonicalized at parse time so the emitted CSL is always uniform.
    assert(commandResult.contains(s"$canonicalHttpsUrl;impersonate"))
    assert(!commandResult.contains("abfss://"))
  }
}

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
  private val Key1 = "key1"
  private val Value1 = "value1"
  private val ExportOption2 = "exportOption2"
  private val Eo2 = "eo2"
  private val CompressionTypeKey = "compressionType"
  private val Gz = "gz"
  private val Compressed = "compressed"
  private val ExpectedOptionsWithGz =
    "with ( namePrefix=\"storms/data/part1\", compressionType=\"gz\",key1=\"value1\",exportOption2=\"eo2\")"

  private val dataCombinations =
    Table(
      ("additionalExportOptions", "expectedOptions", Compressed, "iteration"),
      (
        Map(Key1 -> Value1, ExportOption2 -> Eo2, "sizeLimit" -> "1000"),
        "with (sizeLimit=1048576000 , namePrefix=\"storms/data/part1\", " +
          "compressionType=\"snappy\",key1=\"value1\",exportOption2=\"eo2\")",
        Compressed,
        1),
      // size is not provided. Hence this will fallback to default without size
      (
        Map(Key1 -> Value1, ExportOption2 -> Eo2, CompressionTypeKey -> Gz),
        ExpectedOptionsWithGz,
        Compressed,
        2),
      // Though namePrefix is specified, we do not use this option and ignore this. This has downstream implications
      // where we read the exported data, better to lock this option atleast for now
      (
        Map(
          Key1 -> Value1,
          ExportOption2 -> Eo2,
          CompressionTypeKey -> Gz,
          "namePrefix" -> "Np-2"),
        ExpectedOptionsWithGz,
        Compressed,
        3),
      // when compressed is set as none, this should not appear in the command
      (
        Map(Key1 -> Value1, ExportOption2 -> Eo2, CompressionTypeKey -> Gz, Compressed -> "none"),
        ExpectedOptionsWithGz,
        "",
        4),
      // when compressed is none , it should not be in the command
      (
        Map(Key1 -> Value1, ExportOption2 -> Eo2, CompressionTypeKey -> Gz, Compressed -> "none"),
        ExpectedOptionsWithGz,
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
}

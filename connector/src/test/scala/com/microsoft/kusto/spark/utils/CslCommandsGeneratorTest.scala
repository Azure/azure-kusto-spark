//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

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
}

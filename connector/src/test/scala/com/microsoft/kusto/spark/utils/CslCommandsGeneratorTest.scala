package com.microsoft.kusto.spark.utils

import com.microsoft.kusto.spark.datasource.{TransientStorageCredentials, TransientStorageParameters}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

@RunWith(classOf[JUnitRunner])
class CslCommandsGeneratorTest extends FlatSpec{
  val dataCombinations =
    Table(
      ("sizeLimit", "additionalExportOptions", "expectedOptions","iteration"),
      (Some(1000L), Map("key1" -> "value1", "exportOption2" -> "eo2"), "with (sizeLimit=1048576000 , namePrefix=\"storms/data/part1\", " +
        "compressionType=\"snappy\",key1=\"value1\",exportOption2=\"eo2\")",1),
      (None, Map("key1" -> "value1", "exportOption2" -> "eo2", "namePrefix" -> "Np-2","compressionType"->"gz"),
        "with ( namePrefix=\"Np-2\", compressionType=\"gz\",key1=\"value1\",exportOption2=\"eo2\")",2)
    )

  forAll(dataCombinations) { (sizeLimit, additionalExportOptions, expectedOptions ,iteration)=>
    "TestGenerateExportDataCommand" should s"generate command with additional options-$iteration" in {
      val query = "Storms | take 100"
      val directory = "storms/data/"
      val partitionId = 1
      val transientStorageCredentials = new TransientStorageCredentials(
        "test-storage-account",
        "test-storage-account-key",
        "test-storage-account-container")
      val transientStorageParameters = new TransientStorageParameters(Array(transientStorageCredentials))
      // val additionalExportOptions = Map("key1" -> "value1", "exportOption2" -> "eo2")
      val commandResult = CslCommandsGenerator.generateExportDataCommand(query, directory, partitionId,
        transientStorageParameters, Option.empty[String], sizeLimit, isCompressed = true, additionalExportOptions = additionalExportOptions)
      assert(commandResult.nonEmpty)
      val expectedResult = ".export async compressed to parquet " +
        "(\"https://test-storage-account.blob.core.windows.net/test-storage-account-container;\" h@\"test-storage-account-key\") " +
        s"$expectedOptions <| Storms | take 100"
      assert(commandResult == expectedResult)
    }
  }
}

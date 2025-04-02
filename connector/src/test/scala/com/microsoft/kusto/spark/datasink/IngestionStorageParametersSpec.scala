// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.Seq

class IngestionStorageParametersSpec extends AnyFlatSpec with Matchers {

  "fromString" should "deserialize JSON string to IngestionStorageParameters array" in {
    val url1 = "https://ateststorage.blob.core.windows.net/container1"
    val url2 = "https://ateststorage2.blob.core.windows.net/container2"
    val json =
      s"""[{"storageUrl":"$url1","containerName":"container1","userMsi":"msi1"},
         |{"storageUrl":"$url2","containerName":"container2","userMsi":"msi2"}]""".stripMargin
    val result = IngestionStorageParameters.fromString(json)
    result.length should be(2)
    result(0).storageUrl should be(url1)
    result(0).userMsi should be("msi1")
    result(1).storageUrl should be(url2)
    result(1).userMsi should be("msi2")

    val randomIngestionParameter = IngestionStorageParameters.getRandomIngestionStorage(result)
    Set(
      url2,
      url1
    ) should contain(randomIngestionParameter.storageUrl)
  }

  it should "handle empty JSON array" in {
    val json = "[]"
    val result = IngestionStorageParameters.fromString(json)
    result.length should be (0)
  }

  it should "throw an exception for invalid JSON" in {
    val json =
      """[{"storageUrl":"https://ateststorage.blob.core.windows.net","containerName":"container1","userMsi":"msi1"},
        |"storageUrl":"https://ateststorage.blob.core.windows.net","containerName":"container2"}]""".stripMargin
    an [Exception] should be thrownBy IngestionStorageParameters.fromString(json)
  }

  "toString" should "return a string representation of IngestionStorageParameters" in {
    val params = new IngestionStorageParameters("url","c1", "msi","sas")
    params.toString should be ("storageUrl: url, containerName: c1, userMsi: msi, is-sas: true")
  }

  it should "handle empty fields" in {
    val params = new IngestionStorageParameters("", "", "","")
    params.toString should be ("storageUrl: , containerName: , userMsi: , is-sas: false")
  }
}

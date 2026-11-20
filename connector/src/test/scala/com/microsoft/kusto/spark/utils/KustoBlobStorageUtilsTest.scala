// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.azure.storage.blob.BlobContainerClientBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KustoBlobStorageUtilsTest extends AnyFlatSpec with Matchers {
  "Blob cleanup" should "use the export endpoint for SAS and SharedKey without issuing requests" in {
    for (suffix <- Seq("core.windows.net", "core.usgovcloudapi.net", "core.chinacloudapi.cn");
      sas <- Seq(true, false)) {
      val secret = if (sas) "?sv=2023-11-03&sig=unused" else "dGVzdC1rZXk="
      val connection =
        KustoBlobStorageUtils.storageConnectionString("account", secret, sas, suffix)
      val client = new BlobContainerClientBuilder()
        .connectionString(connection)
        .containerName("container")
        .buildClient()
      client.getBlobContainerUrl shouldBe s"https://account.blob.$suffix/container"
      connection should include(if (sas) "SharedAccessSignature=sv=" else "AccountKey=")
      connection should not include "Signature=?"
    }
  }
}

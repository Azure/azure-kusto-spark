package com.microsoft.kusto.spark.datasource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TransientStorageParametersTest extends AnyFlatSpec {

  "TransientStorageParameters ToString" should "check token not getting printed" in {
    val transientStorage = "{\"storageCredentials\":[{\"storageAccountName\":\"ateststorage\"," +
      "\"blobContainer\":\"kusto\",\"sasUrl\":\"https://ateststorage.blob.core.windows.net/kusto\"," +
      "\"sasKey\":\"?sp=racwdlmeop&st=2020-03-15T04:26:19Z&se=2020-03-16T12:26:19Z&spr=https&sv=2019-12-02" +
      "&sr=c&sig=xxxxxx\"},{\"storageAccountName\":\"ateststorage2\"," +
      "\"blobContainer\":\"kusto2\",\"sasUrl\":\"https://ateststorage2.blob.core.windows.net/kusto2\"," +
      "\"sasKey\":\"?sp=racwdlmeop&st=2020-03-15T04:26:19Z&se=2020-03-16T12:26:19Z&spr=https&sv=2019-12-02" +
      "&sr=c&sig=yyyyyyyyy\"}],\"endpointSuffix\":\"core.windows.net\"}"
    val transientStorageParameters = TransientStorageParameters.fromString(transientStorage)
    val tsString = transientStorageParameters.toString()
    transientStorageParameters.storageCredentials.length shouldEqual 2

    tsString shouldEqual s"[BlobContainer: kusto ,Storage: ateststorage , IsSasKeyDefined: true${System.lineSeparator()}BlobContainer: kusto2 ,Storage: ateststorage2 , IsSasKeyDefined: true, domain: core.windows.net]"
  }
  "TransientStorageCredentials ToString" should "parse SAS and not print tokens " in {
    val transientStorageCredentials = new TransientStorageCredentials(
      "https://ateststorage2.blob.core.windows.net/kusto2" +
        "?sp=racwdlmeop&st=2020-03-15T04:26:19Z&se=2020-03-16T12:26:19Z&spr=https&sv=2019-12-02&sr=c&sig=xxxxxx")
    transientStorageCredentials.toString shouldEqual "BlobContainer: kusto2 ,Storage: ateststorage2 , IsSasKeyDefined: true"
  }
}

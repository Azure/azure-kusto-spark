// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{
  convertToAnyShouldWrapper,
  convertToStringShouldWrapper,
  include
}

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

  "TransientStorageCredentials ToString with impersonate" should "parse and deserialize " in {
    val transientStorageCredentials =
      new TransientStorageParameters(
        Array(
          new TransientStorageCredentials(
            "https://ateststorage.blob.core.windows.net/kusto;impersonate")))

    transientStorageCredentials.toString shouldEqual s"[BlobContainer: kusto ,Storage: ateststorage , IsSasKeyDefined: false, domain: core.windows.net]"
    TransientStorageParameters
      .fromString(transientStorageCredentials.toInsecureString)
      .toString shouldEqual "[BlobContainer: kusto ,Storage: ateststorage , IsSasKeyDefined: false, domain: core.windows.net]"
  }

  "TransientStorageParameters" should "should get parsed for impersonate string" in {
    val transientStorage =
      "{\"storageCredentials\": [{\"storageAccountName\": \"ateststorage\",\"blobContainer\": \"kusto\"," +
        "\"sasUrl\": \"https://ateststorage.blob.core.windows.net/kusto;impersonate\"}," +
        "{\"storageAccountName\": \"ateststorage2\",\"blobContainer\": \"kusto2\"," +
        "\"sasUrl\": \"https://ateststorage2.blob.core.windows.net/kusto2;impersonate\"}],\"endpointSuffix\": \"core.windows.net\"}"
    val transientStorageParameters = TransientStorageParameters.fromString(transientStorage)
    val tsString = transientStorageParameters.toString()
    transientStorageParameters.storageCredentials.length shouldEqual 2

    tsString shouldEqual s"[BlobContainer: kusto ,Storage: ateststorage , IsSasKeyDefined: false${System
        .lineSeparator()}" +
      s"BlobContainer: kusto2 ,Storage: ateststorage2 , IsSasKeyDefined: false, domain: core.windows.net]"
  }

  "TransientStorageCredentials" should "recognize OneLake https URL form" in {
    val url =
      "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/exports"
    TransientStorageCredentials.isOneLakeUrl(url) shouldEqual true
    val cred = new TransientStorageCredentials(url)
    cred.isOneLake shouldEqual true
    cred.authMethod shouldEqual AuthMethod.Impersonation
    cred.oneLakeWorkspace shouldEqual "myws"
    cred.oneLakeEndpoint shouldEqual "onelake.dfs.fabric.microsoft.com"
    cred.oneLakeArtifactPath shouldEqual "mylake.Lakehouse/Files/exports"
    cred.oneLakeAbfssBase shouldEqual
      "abfss://myws@onelake.dfs.fabric.microsoft.com/mylake.Lakehouse/Files/exports"
    cred.oneLakeUrl shouldEqual url
  }

  it should "recognize OneLake abfss URL form and canonicalize to https" in {
    val url =
      "abfss://myws@onelake.dfs.fabric.microsoft.com/mylake.Lakehouse/Files/exports;impersonate"
    TransientStorageCredentials.isOneLakeUrl(url) shouldEqual true
    val cred = new TransientStorageCredentials(url)
    cred.isOneLake shouldEqual true
    cred.oneLakeWorkspace shouldEqual "myws"
    cred.oneLakeEndpoint shouldEqual "onelake.dfs.fabric.microsoft.com"
    cred.oneLakeArtifactPath shouldEqual "mylake.Lakehouse/Files/exports"
    // ;impersonate suffix stripped, and abfss canonicalized to https so CSL emission
    // always uses the form verified to work with Kusto .export.
    cred.oneLakeUrl shouldEqual
      "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/exports"
  }

  it should "round-trip OneLake credentials via JSON" in {
    val transientStorage =
      "{\"storageCredentials\": [{\"sasUrl\": " +
        "\"https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/exports\"}]," +
        "\"endpointSuffix\": \"fabric.microsoft.com\"}"
    val params = TransientStorageParameters.fromString(transientStorage)
    params.storageCredentials.length shouldEqual 1
    val cred = params.storageCredentials.head
    cred.isOneLake shouldEqual true
    cred.oneLakeAbfssBase shouldEqual
      "abfss://myws@onelake.dfs.fabric.microsoft.com/mylake.Lakehouse/Files/exports"

    // Re-serialize and re-parse to verify Jackson handles the OneLake fields cleanly
    val roundTripped = TransientStorageParameters.fromString(params.toInsecureString)
    roundTripped.storageCredentials.head.isOneLake shouldEqual true
    roundTripped.storageCredentials.head.oneLakeWorkspace shouldEqual "myws"
  }

  it should "not classify ordinary blob URLs as OneLake" in {
    TransientStorageCredentials.isOneLakeUrl(
      "https://ateststorage.blob.core.windows.net/kusto?sv=2020") shouldEqual false
    TransientStorageCredentials.isOneLakeUrl(
      "https://ateststorage.dfs.core.windows.net/kusto") shouldEqual false
    TransientStorageCredentials.isOneLakeUrl(null) shouldEqual false
    TransientStorageCredentials.isOneLakeUrl("") shouldEqual false
  }

  it should "not classify lookalike hosts with extra labels after the suffix" in {
    // Loose substring matching used to misclassify these; tightened to endsWith.
    TransientStorageCredentials.isOneLakeUrl(
      "https://foo.blob.fabric.microsoft.test.com/ws/lh.Lakehouse/Files") shouldEqual false
    TransientStorageCredentials.isOneLakeUrl(
      "https://onelake.dfs.fabric.microsoft.com.evil.example/ws/lh") shouldEqual false
  }

  it should "parse OneLake JSON when only oneLakeUrl is supplied" in {
    val transientStorage =
      "{\"storageCredentials\": [{\"oneLakeUrl\": " +
        "\"https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/exports\"}]}"
    val params = TransientStorageParameters.fromString(transientStorage)
    val cred = params.storageCredentials.head
    cred.isOneLake shouldEqual true
    cred.oneLakeWorkspace shouldEqual "myws"
    cred.oneLakeArtifactPath shouldEqual "mylake.Lakehouse/Files/exports"
  }

  "TransientStorageParameters mixing" should "reject OneLake + Blob credentials together" in {
    val transientStorage =
      "{\"storageCredentials\": [" +
        "{\"sasUrl\": \"https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/exports\"}," +
        "{\"sasUrl\": \"https://ateststorage.blob.core.windows.net/kusto;impersonate\"}" +
        "]}"
    val thrown = intercept[java.security.InvalidParameterException] {
      TransientStorageParameters.fromString(transientStorage)
    }
    thrown.getMessage should include("cannot mix OneLake and non-OneLake")
  }

  it should "reject same-credential mixing of OneLake URL with blob fields" in {
    val transientStorage =
      "{\"storageCredentials\": [{" +
        "\"oneLakeUrl\": \"https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/exports\"," +
        "\"storageAccountName\": \"evilstorage\"," +
        "\"blobContainer\": \"kusto\"" +
        "}]}"
    val thrown = intercept[java.security.InvalidParameterException] {
      TransientStorageParameters.fromString(transientStorage)
    }
    thrown.getMessage should include("cannot also specify blob storage fields")
  }

  it should "reject same-credential mixing of OneLake URL with sasKey" in {
    val transientStorage =
      "{\"storageCredentials\": [{" +
        "\"oneLakeUrl\": \"https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/exports\"," +
        "\"sasKey\": \"?sp=racwdl&sig=xxxx\"" +
        "}]}"
    val thrown = intercept[java.security.InvalidParameterException] {
      TransientStorageParameters.fromString(transientStorage)
    }
    thrown.getMessage should include("cannot also specify blob storage fields")
  }

  "OneLake JSON injection" should "ignore attacker-supplied derived fields and recompute from URL" in {
    // Attacker tries to set oneLakeWorkspace to a workspace they don't own while
    // oneLakeUrl points elsewhere — derived fields must always come from the URL.
    val transientStorage =
      "{\"storageCredentials\": [{" +
        "\"oneLakeUrl\": \"https://onelake.dfs.fabric.microsoft.com/realws/reallh.Lakehouse/Files/exports\"," +
        "\"oneLakeWorkspace\": \"victimws\"," +
        "\"oneLakeEndpoint\": \"victim.dfs.fabric.microsoft.com\"," +
        "\"oneLakeArtifactPath\": \"victimlh.Lakehouse/Files/secret\"" +
        "}]}"
    val params = TransientStorageParameters.fromString(transientStorage)
    val cred = params.storageCredentials.head
    cred.oneLakeWorkspace shouldEqual "realws"
    cred.oneLakeEndpoint shouldEqual "onelake.dfs.fabric.microsoft.com"
    cred.oneLakeArtifactPath shouldEqual "reallh.Lakehouse/Files/exports"
  }

  it should "reject oneLakeUrl that is not a recognized OneLake URL" in {
    val transientStorage =
      "{\"storageCredentials\": [{" +
        "\"oneLakeUrl\": \"https://attacker.example.com/ws/lh/Files/exports\"" +
        "}]}"
    val thrown = intercept[java.security.InvalidParameterException] {
      TransientStorageParameters.fromString(transientStorage)
    }
    thrown.getMessage should include("not a recognized Fabric OneLake URL")
  }

  "OneLake path validation" should "reject artifact paths without /Files/ segment" in {
    val thrown = intercept[java.security.InvalidParameterException] {
      new TransientStorageCredentials(
        "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Tables/secret")
    }
    thrown.getMessage should include("Files")
  }

  it should "reject artifact paths with too few segments" in {
    val thrown = intercept[java.security.InvalidParameterException] {
      new TransientStorageCredentials(
        "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse")
    }
    thrown.getMessage should include("artifact")
  }

  it should "reject path traversal" in {
    val thrown = intercept[java.security.InvalidParameterException] {
      new TransientStorageCredentials(
        "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/../secret")
    }
    thrown.getMessage should include("..")
  }

  it should "reject explicit port" in {
    val thrown = intercept[java.security.InvalidParameterException] {
      new TransientStorageCredentials(
        "https://onelake.dfs.fabric.microsoft.com:8443/myws/mylake.Lakehouse/Files/exports")
    }
    thrown.getMessage should include("port")
  }

  it should "reject query strings" in {
    val thrown = intercept[java.security.InvalidParameterException] {
      // isOneLakeUrl strips the query, so this reaches parseOneLake with a query attached.
      // Use the secondary constructor with oneLakeUrl directly to verify parseOneLake rejects.
      val cred = new TransientStorageCredentials()
      cred.oneLakeUrl =
        "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/exports?sig=evil"
      cred.parseOneLake(cred.oneLakeUrl)
    }
    thrown.getMessage should include("query")
  }
}

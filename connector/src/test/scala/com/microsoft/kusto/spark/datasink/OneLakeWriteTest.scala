// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.datasource.{
  TransientStorageCredentials,
  TransientStorageParameters
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.security.InvalidParameterException

class OneLakeWriteTest extends AnyFlatSpec with Matchers {

  "OneLakeWriteResource" should "construct correct ingest URI with ;impersonate" in {
    val httpsUrl =
      "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/staging/test.csv.gz"
    val resource = OneLakeWriteResource(null, null, null, null, httpsUrl)
    resource.ingestUri shouldEqual s"$httpsUrl;impersonate"
  }

  "OneLake ingestion storage parsing" should "parse OneLake JSON from KUSTO_INGESTION_STORAGE" in {
    val json =
      """{"storageCredentials": [{"oneLakeUrl": "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/staging"}]}"""
    val params = TransientStorageParameters.fromString(json)
    params.storageCredentials should have length 1
    val cred = params.storageCredentials.head
    cred.isOneLake shouldEqual true
    cred.oneLakeWorkspace shouldEqual "myws"
    cred.oneLakeEndpoint shouldEqual "onelake.dfs.fabric.microsoft.com"
    cred.oneLakeArtifactPath shouldEqual "mylake.Lakehouse/Files/staging"
  }

  it should "construct correct abfss base URL for file writes" in {
    val json =
      """{"storageCredentials": [{"oneLakeUrl": "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/staging"}]}"""
    val params = TransientStorageParameters.fromString(json)
    val cred = params.storageCredentials.head
    cred.oneLakeAbfssBase shouldEqual "abfss://myws@onelake.dfs.fabric.microsoft.com/mylake.Lakehouse/Files/staging"
  }

  it should "construct correct https URL for ingest submission" in {
    val json =
      """{"storageCredentials": [{"oneLakeUrl": "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/staging"}]}"""
    val params = TransientStorageParameters.fromString(json)
    val cred = params.storageCredentials.head
    val blobName = "mydb_tmp_uuid_0_0_12-00-00_spark.csv.gz"
    val ingestUrl = s"${cred.oneLakeUrl}/$blobName;impersonate"
    ingestUrl shouldEqual
      s"https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/staging/$blobName;impersonate"
  }

  it should "accept abfss URL and canonicalize to https for ingestion" in {
    val json =
      """{"storageCredentials": [{"oneLakeUrl": "abfss://myws@onelake.dfs.fabric.microsoft.com/mylake.Lakehouse/Files/staging"}]}"""
    val params = TransientStorageParameters.fromString(json)
    val cred = params.storageCredentials.head
    cred.isOneLake shouldEqual true
    // oneLakeUrl is always stored in https form
    cred.oneLakeUrl shouldEqual "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/staging"
    // abfss base is still correct for Hadoop FileSystem
    cred.oneLakeAbfssBase shouldEqual "abfss://myws@onelake.dfs.fabric.microsoft.com/mylake.Lakehouse/Files/staging"
  }

  "OneLake write validation" should "reject mixing OneLake + blob in same credentials array" in {
    val json =
      """{"storageCredentials": [
        |  {"oneLakeUrl": "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/staging"},
        |  {"sasUrl": "https://mystorage.blob.core.windows.net/container;impersonate"}
        |]}""".stripMargin
    val thrown = intercept[InvalidParameterException] {
      TransientStorageParameters.fromString(json)
    }
    thrown.getMessage should include("cannot mix OneLake and non-OneLake")
  }

  it should "reject OneLake URL with blob fields" in {
    val json =
      """{"storageCredentials": [{
        |  "oneLakeUrl": "https://onelake.dfs.fabric.microsoft.com/myws/mylake.Lakehouse/Files/staging",
        |  "storageAccountName": "evilstorage"
        |}]}""".stripMargin
    val thrown = intercept[InvalidParameterException] {
      TransientStorageParameters.fromString(json)
    }
    thrown.getMessage should include("cannot also specify blob storage fields")
  }

  it should "reject non-OneLake URL in oneLakeUrl field" in {
    val json =
      """{"storageCredentials": [{"oneLakeUrl": "https://attacker.example.com/ws/lh/Files/staging"}]}"""
    val thrown = intercept[InvalidParameterException] {
      TransientStorageParameters.fromString(json)
    }
    thrown.getMessage should include("not a recognized Fabric OneLake URL")
  }
}

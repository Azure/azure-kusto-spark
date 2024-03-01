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

package com.microsoft.kusto.spark.authentication

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.Callable

class TokenProvider1(map: CaseInsensitiveMap[String]) extends Callable[String] with Serializable {
  override def call(): String = map("token")
}

class TokenProvider2(map: CaseInsensitiveMap[String]) extends Callable[String] with Serializable {
  override def call(): String = map("token")
}

class kustoAuthenticationTests extends AnyFlatSpec {
  "KeyVaultAppAuthentication Equals" should "Check equality and inequality between different KeyVaultAppAuthentication" in {
    val kvaa11 = KeyVaultAppAuthentication("uri1", "appId1", "pass1", null)
    val kvaa11Duplicate = KeyVaultAppAuthentication("uri1", "appId1", "pass2", null)
    val kvaa12 = KeyVaultAppAuthentication("uri1", "appId2", "pass123", null)
    val kvaa21 = KeyVaultAppAuthentication("uri2", "appId1", "pass12", null)
    val kvaa22 = KeyVaultAppAuthentication("uri2", "appId2", "pass12", null)
    assert(kvaa11 == kvaa11Duplicate)
    assert(kvaa11 != kvaa12)
    assert(kvaa11 != kvaa21)
    assert(kvaa11 != kvaa22)
    assert(kvaa12 != kvaa21)
    assert(kvaa12 != kvaa22)
    assert(kvaa21 != kvaa22)
  }

  "KeyVaultCertificateAuthentication Equals" should "Check equality and inequality between different KeyVaultCertificateAuthentication" in {
    val kvca11 = KeyVaultCertificateAuthentication("uri1", "path1", "pass1", null)
    val kvca11Duplicate = KeyVaultCertificateAuthentication("uri1", "path1", "pass2", null)
    val kvca12 = KeyVaultCertificateAuthentication("uri1", "path2", "pass123", null)
    val kvca21 = KeyVaultCertificateAuthentication("uri2", "path1", "pass12", null)
    val kvca22 = KeyVaultCertificateAuthentication("uri2", "path2", "pass12", null)

    assert(kvca11 == kvca11Duplicate)
    assert(kvca11 != kvca12)
    assert(kvca11 != kvca21)
    assert(kvca11 != kvca22)
    assert(kvca12 != kvca21)
    assert(kvca12 != kvca22)
    assert(kvca21 != kvca22)
  }

  "KustoAccessTokenAuthentication Equals" should "Check equality and inequality between different KustoAccessTokenAuthentication" in {
    val kata1 = KustoAccessTokenAuthentication("token1")
    val kata1Duplicate = KustoAccessTokenAuthentication("token1")
    val kata2 = KustoAccessTokenAuthentication("token2")

    assert(kata1 == kata1Duplicate)
    assert(kata1 != kata2)
  }

  "KustoAuthentication Equals" should "Check token not getting printed" in {
    val kata1 = KustoAccessTokenAuthentication("token1")
    val kvaa11 = KeyVaultAppAuthentication("uri1", "appId1", "pass1", null)
    val kvca11 = KeyVaultCertificateAuthentication("uri1", "path1", "pass1", null)

    assert(kata1.toString == "")
    assert(kvaa11.toString == "")
    assert(kvca11.toString == "")
  }

  "KustoAccessTokenAuthentication Equals" should "Verify that different types of authentication won't equal" in {
    val kvaa11 = KeyVaultAppAuthentication("uri1", "appId1", "pass1", null)
    val kvca11 = KeyVaultCertificateAuthentication("uri1", "path1", "pass1", null)
    val kata1 = KustoAccessTokenAuthentication("token1")

    assert(kvaa11 != kvca11)
    assert(kvaa11 != kata1)
    assert(kvca11 != kata1)
  }

  "KustoTokenProviderAuthentication Equals" should "Verify that different types of authentication won't equal" in {
    val params = CaseInsensitiveMap(Map[String, String]("token" -> "token"))

    val tokenProvider1 = java.lang.ClassLoader.getSystemClassLoader
      .loadClass("com.microsoft.kusto.spark.authentication.TokenProvider1")
      .getConstructor(params.getClass)
      .newInstance(params)

    val tokenProvider2 = java.lang.ClassLoader.getSystemClassLoader
      .loadClass("com.microsoft.kusto.spark.authentication.TokenProvider1")
      .getConstructor(params.getClass)
      .newInstance(params)

    val ktp1 = KustoTokenProviderAuthentication(tokenProvider1.asInstanceOf[Callable[String]])
    val ktp2 = KustoTokenProviderAuthentication(tokenProvider2.asInstanceOf[Callable[String]])
    assert(ktp1 != ktp2)
  }
}

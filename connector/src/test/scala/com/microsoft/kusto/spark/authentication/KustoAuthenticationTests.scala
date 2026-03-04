// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.authentication

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.Callable

// scalastyle:off null - test code uses null for optional authority parameter
private[authentication] object AuthTestConstants {
  val TokenKey = "token"
  val Uri1 = "uri1"
  val Uri2 = "uri2"
  val AppId1 = "appId1"
  val Pass1 = "pass1"
  val Pass12 = "pass12"
  val Path1 = "path1"
  val Token1 = "token1"
}

class TokenProvider1(map: CaseInsensitiveMap[String]) extends Callable[String] with Serializable {
  override def call(): String = map(AuthTestConstants.TokenKey)
}

class TokenProvider2(map: CaseInsensitiveMap[String]) extends Callable[String] with Serializable {
  override def call(): String = map(AuthTestConstants.TokenKey)
}

class kustoAuthenticationTests extends AnyFlatSpec {
  import AuthTestConstants._

  "KeyVaultAppAuthentication Equals" should "Check equality and inequality between different KeyVaultAppAuthentication" in {
    val kvaa11 = KeyVaultAppAuthentication(Uri1, AppId1, Pass1, null)
    val kvaa11Duplicate = KeyVaultAppAuthentication(Uri1, AppId1, "pass2", null)
    val kvaa12 = KeyVaultAppAuthentication(Uri1, "appId2", "pass123", null)
    val kvaa21 = KeyVaultAppAuthentication(Uri2, AppId1, Pass12, null)
    val kvaa22 = KeyVaultAppAuthentication(Uri2, "appId2", Pass12, null)
    assert(kvaa11 == kvaa11Duplicate)
    assert(kvaa11 != kvaa12)
    assert(kvaa11 != kvaa21)
    assert(kvaa11 != kvaa22)
    assert(kvaa12 != kvaa21)
    assert(kvaa12 != kvaa22)
    assert(kvaa21 != kvaa22)
  }

  "KeyVaultCertificateAuthentication Equals" should "Check equality and inequality between different KeyVaultCertificateAuthentication" in {
    val kvca11 = KeyVaultCertificateAuthentication(Uri1, Path1, Pass1, null)
    val kvca11Duplicate = KeyVaultCertificateAuthentication(Uri1, Path1, "pass2", null)
    val kvca12 = KeyVaultCertificateAuthentication(Uri1, "path2", "pass123", null)
    val kvca21 = KeyVaultCertificateAuthentication(Uri2, Path1, Pass12, null)
    val kvca22 = KeyVaultCertificateAuthentication(Uri2, "path2", Pass12, null)

    assert(kvca11 == kvca11Duplicate)
    assert(kvca11 != kvca12)
    assert(kvca11 != kvca21)
    assert(kvca11 != kvca22)
    assert(kvca12 != kvca21)
    assert(kvca12 != kvca22)
    assert(kvca21 != kvca22)
  }

  "KustoAccessTokenAuthentication Equals" should "Check equality and inequality between different KustoAccessTokenAuthentication" in {
    val kata1 = KustoAccessTokenAuthentication(Token1)
    val kata1Duplicate = KustoAccessTokenAuthentication(Token1)
    val kata2 = KustoAccessTokenAuthentication("token2")

    assert(kata1 == kata1Duplicate)
    assert(kata1 != kata2)
  }

  "KustoAuthentication Equals" should "Check token not getting printed" in {
    val kata1 = KustoAccessTokenAuthentication(Token1)
    val kvaa11 = KeyVaultAppAuthentication(Uri1, AppId1, Pass1, null)
    val kvca11 = KeyVaultCertificateAuthentication(Uri1, Path1, Pass1, null)

    assert(kata1.toString == "")
    assert(kvaa11.toString == "")
    assert(kvca11.toString == "")
  }

  "KustoAccessTokenAuthentication Equals" should "Verify that different types of authentication won't equal" in {
    val kvaa11 = KeyVaultAppAuthentication(Uri1, AppId1, Pass1, null)
    val kvca11 = KeyVaultCertificateAuthentication(Uri1, Path1, Pass1, null)
    val kata1 = KustoAccessTokenAuthentication(Token1)

    assert(kvaa11 != kvca11)
    assert(kvaa11 != kata1)
    assert(kvca11 != kata1)
  }

  "KustoTokenProviderAuthentication Equals" should "Verify that different types of authentication won't equal" in {
    val params = CaseInsensitiveMap(Map[String, String](TokenKey -> TokenKey))

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

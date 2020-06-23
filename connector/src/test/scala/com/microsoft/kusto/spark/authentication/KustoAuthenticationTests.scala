package com.microsoft.kusto.spark.authentication

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class kustoAuthenticationTests extends FlatSpec {
  "KeyVaultAppAuthentication Equals" should "Check equality and inequality between different KeyVaultAppAuthentication" in {
    val kvaa11 = KeyVaultAppAuthentication("uri1", "appId1", "pass1")
    val kvaa11Duplicate = KeyVaultAppAuthentication("uri1", "appId1", "pass2")
    val kvaa12 = KeyVaultAppAuthentication("uri1", "appId2", "pass123")
    val kvaa21 = KeyVaultAppAuthentication("uri2", "appId1", "pass12")
    val kvaa22 = KeyVaultAppAuthentication("uri2", "appId2", "pass12")

    assert(kvaa11 == kvaa11Duplicate)
    assert(kvaa11 != kvaa12)
    assert(kvaa11 != kvaa21)
    assert(kvaa11 != kvaa22)
    assert(kvaa12 != kvaa21)
    assert(kvaa12 != kvaa22)
    assert(kvaa21 != kvaa22)
  }

  "KeyVaultCertificateAuthentication Equals" should "Check equality and inequality between different KeyVaultCertificateAuthentication" in {
    val kvca11 = KeyVaultCertificateAuthentication("uri1", "path1", "pass1")
    val kvca11Duplicate = KeyVaultCertificateAuthentication("uri1", "path1", "pass2")
    val kvca12 = KeyVaultCertificateAuthentication("uri1", "path2", "pass123")
    val kvca21 = KeyVaultCertificateAuthentication("uri2", "path1", "pass12")
    val kvca22 = KeyVaultCertificateAuthentication("uri2", "path2", "pass12")

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

  "KustoAccessTokenAuthentication Equals" should "Verify that different types of authentication won't equal" in {
    val kvaa11 = KeyVaultAppAuthentication("uri1", "appId1", "pass1")
    val kvca11 = KeyVaultCertificateAuthentication("uri1", "path1", "pass1")
    val kata1 = KustoAccessTokenAuthentication("token1")

    assert(kvaa11 != kvca11)
    assert(kvaa11 != kata1)
    assert(kvca11 != kata1)
  }
}

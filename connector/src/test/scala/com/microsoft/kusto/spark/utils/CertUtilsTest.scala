// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import org.scalatest.flatspec.AnyFlatSpec

import java.security.UnrecoverableKeyException

class CertUtilsTest extends AnyFlatSpec {

  "pfx cert" should "be read from file into KeyCert type" in {
    val certPath = this.getClass.getResource("/certs/cert.pfx").getPath
    val cert = CertUtils.readPfx(certPath, "")
    cert.key.getFormat.equals("PKCS#8")
    cert.cert.getNotAfter.toString.equals("Thu Aug 12 18:14:31 PDT 2021")
  }

  "pfx password protected cert" should "be read from file into KeyCert type" in {
    val certPasswordProtectedPath =
      this.getClass.getResource("/certs/cert-password-protected.pfx").getPath
    val cert = CertUtils.readPfx(certPasswordProtectedPath, "testing")
    cert.key.getFormat.equals("PKCS#8")
    cert.cert.getNotAfter.toString.equals("Thu Aug 12 18:14:31 PDT 2021")
  }

  it should "throw UnrecoverableKeyException if pfx cert does not have private key" in {
    val certPasswordProtectedPath =
      this.getClass.getResource("/certs/cert-no-privatekey.pfx").getPath
    assertThrows[UnrecoverableKeyException] {
      CertUtils.readPfx(certPasswordProtectedPath, "")
    }
  }
}

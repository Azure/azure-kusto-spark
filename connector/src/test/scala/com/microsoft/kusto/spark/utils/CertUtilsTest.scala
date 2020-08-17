package com.microsoft.kusto.spark.utils

import java.security.UnrecoverableKeyException

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CertUtilsTest extends FlatSpec{

  "pfx cert" should "be read from file into KeyCert type" in {
    val certPath = this.getClass.getResource("/certs/cert.pfx").getPath
    val cert = CertUtils.readPfx(certPath, "")
    cert.key.getFormat.equals("PKCS#8")
    cert.cert.getNotAfter.toString.equals("Thu Aug 12 18:14:31 PDT 2021")
  }

  "pfx password protected cert" should "be read from file into KeyCert type" in {
    val certPasswordProtectedPath = this.getClass.getResource("/certs/cert-password-protected.pfx").getPath
    val cert = CertUtils.readPfx(certPasswordProtectedPath, "testing")
    cert.key.getFormat.equals("PKCS#8")
    cert.cert.getNotAfter.toString.equals("Thu Aug 12 18:14:31 PDT 2021")
  }

  it should "throw UnrecoverableKeyException if pfx cert does not have private key" in {
    val certPasswordProtectedPath = this.getClass.getResource("/certs/cert-no-privatekey.pfx").getPath
    assertThrows[UnrecoverableKeyException] {
      CertUtils.readPfx(certPasswordProtectedPath, "")
    }
  }
}

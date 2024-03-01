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

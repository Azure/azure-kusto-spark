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

import java.io.{FileInputStream, IOException}
import java.security._
import java.security.cert.{CertificateException, X509Certificate}

object CertUtils {

  case class KeyCert(cert: X509Certificate, key: PrivateKey)

  /**
   * Read pfx file and get privateKey
   *
   * @param path
   *   pfx file path
   * @param password
   *   the password to the pfx file
   */
  @throws[NoSuchProviderException]
  @throws[KeyStoreException]
  @throws[IOException]
  @throws[NoSuchAlgorithmException]
  @throws[CertificateException]
  @throws[UnrecoverableKeyException]
  def readPfx(path: String, password: String): CertUtils.KeyCert = {
    val stream = new FileInputStream(path)
    try {
      // Access Java keystore
      val store = KeyStore.getInstance("pkcs12", "SunJSSE")
      // Load Java Keystore with password for access
      store.load(stream, password.toCharArray)
      // Iterate over all aliases to find the private key
      val aliases = store.aliases
      var alias: Option[String] = Option.empty
      // Break if alias refers to a private key because we want to use that
      // certificate
      while (aliases.hasMoreElements && alias.isEmpty) {
        val currentAlias = aliases.nextElement
        if (store.isKeyEntry(currentAlias)) {
          alias = Option.apply(currentAlias)
        }
      }
      // Retrieves the certificate from the Java keystore
      if (alias.isDefined) {
        val certificate = store.getCertificate(alias.get).asInstanceOf[X509Certificate]
        // Retrieves the private key from the Java keystore
        val key = store.getKey(alias.get, password.toCharArray).asInstanceOf[PrivateKey]
        KeyCert(certificate, key)
      } else {
        throw new UnrecoverableKeyException(s"cert could not be read from pfx path ${path}")
      }
    } finally {
      if (stream != null) {
        stream.close()
      }
    }
  }
}

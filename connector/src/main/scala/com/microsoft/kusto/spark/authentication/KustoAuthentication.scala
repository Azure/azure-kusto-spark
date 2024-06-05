// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.authentication

import com.microsoft.kusto.spark.utils.KustoConstants

import java.util.concurrent.Callable

trait KustoAuthentication {
  def canEqual(that: Any): Boolean

  override def equals(that: Any): Boolean = that match {
    case auth: KustoAuthentication => auth.canEqual(this) && auth == this
    case _ => false
  }

  override def toString: String = KustoConstants.EmptyString

  override def hashCode(): Int = this.hashCode()
}

abstract class KeyVaultAuthentication(uri: String, authority: String) extends KustoAuthentication

case class AadApplicationAuthentication(ID: String, password: String, authority: String)
    extends KustoAuthentication {
  def canEqual(that: Any): Boolean = that.isInstanceOf[AadApplicationAuthentication]

  override def equals(that: Any): Boolean = that match {
    case auth: AadApplicationAuthentication => ID == auth.ID && authority == auth.authority
    case _ => false
  }

  override def hashCode(): Int = ID.hashCode + (if (authority == null) 0 else authority.hashCode)
}

case class ManagedIdentityAuthentication(clientId: Option[String]) extends KustoAuthentication {
  def canEqual(that: Any): Boolean = that.isInstanceOf[ManagedIdentityAuthentication]

  override def equals(that: Any): Boolean = that match {
    case auth: ManagedIdentityAuthentication => clientId == auth.clientId
    case _ => false
  }

  override def hashCode(): Int = if (clientId.isDefined) clientId.hashCode() else 0
}

case class AadApplicationCertificateAuthentication(
    appId: String,
    certFilePath: String,
    certPassword: String,
    authority: String)
    extends KustoAuthentication {
  def canEqual(that: Any): Boolean = that.isInstanceOf[AadApplicationCertificateAuthentication]

  override def equals(that: Any): Boolean = that match {
    case auth: AadApplicationCertificateAuthentication =>
      appId == auth.appId && certFilePath == auth.certFilePath && certPassword == auth.certPassword && authority == auth.authority
    case _ => false
  }

  override def hashCode(): Int = appId.hashCode + certFilePath.hashCode + certPassword.hashCode()
}

final case class KeyVaultAppAuthentication(
    uri: String,
    keyVaultAppID: String,
    keyVaultAppKey: String,
    authority: String)
    extends KeyVaultAuthentication(uri, authority) {
  def canEqual(that: Any): Boolean = that.isInstanceOf[KeyVaultAppAuthentication]

  override def equals(that: Any): Boolean = that match {
    case auth: KeyVaultAppAuthentication => uri == auth.uri && keyVaultAppID == auth.keyVaultAppID
    case _ => false
  }

  override def hashCode(): Int = uri.hashCode + keyVaultAppID.hashCode
}

final case class KeyVaultCertificateAuthentication(
    uri: String,
    pemFilePath: String,
    pemFilePassword: String,
    authority: String)
    extends KeyVaultAuthentication(uri, authority) {
  def canEqual(that: Any): Boolean = that.isInstanceOf[KeyVaultCertificateAuthentication]

  override def equals(that: Any): Boolean = that match {
    case auth: KeyVaultCertificateAuthentication =>
      uri == auth.uri && pemFilePath == auth.pemFilePath
    case _ => false
  }

  override def hashCode(): Int = uri.hashCode + pemFilePath.hashCode
}

case class KustoAccessTokenAuthentication(token: String) extends KustoAuthentication {
  def canEqual(that: Any): Boolean = that.isInstanceOf[KustoAccessTokenAuthentication]

  override def equals(that: Any): Boolean = that match {
    case auth: KustoAccessTokenAuthentication => token == auth.token
    case _ => false
  }

  override def hashCode(): Int = token.hashCode
}

case class KustoTokenProviderAuthentication(tokenProviderCallback: Callable[String])
    extends KustoAuthentication {
  def canEqual(that: Any): Boolean = that.isInstanceOf[KustoTokenProviderAuthentication]

  override def equals(that: Any): Boolean = that match {
    case auth: KustoTokenProviderAuthentication =>
      tokenProviderCallback == auth.tokenProviderCallback
    case _ => false
  }

  override def hashCode(): Int = tokenProviderCallback.hashCode
}

case class KustoUserPromptAuthentication(authority: String) extends KustoAuthentication {
  def canEqual(that: Any): Boolean = that.isInstanceOf[KustoUserPromptAuthentication]

  override def equals(that: Any): Boolean = that match {
    case _: KustoUserPromptAuthentication => true
    case _ => false
  }

  override def hashCode(): Int =
    "KustoUserPromptAuthentication".hashCode + (if (authority == null) 0 else authority.hashCode)
}

package com.microsoft.kusto.spark.authentication

trait KustoAuthentication {
  def canEqual(that: Any) : Boolean
  override def equals(that: Any) : Boolean = that match {
    case auth : KustoAuthentication => auth.canEqual(this) && auth == this
    case _ => false
  }
}

abstract class KeyVaultAuthentication(uri: String) extends KustoAuthentication

case class AadApplicationAuthentication(ID: String, password: String, authority: String) extends KustoAuthentication {
  def canEqual(that: Any) : Boolean = that.isInstanceOf[AadApplicationAuthentication]
  override def equals(that: Any) : Boolean = that match {
    case auth : AadApplicationAuthentication => ID == auth.ID && authority == auth.authority
    case _ => false
  }

  override def hashCode(): Int = ID.hashCode + authority.hashCode
}

case class KeyVaultAppAuthentication(uri: String, keyVaultAppID: String, keyVaultAppKey: String) extends KeyVaultAuthentication(uri) {
  def canEqual(that: Any) : Boolean = that.isInstanceOf[AadApplicationAuthentication]
  override def equals(that: Any) : Boolean = that match {
    case auth : KeyVaultAppAuthentication => uri == auth.uri && keyVaultAppID == auth.keyVaultAppID
    case _ => false
  }

  override def hashCode(): Int = uri.hashCode + keyVaultAppID.hashCode
}

case class KeyVaultCertificateAuthentication(uri: String, pemFilePath: String, pemFilePassword: String) extends KeyVaultAuthentication(uri) {
  def canEqual(that: Any) : Boolean = that.isInstanceOf[AadApplicationAuthentication]
  override def equals(that: Any) : Boolean = that match {
    case auth : KeyVaultCertificateAuthentication => uri == auth.uri && pemFilePath == auth.pemFilePath
    case _ => false
  }

  override def hashCode(): Int = uri.hashCode + pemFilePath.hashCode
}

case class KustoAccessTokenAuthentication(token: String) extends KustoAuthentication {
  def canEqual(that: Any) : Boolean = that.isInstanceOf[AadApplicationAuthentication]
  override def equals(that: Any) : Boolean = that match {
    case auth : KustoAccessTokenAuthentication => token == auth.token
    case _ => false
  }

  override def hashCode(): Int = token.hashCode
}


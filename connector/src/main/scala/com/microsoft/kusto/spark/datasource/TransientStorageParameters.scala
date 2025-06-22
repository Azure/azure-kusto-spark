// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils
import org.apache.commons.lang3.StringUtils

import java.security.InvalidParameterException
import scala.util.matching.Regex
class TransientStorageParameters(
    val storageCredentials: scala.Array[TransientStorageCredentials],
    var endpointSuffix: String = KustoDataSourceUtils.defaultDomainPostfix) {

  // C'tor for serialization
  def this() {
    this(Array())
  }

  override def toString: String = {
    storageCredentials
      .map(tsc => tsc.toString)
      .mkString("[", System.lineSeparator(), s", domain: $endpointSuffix]")
  }

  def toInsecureString: String = {
    new ObjectMapper()
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(this)
  }

}

final case class TransientStorageCredentials() {
  var blobContainer: String = _
  var storageAccountName: String = _
  var storageAccountKey: String = _
  var sasKey: String = _

  // TODO next breaking - change to Option[String]
  var sasUrl: String = _
  var domainSuffix: String = _

  def this(storageAccountName: String, storageAccountKey: String, blobContainer: String) {
    this()
    this.blobContainer = blobContainer
    this.storageAccountName = storageAccountName
    this.storageAccountKey = storageAccountKey
    validate()
  }

  def this(sas: String) {
    this()
    sasUrl = sas
    parseSas(sas)
  }

  def authMethod: AuthMethod.AuthMethod = {
    sasKey match {
      case null => AuthMethod.Key
      case TransientStorageParameters.ImpersonationString => AuthMethod.Impersonation
      case _ => AuthMethod.Sas
    }
  }

  def validate(): Unit = {
    authMethod match {
      case AuthMethod.Sas =>
        if (sasUrl.isEmpty) throw new InvalidParameterException("sasUrl is null or empty")
      case AuthMethod.Key =>
        if (StringUtils.isBlank(storageAccountName)) {
          throw new InvalidParameterException("storageAccount name is null or empty")
        }
        if (StringUtils.isBlank(storageAccountKey)) {
          throw new InvalidParameterException("storageAccount key is null or empty")
        }
        if (StringUtils.isBlank(blobContainer)) {
          throw new InvalidParameterException("blob container name is null or empty")
        }
      case _ =>
    }
  }

  private[kusto] def parseSas(url: String): Unit = {
    url match {
      case TransientStorageCredentials.SasPattern(
            storageAccountName,
            maybeZone,
            cloud,
            container,
            sasKey) =>
        this.storageAccountName = storageAccountName + (if (maybeZone == null) "" else maybeZone)
        this.blobContainer = container
        this.sasKey = sasKey
        domainSuffix = cloud
      case _ =>
        throw new InvalidParameterException(
          "SAS url couldn't be parsed. Should be https://<storage-account>.blob.<domainEndpointSuffix>/<container>?<SAS-Token>")
    }
  }

  override def toString: String = {
    // TODO next breaking - change to "authMethod:"
    s"BlobContainer: $blobContainer ,Storage: $storageAccountName , IsSasKeyDefined: ${authMethod == AuthMethod.Sas}"
  }

}

object TransientStorageParameters {
  val ImpersonationString = ";impersonate"
  private[kusto] def fromString(json: String): TransientStorageParameters = {
    new ObjectMapper()
      .registerModule(new JavaTimeModule())
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .readValue(json, classOf[TransientStorageParameters])
  }
}

object TransientStorageCredentials {
  val SasPattern: Regex =
    raw"https:\/\/([^.]+)(\.[^.]+)?\.blob\.([^\/]+)\/([^?]+)(;impersonate|[\?].+)".r
}

object AuthMethod extends Enumeration {
  type AuthMethod = Value
  val Sas, Key, Impersonation = Value
}

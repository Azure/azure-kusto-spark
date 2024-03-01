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
    var endpointSuffix: String = KustoDataSourceUtils.DefaultDomainPostfix) {

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

  def sasDefined: Boolean = {
    sasUrl != null
  }

  def validate(): Unit = {
    if (sasDefined) {
      if (sasUrl.isEmpty) {
        throw new InvalidParameterException("sasUrl is null or empty")
      }
    } else {
      if (StringUtils.isBlank(storageAccountName)) {
        throw new InvalidParameterException("storageAccount name is null or empty")
      }
      if (StringUtils.isBlank(storageAccountKey)) {
        throw new InvalidParameterException("storageAccount key is null or empty")
      }
      if (StringUtils.isBlank(blobContainer)) {
        throw new InvalidParameterException("blob container name is null or empty")
      }
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
    s"BlobContainer: $blobContainer ,Storage: $storageAccountName , IsSasKeyDefined: $sasDefined"
  }

}

object TransientStorageParameters {
  private[kusto] def fromString(json: String): TransientStorageParameters = {
    new ObjectMapper()
      .registerModule(new JavaTimeModule())
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
      .readValue(json, classOf[TransientStorageParameters])
  }
}

object TransientStorageCredentials {
  private val SasPattern: Regex =
    raw"https:\/\/([^.]+)(\.[^.]+)?\.blob\.([^\/]+)\/([^?]+)(\?.+)".r
}

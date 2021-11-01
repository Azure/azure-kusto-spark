package com.microsoft.kusto.spark.datasource

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils
import org.apache.commons.lang3.StringUtils
import org.apache.htrace.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.codehaus.jackson.JsonParser
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility
import org.codehaus.jackson.annotate.JsonMethod
import org.codehaus.jackson.map.{DeserializationContext, JsonDeserializer, ObjectMapper}
import py4j.StringUtil

import java.security.InvalidParameterException
import scala.util.matching.Regex

class TransientStorageParameters(val storageCredentials: Array[TransientStorageCredentials],
                                 var endpointSuffix: String = KustoDataSourceUtils.DefaultDomainPostfix){

  // C'tor for serialization
  def this(){
    this(Array())
  }

  override def toString: String = {
    new ObjectMapper().setVisibility(JsonMethod.FIELD, Visibility.ANY)
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(this)
  }
}

case class TransientStorageCredentials() {
  var blobContainer: String = _
  var storageAccountName: String = _
  var storageAccountKey: String = _
  var sasKey: String = _
  var sasUrl: String = _
  var domainSuffix: String = _

  def this(storageAccountName: String,  storageAccountKey: String, blobContainer: String) {
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

  def isSas: Boolean = {
    sasUrl != null;
  }

  def validate(): Unit = {
    if (isSas) {
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
      case TransientStorageCredentials.SasPattern(storageAccountName, cloud, container, sasKey) => {
        this.storageAccountName = storageAccountName
        this.blobContainer = container
        this.sasKey = sasKey
        domainSuffix = cloud
      }
      case _ => throw new InvalidParameterException(
        "SAS url couldn't be parsed. Should be https://<storage-account>.blob.<domainEndpointSuffix>/<container>?<SAS-Token>"
      )
    }
  }
}

object TransientStorageParameters {
  private[kusto] def fromString(json: String): TransientStorageParameters = {
    new ObjectMapper().setVisibility(JsonMethod.FIELD, Visibility.ANY).readValue(json, classOf[TransientStorageParameters])
  }
}

object TransientStorageCredentials {
  val SasPattern: Regex = raw"(?:https://)?([^.]+).blob.([^/]+)/([^?]+)?(.+)".r
}

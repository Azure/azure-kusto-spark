// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.kusto.spark.datasink

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper

import java.io.Serializable
import java.util.Objects
import scala.util.Random

object IngestionStorageParameters extends Serializable {
  private final val objectMapper = new ObjectMapper()
    .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
    .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)

  private[kusto] def fromString(json: String): Array[IngestionStorageParameters] = {
    objectMapper.readValue(json, classOf[Array[IngestionStorageParameters]])
  }

  def toJsonString(
      storageParams: Array[IngestionStorageParameters]): String = {
    if (Objects.isNull(storageParams)|| storageParams.isEmpty) {
      throw new IllegalArgumentException("storageParams cannot be null or empty")
    }
    objectMapper.writeValueAsString(storageParams)
  }

  private[kusto] def getRandomIngestionStorage(
      storageParams: Array[IngestionStorageParameters]): IngestionStorageParameters = {
    if (storageParams == null || storageParams.isEmpty) {
      throw new IllegalArgumentException("storageParams cannot be null or empty")
    }
    storageParams(Random.nextInt(storageParams.length))
  }
}

class IngestionStorageParameters(
    val storageUrl: String,
    val containerName: String,
    val userMsi: String)
    extends Serializable {
  // C'tor for serialization
  def this() {
    this("", "", "")
  }

  def getStorageUrl: String = {
    s"$storageUrl/$containerName"
  }
  override def toString: String = {
    s"storageUrl: $storageUrl, containerName: $containerName, userMsi: $userMsi"
  }
}

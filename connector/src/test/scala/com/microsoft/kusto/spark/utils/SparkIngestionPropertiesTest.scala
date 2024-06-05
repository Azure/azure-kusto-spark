// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.kusto.spark.datasink.SparkIngestionProperties
import org.apache.commons.lang3.builder.EqualsBuilder

import java.time.{Clock, Instant}
import org.scalatest.flatspec.AnyFlatSpec

class SparkIngestionPropertiesTest extends AnyFlatSpec {

  "props" should "be same after clone" in {
    val ingestByTags = new java.util.ArrayList[String]
    val tag = "dammyTag"
    ingestByTags.add(tag)

    val sp = new SparkIngestionProperties(
      flushImmediately = true,
      csvMappingNameReference = "mapy",
      ingestByTags = ingestByTags,
      creationTime = Instant.now(Clock.systemUTC()),
      ingestIfNotExists = ingestByTags,
      additionalTags = ingestByTags,
      csvMapping = "[{\"Column\": \"a\", \"Properties\": {\"Ordinal\": \"0\"}}]")
    val stringProps = sp.toString
    val spFromString = SparkIngestionProperties.fromString(stringProps)

    assert(EqualsBuilder.reflectionEquals(spFromString, sp))
    val ingestByTags2 = new java.util.ArrayList[String]
    val tag2 = "dammyTag2"
    ingestByTags.add(tag2)
    sp.ingestByTags = ingestByTags2
    assert(!EqualsBuilder.reflectionEquals(spFromString, sp))

    val ingestionProperties = spFromString
      .toIngestionProperties("database", "tableName")
    val cloned = SparkIngestionProperties.cloneIngestionProperties(ingestionProperties)
    assert(EqualsBuilder.reflectionEquals(ingestionProperties, cloned))
  }
}

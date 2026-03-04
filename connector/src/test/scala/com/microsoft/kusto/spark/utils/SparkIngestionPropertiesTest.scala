// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.kusto.spark.datasink.SparkIngestionProperties
import org.apache.commons.lang3.builder.EqualsBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

import java.time.{Clock, Instant}
import java.util.Collections

class SparkIngestionPropertiesTest extends AnyFlatSpec {

  "props" should "be same after clone" in {
    val ingestByTags = new java.util.ArrayList[String]
    val tag = "dummyTag"
    ingestByTags.add(tag)

    val sp = new SparkIngestionProperties(
      flushImmediately = true,
      csvMappingNameReference = Some("mapy"),
      ingestByTags = Some(ingestByTags),
      creationTime = Some(Instant.now(Clock.systemUTC())),
      ingestIfNotExists = Some(ingestByTags),
      additionalTags = Some(ingestByTags),
      csvMapping = Some("[{\"Column\": \"a\", \"Properties\": {\"Ordinal\": \"0\"}}]"))
    val stringProps = sp.toString
    val spFromString = SparkIngestionProperties.fromString(stringProps)

    assert(EqualsBuilder.reflectionEquals(spFromString, sp))
    val ingestByTags2 = new java.util.ArrayList[String]
    val tag2 = "dummyTag2"
    ingestByTags.add(tag2)
    sp.ingestByTags = Some(ingestByTags2)
    assert(!EqualsBuilder.reflectionEquals(spFromString, sp))

    val ingestionProperties = spFromString
      .toIngestionProperties("database", "tableName")
    val cloned = SparkIngestionProperties.cloneIngestionProperties(ingestionProperties)
    assert(EqualsBuilder.reflectionEquals(ingestionProperties, cloned))
  }

  // This will be called only for WriteOption "Stream"
  "validateStreamingProperties" should "validate unsupported properties" in {
    val testCombinations =
      Table(
        ("ingestByTags", "dropByTags", "additionalTags", "creationTime", "isInvalid"),
        (
          Some(Collections.emptyList[String]()),
          Some(Collections.emptyList[String]()),
          Some(Collections.emptyList[String]()),
          Some(Instant.now(Clock.systemUTC())),
          true),
        (
          Some(Collections.singletonList("ingestTag")),
          Some(Collections.emptyList[String]()),
          Some(Collections.emptyList[String]()),
          None,
          true),
        (
          Some(Collections.emptyList[String]()),
          Some(Collections.singletonList("dropTag")),
          Some(Collections.emptyList[String]()),
          None,
          true),
        (
          Some(Collections.singletonList("ingestTag")),
          Some(Collections.singletonList("dropTag")),
          Some(Collections.singletonList("additionalTag")),
          Some(Instant.now(Clock.systemUTC())),
          true),
        (
          Some(Collections.emptyList[String]()),
          Some(Collections.emptyList[String]()),
          Some(Collections.emptyList[String]()),
          None,
          false))
    forAll(testCombinations) {
      (ingestByTags, dropByTags, additionalTags, creationTime, isInvalid) =>
        val sp = new SparkIngestionProperties(false)
        // Set these properties
        sp.ingestByTags = ingestByTags
        sp.dropByTags = dropByTags
        sp.creationTime = creationTime
        sp.additionalTags = additionalTags
        if (isInvalid) {
          assertThrows[IllegalArgumentException](sp.validateStreamingProperties())
        }
    }
  }
}

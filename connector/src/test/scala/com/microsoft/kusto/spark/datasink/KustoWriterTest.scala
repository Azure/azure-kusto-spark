// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.auth.CloudInfo
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind
import com.microsoft.azure.kusto.ingest.IngestionProperties
import com.microsoft.kusto.spark.authentication.KustoUserPromptAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

import java.time.{Clock, Instant}
import java.util.{Collections, UUID}

class KustoWriterTest extends AnyFunSuiteLike {

  private val tableName = "ut-table"
  private val dbName = "database"
  private val cloudInfo =
    new CloudInfo(false, "login-endpoint", "app-id", "app-key", "svc-res-id", "1P-Url")
  private val structType = new StructType()
  private val kustoCoordinates = KustoCoordinates(
    clusterUrl = "cluster",
    database = dbName,
    table = Some(tableName),
    clusterAlias = "test-cluster",
    ingestionUrl = None)

  private val dropByTags = Collections.singletonList("drop-by-tag")
  private val ingestByTags = Collections.singletonList("ingest-by-tag")
  private val additionalTags = Collections.singletonList("additional-tag")
  private val ingestIfNotExistsTag = Collections.singletonList("ingest-if-not-exists")

  private val writeOptions = WriteOptions()
  private val sip = new SparkIngestionProperties(
    flushImmediately = false,
    dropByTags = dropByTags,
    ingestByTags = ingestByTags,
    additionalTags = additionalTags,
    ingestIfNotExists = ingestIfNotExistsTag,
    creationTime = Instant.now(Clock.systemUTC()),
    csvMappingNameReference = "csvMappingRef")

  private val testCases = Table(
    ("coordinates", "writeOptions"),
    // Add your test cases here. Example:
    (kustoCoordinates, writeOptions.copy(writeMode = WriteMode.Transactional)),
    (kustoCoordinates, writeOptions.copy(writeMode = WriteMode.Queued)),
    (kustoCoordinates, writeOptions.copy(writeMode = WriteMode.KustoStreaming)),
    (
      kustoCoordinates,
      writeOptions.copy(
        writeMode = WriteMode.Transactional,
        maybeSparkIngestionProperties = Some(sip))),
    (
      kustoCoordinates,
      writeOptions.copy(
        writeMode = WriteMode.KustoStreaming,
        maybeSparkIngestionProperties = Some(sip))),
    (
      kustoCoordinates,
      writeOptions.copy(writeMode = WriteMode.Queued, maybeSparkIngestionProperties = Some(sip)))
    // Add more test cases as needed
  )

  forAll(testCases) { (kustoCoordinates: KustoCoordinates, writeOptions: WriteOptions) =>
    test(
      s"Test for mode: ${writeOptions.writeMode.toString} with SIP: ${writeOptions.maybeSparkIngestionProperties.isDefined}") {
      val tempTableName = s"${UUID.randomUUID().toString}-${kustoCoordinates.table.getOrElse("")}"
      val kustoWriteResource = KustoWriteResource(
        authentication = KustoUserPromptAuthentication("microsoft.com"),
        coordinates = kustoCoordinates,
        schema = structType,
        writeOptions = writeOptions,
        tmpTableName = tempTableName,
        cloudInfo = cloudInfo)

      val actualIngestionProperties = KustoWriter.getIngestionProperties(kustoWriteResource)
      assert(actualIngestionProperties.getDatabaseName == dbName)
      assert(actualIngestionProperties.getDataFormat == IngestionProperties.DataFormat.CSV)
      // for transactional mode this should match the temp table name which is used
      if (writeOptions.writeMode == WriteMode.Transactional) {
        assert(actualIngestionProperties.getTableName == tempTableName)
        assert(
          actualIngestionProperties.getReportLevel == IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES)
        assert(
          actualIngestionProperties.getReportMethod == IngestionProperties.IngestionReportMethod.TABLE)
        assert(actualIngestionProperties.getTableName == tempTableName)

      } else {
        assert(actualIngestionProperties.getTableName == tableName)
      }
      // SIP props are independent
      if (writeOptions.maybeSparkIngestionProperties.isDefined) {
        assert(
          actualIngestionProperties.getFlushImmediately == writeOptions.maybeSparkIngestionProperties.get.flushImmediately)
        assert(
          actualIngestionProperties.getDropByTags == writeOptions.maybeSparkIngestionProperties.get.dropByTags)
        assert(
          actualIngestionProperties.getIngestByTags == writeOptions.maybeSparkIngestionProperties.get.ingestByTags)
        assert(
          actualIngestionProperties.getAdditionalTags == writeOptions.maybeSparkIngestionProperties.get.additionalTags)
        assert(
          actualIngestionProperties.getIngestIfNotExists == writeOptions.maybeSparkIngestionProperties.get.ingestIfNotExists)
        assert(
          actualIngestionProperties.getIngestionMapping.getIngestionMappingReference ==
            writeOptions.maybeSparkIngestionProperties.get.csvMappingNameReference)
        assert(
          actualIngestionProperties.getIngestionMapping.getIngestionMappingKind == IngestionMappingKind.CSV)
      }
    }
  }
}

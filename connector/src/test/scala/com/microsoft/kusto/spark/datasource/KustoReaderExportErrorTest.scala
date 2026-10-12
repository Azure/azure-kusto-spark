// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.kusto.data.{
  Client,
  ClientRequestProperties,
  KustoOperationResult,
  KustoResultSetTable
}
import com.microsoft.azure.kusto.data.exceptions.DataServiceException
import com.microsoft.kusto.spark.authentication.KustoAccessTokenAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.exceptions.{
  FailedOperationException,
  TimeoutAwaitingPendingOperationException
}
import com.microsoft.kusto.spark.utils.ExtendedKustoClient
import io.github.resilience4j.retry.RetryConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.MockMakers
import org.mockito.Mockito.{mock, never, times, verify, when, withSettings}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class KustoReaderExportErrorTest extends AnyFlatSpec with Matchers {
  private val database = "test-database"
  private val requestId = "test-read-request"
  private val operationId = "11111111-2222-3333-4444-555555555555"
  private val query = "SensitiveSource | where value == 'private-query-value'"
  private val sas = "sig=private-sas-value"
  private val capacityStatus =
    "The control command was aborted due to throttling. Retrying after some backoff might succeed. " +
      "CommandType: 'DataExportToFile', Capacity: 1, Origin: 'CapacityPolicy/Export'."
  private val mapper = new ObjectMapper()

  private def response(columns: Seq[String], values: Seq[String]): KustoOperationResult = {
    val columnJson = columns
      .map(name => s"""{"ColumnName":"$name","DataType":"String","ColumnType":"string"}""")
      .mkString(",")
    val rowJson = values.map(value => mapper.writeValueAsString(value)).mkString(",")
    new KustoOperationResult(
      s"""{"Tables":[{"TableName":"Table_0","Columns":[$columnJson],"Rows":[[$rowJson]]}]}""",
      "v1")
  }

  private class Fixture(state: String, status: String) {
    val engine: Client = mock(classOf[Client])
    val submission: KustoOperationResult = response(Seq("OperationId"), Seq(operationId))
    val completion: KustoOperationResult =
      response(Seq("OperationId", "State", "Status"), Seq(operationId, state, status))
    val result: KustoResultSetTable = completion.getPrimaryResults
    val client: ExtendedKustoClient = new ExtendedKustoClient(null, null, "testcluster") {
      override lazy val engineClient: Client = engine
    }

    when(engine.executeMgmt(anyString(), anyString(), any(classOf[ClientRequestProperties])))
      .thenReturn(submission)
    when(engine.executeMgmt(database, s".show operations $operationId")).thenReturn(completion)

    def verifySingleAttempt(): Unit = {
      verify(engine, times(1)).executeMgmt(
        anyString(),
        anyString(),
        any(classOf[ClientRequestProperties]))
      verify(engine, times(1)).executeMgmt(database, s".show operations $operationId")
    }
  }

  private def runExport(client: ExtendedKustoClient): Unit = {
    val spark = mock(classOf[SparkSession], withSettings().mockMaker(MockMakers.SUBCLASS))
    when(spark.version).thenReturn("4.0.0")
    val request = KustoReadRequest(
      spark,
      KustoSchema(StructType(Seq(StructField("value", StringType))), Set.empty),
      KustoCoordinates("https://testcluster.kusto.windows.net", "testcluster", database, None),
      query,
      KustoAccessTokenAuthentication("private-access-token"),
      10.seconds,
      Some(new ClientRequestProperties()),
      requestId)
    val storage = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials(
          s"https://teststorage.blob.core.windows.net/testcontainer?$sas")))
    new KustoReader(client).exportPartitionToBlob(
      KustoPartition(None, 0),
      request,
      storage,
      "test-directory/",
      KustoReadOptions(
        partitionOptions = PartitionOptions(1, None, None),
        queryFilterPushDown = None),
      KustoFiltering())
  }

  private def failingClient(error: Throwable): ExtendedKustoClient =
    new ExtendedKustoClient(null, null, "testcluster") {
      override def executeEngine(
          database: String,
          command: String,
          activityName: String,
          crp: ClientRequestProperties,
          isMgmtCommand: Boolean,
          retryConfig: Option[RetryConfig]): KustoOperationResult = throw error
    }

  "exportPartitionToBlob" should "explain export-capacity throttling during a Spark read" in {
    val fixture = new Fixture("Throttled", capacityStatus)

    val error = intercept[FailedOperationException](runExport(fixture.client))

    error.getMessage should include("Kusto Spark read failed")
    error.getMessage should include("concurrent export capacity was exceeded")
    error.getMessage should include("temporary storage")
    error.getMessage should include("Reduce concurrent reads or exports")
    error.getMessage should include(requestId)
    error.getMessage should include(operationId)
    error.getMessage should include(capacityStatus)
    error.getClass shouldBe classOf[FailedOperationException]
    val original = error.getCause.asInstanceOf[FailedOperationException]
    error.getMessage should include(original.getMessage)
    error.getResult.get should be theSameInstanceAs fixture.result
    original.getResult.get should be theSameInstanceAs fixture.result
    fixture.result.getString("State") shouldBe "Throttled"
    error.getMessage should not include query
    error.getMessage should not include sas
    error.getMessage should not include "private-access-token"
    fixture.verifySingleAttempt()
  }

  it should "leave a completed export successful without resubmitting it" in {
    val fixture = new Fixture("Completed", "")

    runExport(fixture.client)

    fixture.verifySingleAttempt()
  }

  it should "avoid attributing other or unknown throttling origins to export capacity" in {
    Seq(
      "CommandType: 'DataExportToFile', Origin: 'RequestRateLimitPolicy'.",
      "CommandType: 'DataExportToFile', Origin: 'CapacityPolicy/ExportExtra'.",
      "CapacityPolicy/Export was mentioned without an origin field",
      "CommandType: 'DataExportToFile', OtherOrigin: 'CapacityPolicy/Export'.",
      "",
      null).foreach { status =>
      val fixture = new Fixture("Throttled", status)

      val error = intercept[FailedOperationException](runExport(fixture.client))

      error.getMessage should include("distributed export was throttled")
      error.getMessage should include("temporary storage")
      error.getMessage should not include "concurrent export capacity was exceeded"
      fixture.verifySingleAttempt()
    }
  }

  it should "not infer throttling from status text when the operation has another state" in {
    Seq("Failed", "Canceled", "PartiallySucceeded", "BadInput", "Abandoned").foreach { state =>
      val fixture = new Fixture(state, capacityStatus)

      val error = intercept[FailedOperationException](runExport(fixture.client))

      error.getMessage should startWith("Kusto Spark read failed during a distributed export.")
      error.getMessage should include(s"State: '$state'")
      error.getMessage should not include "Reduce concurrent reads or exports"
      error.getResult.get.getString("State") shouldBe state
      fixture.verifySingleAttempt()
    }
  }

  it should "preserve failures without result metadata rather than classify message text" in {
    val original = new FailedOperationException(capacityStatus, None)

    val error = intercept[FailedOperationException](runExport(failingClient(original)))

    error.getMessage should startWith("Kusto Spark read failed during a distributed export.")
    error.getMessage should include(capacityStatus)
    error.getCause should be theSameInstanceAs original
    error.getResult shouldBe None
  }

  it should "not mask the original failure when state metadata is malformed" in {
    val table = response(Seq("Status"), Seq(capacityStatus)).getPrimaryResults
    table.next()
    val original = new FailedOperationException("original operation failure", Some(table))

    val error = intercept[FailedOperationException](runExport(failingClient(original)))

    error.getMessage should startWith("Kusto Spark read failed during a distributed export.")
    error.getMessage should include(original.getMessage)
    error.getCause should be theSameInstanceAs original
    error.getResult.get should be theSameInstanceAs table
    table.getString("Status") shouldBe capacityStatus
  }

  it should "retain known throttling when only status metadata is unreadable" in {
    val table = response(Seq("State"), Seq("Throttled")).getPrimaryResults
    table.next()
    val original = new FailedOperationException("original throttling failure", Some(table))

    val error = intercept[FailedOperationException](runExport(failingClient(original)))

    error.getMessage should include("distributed export was throttled")
    error.getMessage should not include "concurrent export capacity was exceeded"
    error.getCause should be theSameInstanceAs original
    table.getString("State") shouldBe "Throttled"
  }

  it should "leave SDK submission errors unchanged" in {
    val fixture = new Fixture("Completed", "")
    val original = new DataServiceException("testcluster", "permission denied", true)
    when(
      fixture.engine.executeMgmt(anyString(), anyString(), any(classOf[ClientRequestProperties])))
      .thenThrow(original)

    val error = intercept[DataServiceException](runExport(fixture.client))

    error should be theSameInstanceAs original
    verify(fixture.engine, times(1)).executeMgmt(
      anyString(),
      anyString(),
      any(classOf[ClientRequestProperties]))
    verify(fixture.engine, never()).executeMgmt(database, s".show operations $operationId")
  }

  it should "leave timeout failures unchanged" in {
    val original = new TimeoutAwaitingPendingOperationException("operation timed out")

    val error =
      intercept[TimeoutAwaitingPendingOperationException](runExport(failingClient(original)))

    error should be theSameInstanceAs original
  }

  it should "leave interruption unchanged" in {
    val original = new InterruptedException("read canceled")

    val error = intercept[InterruptedException](runExport(failingClient(original)))

    error should be theSameInstanceAs original
  }
}

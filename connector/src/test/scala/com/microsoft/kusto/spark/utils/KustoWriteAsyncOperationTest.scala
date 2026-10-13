// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.kusto.data.{Client, ClientRequestProperties, KustoOperationResult, KustoResultSetTable}
import com.microsoft.azure.kusto.data.exceptions.DataServiceException
import com.microsoft.kusto.spark.datasink.WriteOptions
import com.microsoft.kusto.spark.exceptions.{FailedOperationException, TimeoutAwaitingPendingOperationException}
import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo, startsWith}
import org.mockito.Mockito.{mock, times, verify, verifyNoMoreInteractions, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration._

// Write-helper regressions using the real poller, not the full FinalizeHelper pipeline.
class KustoWriteAsyncOperationTest extends AnyFlatSpec with Matchers {
  private val database = "test-database"
  private val source = "staging"
  private val target = "destination"
  private val dropCommand = CslCommandsGenerator.generateExtentTagsDropByPrefixCommand(source, "dedup")
  private val mvCommand = CslCommandsGenerator.generateIsTableMaterializedViewSourceCommand(target)
  private val movePrefix = s".move async extents to table $target "
  private val mapper = new ObjectMapper()

  private def response(columns: Seq[String], rows: Seq[Seq[String]]): KustoOperationResult = {
    val columnJson = columns
      .map(name => s"""{"ColumnName":"$name","DataType":"String","ColumnType":"string"}""")
      .mkString(",")
    val rowJson = rows
      .map(row => row.map(value => mapper.writeValueAsString(value)).mkString("[", ",", "]"))
      .mkString(",")
    new KustoOperationResult(
      s"""{"Tables":[{"TableName":"Table_0","Columns":[$columnJson],"Rows":[$rowJson]}]}""",
      "v1")
  }

  private class Fixture(states: Seq[String], detailCounts: Seq[Int] = Seq.empty) {
    val engine: Client = mock(classOf[Client])
    val ids: Seq[String] = states.indices.map(i => f"00000000-0000-0000-0000-${i + 1}%012d")
    val submissions = new AtomicInteger()
    val lastStatus = new AtomicReference[KustoResultSetTable]()
    var moveRetryErrors = Vector.empty[Object]
    val client = new ExtendedKustoClient(null, null, "test-cluster") {
      override lazy val engineClient: Client = engine
      override def handleRetryFail(
          curBatchSize: Int,
          retry: Int,
          currentSleepTime: Int,
          targetTable: String,
          error: Object): (Int, Int) = {
        moveRetryErrors :+= error
        super.handleRetryFail(curBatchSize, retry, 0, targetTable, error)
      }
    }

    def pollCommand(index: Int): String = CslCommandsGenerator.generateOperationsShowCommand(ids(index))
    def detailsCommand(index: Int): String = CslCommandsGenerator.generateShowOperationDetails(ids(index))

    when(engine.executeMgmt(anyString(), anyString(), any[ClientRequestProperties]()))
      .thenAnswer { invocation =>
        val command = invocation.getArgument[String](1)
        if (command == dropCommand || command.startsWith(movePrefix)) {
          val index = submissions.getAndIncrement()
          require(index < ids.size, "Unexpected write resubmission")
          response(Seq("OperationId"), Seq(Seq(ids(index))))
        } else if (command == mvCommand) {
          new KustoOperationResult(
            """{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"Count","DataType":"Int64","ColumnType":"long"}],"Rows":[[0]]}]}""",
            "v1")
        } else {
          val index = ids.indices.find(i => command == detailsCommand(i))
            .getOrElse(throw new IllegalArgumentException(s"Unexpected command: $command"))
          response(
            Seq("SourceExtentId", "TargetExtentId", "Details"),
            (0 until detailCounts(index)).map(i => Seq(s"source-$i", s"target-$i", "")))
        }
      }
    when(engine.executeMgmt(anyString(), anyString())).thenAnswer { invocation =>
      val command = invocation.getArgument[String](1)
      val index = ids.indices.find(i => command == pollCommand(i))
        .getOrElse(throw new IllegalArgumentException(s"Unexpected poll: $command"))
      val result = response(
        Seq("OperationId", "State", "Status"),
        Seq(Seq(ids(index), states(index), s"write-status-$index")))
      lastStatus.set(result.getPrimaryResults)
      result
    }

    def drop(timeout: FiniteDuration = 5.seconds): Option[KustoResultSetTable] =
      client.retryAsyncOp(
        database, dropCommand, new ClientRequestProperties(), timeout,
        "drop dedup tags", "extentsDrop", "write-test")

    def move(totalAmount: Int): Unit =
      client.moveExtentsWithRetries(
        Some(2), totalAmount, database, source, target, Instant.EPOCH,
        new ClientRequestProperties(),
        WriteOptions(
          timeout = 5.seconds,
          requestId = "write-test",
          kustoCustomDebugWriteOptions = KustoCustomDebugWriteOptions(maxRetriesOnMoveExtents = 2)))

    def verifyDropSubmissions(count: Int): Unit =
      verify(engine, times(count)).executeMgmt(eqTo(database), eqTo(dropCommand), any[ClientRequestProperties]())

    def verifyMoveSubmissions(count: Int): Unit = {
      verify(engine).executeMgmt(eqTo(database), eqTo(mvCommand), any[ClientRequestProperties]())
      verify(engine, times(count)).executeMgmt(eqTo(database), startsWith(movePrefix), any[ClientRequestProperties]())
    }
  }

  "retryAsyncOp for dedup-tag removal" should "resubmit a Failed operation and preserve the successful cursor" in {
    val f = new Fixture(Seq("Failed", "Completed"))

    val result = f.drop().get

    result should be theSameInstanceAs f.lastStatus.get()
    result.getString("OperationId") shouldBe f.ids(1)
    result.getString("State") shouldBe "Completed"
    result.getString("Status") shouldBe "write-status-1"
    f.verifyDropSubmissions(2)
    f.ids.indices.foreach(i => verify(f.engine).executeMgmt(database, f.pollCommand(i)))
    verifyNoMoreInteractions(f.engine)
  }

  it should "retain the last terminal result and cursor when the write retries are exhausted" in {
    val f = new Fixture(Seq.fill(KustoConstants.MaxCommandsRetryAttempts)("Failed"))

    val error = intercept[FailedOperationException](f.drop())

    error.getResult.get should be theSameInstanceAs f.lastStatus.get()
    error.getResult.get.getString("OperationId") shouldBe f.ids.last
    error.getResult.get.getString("State") shouldBe "Failed"
    error.getResult.get.getString("Status") shouldBe s"write-status-${f.ids.size - 1}"
    error.getMessage should include(f.ids.last)
    error.getMessage should include(s"write-status-${f.ids.size - 1}")
    f.verifyDropSubmissions(KustoConstants.MaxCommandsRetryAttempts)
    f.ids.indices.foreach(i => verify(f.engine).executeMgmt(database, f.pollCommand(i)))
    verifyNoMoreInteractions(f.engine)
  }

  it should "not resubmit or poll after a zero timeout" in {
    val f = new Fixture(Seq("Completed"))

    val error = intercept[TimeoutAwaitingPendingOperationException](f.drop(Duration.Zero))

    error.getMessage should include(f.ids.head)
    f.verifyDropSubmissions(1)
    verifyNoMoreInteractions(f.engine)
  }

  it should "propagate a permanent polling failure without resubmitting" in {
    val f = new Fixture(Seq("Completed"))
    val cause = new DataServiceException("test-cluster", "permission denied", true)
    when(f.engine.executeMgmt(database, f.pollCommand(0))).thenThrow(cause)

    val error = intercept[Exception](f.drop())

    error.getCause should be theSameInstanceAs cause
    error.getMessage should include(f.pollCommand(0))
    f.verifyDropSubmissions(1)
    verify(f.engine).executeMgmt(database, f.pollCommand(0))
    verifyNoMoreInteractions(f.engine)
  }

  "moveExtentsWithRetries" should "consume successful operation details and counts with or without a Failed retry" in {
    Seq(false, true).foreach { failFirst =>
      val states = (if (failFirst) Seq("Failed") else Seq.empty) ++ Seq("Completed", "Completed")
      val counts = (if (failFirst) Seq(0) else Seq.empty) ++ Seq(2, 1)
      val f = new Fixture(states, counts)

      f.move(totalAmount = 3)

      f.submissions.get() shouldBe states.size
      f.moveRetryErrors shouldBe (if (failFirst) Vector("write-status-0") else Vector.empty)
      f.verifyMoveSubmissions(states.size)
      states.indices.foreach { i =>
        verify(f.engine).executeMgmt(database, f.pollCommand(i))
        if (states(i) == "Completed") {
          verify(f.engine).executeMgmt(eqTo(database), eqTo(f.detailsCommand(i)), any[ClientRequestProperties]())
        }
      }
      verifyNoMoreInteractions(f.engine)
    }
  }

  it should "abort on a permanent polling error without details, count reads, or move retries" in {
    val f = new Fixture(Seq("Completed"))
    val cause = new DataServiceException("test-cluster", "permission denied", true)
    when(f.engine.executeMgmt(database, f.pollCommand(0))).thenThrow(cause)

    val error = intercept[Exception](f.move(totalAmount = 3))

    error.getCause should be theSameInstanceAs cause
    error.getMessage should include(f.pollCommand(0))
    f.moveRetryErrors shouldBe empty
    f.verifyMoveSubmissions(1)
    verify(f.engine).executeMgmt(database, f.pollCommand(0))
    verifyNoMoreInteractions(f.engine)
  }
}
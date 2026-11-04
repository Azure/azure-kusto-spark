// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.kusto.data.{Client, KustoOperationResult, KustoResultSetTable}
import com.microsoft.azure.kusto.data.exceptions.{
  DataClientException,
  DataServiceException,
  ThrottleException
}
import com.microsoft.kusto.spark.exceptions.{
  FailedOperationException,
  TimeoutAwaitingPendingOperationException
}
import org.mockito.Mockito.{mock, times, verify, verifyNoInteractions, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.{Timer, TimerTask}
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class KustoAsyncOperationPollerTest extends AnyFlatSpec with Matchers {
  private val database = "test-database"
  private val operationId = "11111111-2222-3333-4444-555555555555"
  private val command = s".show operations $operationId"
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

  private def submission: KustoResultSetTable =
    response(Seq("OperationId"), Seq(Seq(operationId))).getPrimaryResults

  private def status(state: String, message: String = ""): KustoOperationResult =
    response(Seq("OperationId", "State", "Status"), Seq(Seq(operationId, state, message)))

  private def emptyStatus: KustoOperationResult =
    response(Seq("OperationId", "State", "Status"), Seq.empty)

  // Execute queued tasks explicitly so cancellation and backoff assertions need no sleeps.
  private class ControlledTimer(immediateFirst: Boolean) extends Timer(true) {
    private val pending = new LinkedBlockingQueue[(TimerTask, Long)]()
    private val first = new AtomicBoolean(true)
    val cancelled = new AtomicBoolean(false)
    val rejectedSchedules = new AtomicInteger()

    override def schedule(task: TimerTask, delay: Long): Unit = synchronized {
      if (cancelled.get()) {
        rejectedSchedules.incrementAndGet()
        throw new IllegalStateException("Timer already cancelled")
      }
      if (immediateFirst && first.compareAndSet(true, false)) task.run()
      else pending.add((task, delay))
    }

    override def cancel(): Unit = synchronized {
      cancelled.set(true)
      pending.clear()
      super.cancel()
    }

    def runNext(): Long = {
      val next = pending.poll(5, TimeUnit.SECONDS)
      assert(next != null, "No status poll was scheduled")
      next._1.run()
      next._2
    }

    def pendingCount: Int = pending.size()
  }

  private class Invocation(body: () => Option[KustoResultSetTable]) {
    val result = new AtomicReference[Option[KustoResultSetTable]]()
    val error = new AtomicReference[Throwable]()
    private val done = new CountDownLatch(1)
    val thread = new Thread(
      new Runnable {
        override def run(): Unit = {
          try result.set(body())
          catch { case failure: Throwable => error.set(failure) }
          finally done.countDown()
        }
      },
      "poller-test-caller")
    thread.setDaemon(true)
    thread.start()

    def await(): Unit = {
      assert(done.await(5, TimeUnit.SECONDS), "Poller caller did not finish")
    }
  }

  private class Fixture(immediateFirst: Boolean) {
    val client: Client = mock(classOf[Client])
    val timer = new ControlledTimer(immediateFirst)
    val factoryCalls = new AtomicInteger()
    private val invocations = ArrayBuffer.empty[Invocation]

    def run(timeout: FiniteDuration = (-1).seconds): Option[KustoResultSetTable] =
      KustoDataSourceUtils.verifyAsyncCommandCompletion(
        client,
        database,
        submission,
        1.millisecond,
        timeout,
        "export",
        "poller-test",
        "test-request",
        () => {
          factoryCalls.incrementAndGet()
          timer
        })

    def start(timeout: FiniteDuration = (-1).seconds): Invocation = {
      val invocation = new Invocation(() => run(timeout))
      invocations += invocation
      invocation
    }

    def close(): Unit = {
      invocations.foreach { invocation =>
        invocation.thread.interrupt()
        invocation.thread.join(5000)
      }
      timer.cancel()
    }
  }

  private def withFixture(test: Fixture => Unit, immediateFirst: Boolean = false): Unit = {
    val fixture = new Fixture(immediateFirst)
    try test(fixture)
    finally fixture.close()
  }

  "verifyAsyncCommandCompletion" should "return the completed table at its current row" in {
    withFixture(f => {
      val completed = status("Completed")
      when(f.client.executeMgmt(database, command)).thenReturn(completed)
      val call = f.start()

      f.timer.runNext() shouldBe 0L
      call.await()

      call.error.get() shouldBe null
      call.result.get().get should be theSameInstanceAs completed.getPrimaryResults
      call.result.get().get.getString("State") shouldBe "Completed"
      call.result.get().get.getString("OperationId") shouldBe operationId
      f.timer.cancelled.get() shouldBe true
      verify(f.client, times(1)).executeMgmt(database, command)
    })
  }

  it should "preserve the terminal throttled result, cursor, and failure message" in {
    withFixture(f => {
      val throttled = status("Throttled", "CapacityPolicy/Export")
      when(f.client.executeMgmt(database, command)).thenReturn(throttled)
      val call = f.start()
      f.timer.runNext()
      call.await()

      val error = call.error.get().asInstanceOf[FailedOperationException]
      error.getMessage shouldBe
        s"Failed to execute Kusto operation with OperationId '$operationId', " +
        "State: 'Throttled', Status: 'CapacityPolicy/Export'"
      error.getResult.get should be theSameInstanceAs throttled.getPrimaryResults
      error.getResult.get.getString("State") shouldBe "Throttled"
      error.getResult.get.getString("OperationId") shouldBe operationId
      f.timer.cancelled.get() shouldBe true
    })
  }

  it should "poll Scheduled and InProgress on the same operation with backoff" in {
    withFixture(f => {
      when(f.client.executeMgmt(database, command))
        .thenReturn(status("Scheduled"), status("InProgress"), status("Completed"))
      val call = f.start()

      f.timer.runNext() shouldBe 0L
      f.timer.runNext() shouldBe 2L
      f.timer.runNext() shouldBe 4L
      call.await()

      call.error.get() shouldBe null
      call.result.get().get.getString("State") shouldBe "Completed"
      verify(f.client, times(3)).executeMgmt(database, command)
    })
  }

  it should "retry transient client, service, and HTTP 429 lookup errors on the same operation" in {
    withFixture(f => {
      when(f.client.executeMgmt(database, command))
        .thenThrow(new DataClientException("testcluster", "temporary client failure"))
        .thenReturn(status("InProgress"))
        .thenThrow(new DataServiceException("testcluster", "temporary service failure", false))
        .thenThrow(new ThrottleException("testcluster"))
        .thenReturn(status("Completed"))
      val call = f.start()
      (1 to 5).foreach(_ => f.timer.runNext())
      call.await()

      call.error.get() shouldBe null
      call.result.get().get.getString("State") shouldBe "Completed"
      verify(f.client, times(5)).executeMgmt(database, command)
    })
  }

  it should "poll again after an empty status table without advancing an absent row" in {
    withFixture(f => {
      when(f.client.executeMgmt(database, command))
        .thenReturn(emptyStatus, status("Completed"))
      val call = f.start()
      f.timer.runNext()
      f.timer.runNext()
      call.await()

      call.error.get() shouldBe null
      call.result.get().get.getString("State") shouldBe "Completed"
      verify(f.client, times(2)).executeMgmt(database, command)
    })
  }

  it should "report timeout before dereferencing a response when no poll has returned" in {
    withFixture(f => {
      val error = intercept[TimeoutAwaitingPendingOperationException](f.run(1.nanosecond))

      error.getMessage should include(operationId)
      f.timer.cancelled.get() shouldBe true
      f.timer.pendingCount shouldBe 0
      verifyNoInteractions(f.client)
    })
  }

  it should "report timeout after a transient lookup failure with no response" in {
    withFixture(
      f => {
        when(f.client.executeMgmt(database, command))
          .thenThrow(new DataClientException("testcluster", "temporary failure"))

        intercept[TimeoutAwaitingPendingOperationException](f.run(1.nanosecond))

        f.timer.cancelled.get() shouldBe true
        f.timer.pendingCount shouldBe 0
        verify(f.client, times(1)).executeMgmt(database, command)
      },
      immediateFirst = true)
  }

  it should "report timeout rather than a terminal failure for empty status results" in {
    withFixture(
      f => {
        when(f.client.executeMgmt(database, command)).thenReturn(emptyStatus)

        intercept[TimeoutAwaitingPendingOperationException](f.run(1.nanosecond))

        f.timer.cancelled.get() shouldBe true
        f.timer.pendingCount shouldBe 0
        verify(f.client, times(1)).executeMgmt(database, command)
      },
      immediateFirst = true)
  }

  it should "fail a zero timeout without creating a timer or polling" in {
    withFixture(f => {
      intercept[TimeoutAwaitingPendingOperationException](f.run(Duration.Zero))

      f.factoryCalls.get() shouldBe 0
      verifyNoInteractions(f.client)
    })
  }

  it should "deliver permanent lookup failures to the caller with their original cause" in {
    withFixture(f => {
      val original = new DataServiceException("testcluster", "permission denied", true)
      when(f.client.executeMgmt(database, command)).thenThrow(original)
      val call = f.start()
      f.timer.runNext()
      call.await()

      call.error.get().getCause should be theSameInstanceAs original
      call.error.get().getMessage should include(command)
      f.timer.cancelled.get() shouldBe true
      f.timer.pendingCount shouldBe 0
      verify(f.client, times(1)).executeMgmt(database, command)
    })
  }

  it should "deliver unexpected worker exceptions without throwing on the worker" in {
    withFixture(f => {
      val original = new IllegalArgumentException("invalid status response")
      when(f.client.executeMgmt(database, command)).thenThrow(original)
      val call = f.start()
      f.timer.runNext()
      call.await()

      call.error.get() should be theSameInstanceAs original
      f.timer.cancelled.get() shouldBe true
    })
  }

  it should "cancel queued polling when the caller is interrupted" in {
    withFixture(f => {
      when(f.client.executeMgmt(database, command)).thenReturn(status("InProgress"))
      val call = f.start()
      f.timer.runNext()
      f.timer.pendingCount shouldBe 1

      call.thread.interrupt()
      call.await()

      call.error.get() shouldBe a[InterruptedException]
      f.timer.cancelled.get() shouldBe true
      f.timer.pendingCount shouldBe 0
      verify(f.client, times(1)).executeMgmt(database, command)
    })
  }

  it should "safely reject rescheduling after cancellation during an in-flight poll" in {
    withFixture(f => {
      val entered = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val workerError = new AtomicReference[Throwable]()
      when(f.client.executeMgmt(database, command)).thenAnswer { _ =>
        entered.countDown()
        assert(release.await(5, TimeUnit.SECONDS), "Status poll was not released")
        status("InProgress")
      }
      val call = f.start()
      val worker = new Thread(
        new Runnable {
          override def run(): Unit = {
            try f.timer.runNext()
            catch { case error: Throwable => workerError.set(error) }
          }
        },
        "poller-test-in-flight")
      worker.setDaemon(true)
      worker.start()
      try {
        assert(entered.await(5, TimeUnit.SECONDS), "Status poll did not start")
        call.thread.interrupt()
        call.await()
        call.error.get() shouldBe a[InterruptedException]
        f.timer.cancelled.get() shouldBe true
      } finally {
        release.countDown()
        worker.join(5000)
      }

      worker.isAlive shouldBe false
      workerError.get() shouldBe null
      f.timer.rejectedSchedules.get() shouldBe 1
      f.timer.pendingCount shouldBe 0
      verify(f.client, times(1)).executeMgmt(database, command)
    })
  }

  it should "report unknown or malformed state as a monitoring error" in {
    val unreadable = response(Seq("State"), Seq(Seq("unreadable")))
    unreadable.getPrimaryResults.getData
      .get(0)
      .set(
        0,
        new Object {
          override def toString: String = throw new IllegalArgumentException("Unreadable state")
        })
    Seq(
      status("Unknown"),
      status(null),
      status(""),
      response(Seq("Status"), Seq(Seq("missing state"))),
      unreadable)
      .foreach { terminal =>
        withFixture(f => {
          when(f.client.executeMgmt(database, command)).thenReturn(terminal)
          val call = f.start()
          f.timer.runNext()
          call.await()

          val error = call.error.get()
          error shouldBe a[IllegalStateException]
          error.getMessage should include("Cannot determine the outcome")
          error.getMessage should include(operationId)
          error.getMessage should include("test-request")
          f.timer.cancelled.get() shouldBe true
          f.timer.pendingCount shouldBe 0
          verify(f.client, times(1)).executeMgmt(database, command)
        })
      }
  }

  it should "retain recognized terminal failures even when status text is missing" in {
    Seq(
      "Failed",
      "PartiallySucceeded",
      "Abandoned",
      "BadInput",
      "Throttled",
      "Canceled",
      "Skipped")
      .foreach { state =>
        withFixture(f => {
          val terminal = response(Seq("OperationId", "State"), Seq(Seq(operationId, state)))
          when(f.client.executeMgmt(database, command)).thenReturn(terminal)
          val call = f.start()
          f.timer.runNext()
          call.await()

          val error = call.error.get().asInstanceOf[FailedOperationException]
          error.getResult.get should be theSameInstanceAs terminal.getPrimaryResults
          error.getResult.get.getString("State") shouldBe state
          error.getMessage should include(operationId)
          error.getMessage should include("Status: '<unavailable>'")
          f.timer.cancelled.get() shouldBe true
          verify(f.client, times(1)).executeMgmt(database, command)
        })
      }
  }

  it should "accept Completed even when optional status text is absent" in {
    Seq(response(Seq("State"), Seq(Seq("Completed"))), status("Completed", null)).foreach {
      completed =>
        withFixture(f => {
          when(f.client.executeMgmt(database, command)).thenReturn(completed)
          val call = f.start()
          f.timer.runNext()
          call.await()

          call.error.get() shouldBe null
          call.result.get().get should be theSameInstanceAs completed.getPrimaryResults
          f.timer.cancelled.get() shouldBe true
        })
    }
  }

  it should "terminate its named daemon timer on success and worker failure" in {
    Seq(false, true).foreach { fail =>
      val client = mock(classOf[Client])
      val worker = new AtomicReference[Thread]()
      val uncaught = new AtomicReference[Throwable]()
      val original = new IllegalArgumentException("worker failure")
      when(client.executeMgmt(database, command)).thenAnswer { _ =>
        val thread = Thread.currentThread()
        worker.set(thread)
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
          override def uncaughtException(thread: Thread, error: Throwable): Unit =
            uncaught.set(error)
        })
        if (fail) throw original
        status("Completed")
      }
      def run(): Option[KustoResultSetTable] =
        KustoDataSourceUtils.verifyAsyncCommandCompletion(
          client,
          database,
          submission,
          1.millisecond,
          5.seconds,
          "export",
          "poller-test",
          "test-request")

      if (fail) intercept[IllegalArgumentException](run()) should be theSameInstanceAs original
      else run().get.getString("State") shouldBe "Completed"

      val thread = worker.get()
      thread.getName shouldBe "kusto-async-operation-poller"
      thread.isDaemon shouldBe true
      thread.join(5000)
      thread.isAlive shouldBe false
      uncaught.get() shouldBe null
    }
  }

  "doWhile" should "retain its public callback and latch behavior" in {
    val iterations = new AtomicInteger()
    val finalValue = new AtomicInteger()
    val latch = KustoDataSourceUtils.doWhile[Int](
      () => iterations.incrementAndGet(),
      0,
      1,
      _ < 3,
      value => finalValue.set(value),
      4,
      8)

    assert(latch.await(5, TimeUnit.SECONDS), "Legacy doWhile did not finish")
    iterations.get() shouldBe 3
    finalValue.get() shouldBe 3
  }
}

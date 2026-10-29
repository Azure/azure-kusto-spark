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
import com.microsoft.azure.kusto.data.exceptions.{
  DataClientException,
  DataServiceException,
  ThrottleException
}
import com.microsoft.kusto.spark.authentication.KustoAccessTokenAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.exceptions.{
  FailedOperationException,
  TimeoutAwaitingPendingOperationException
}
import com.microsoft.kusto.spark.utils.ExtendedKustoClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.MockMakers
import org.mockito.Mockito.{mock, when, withSettings}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class KustoReaderExportRetryTest extends AnyFlatSpec with Matchers {
  private val database = "test-database"
  private val requestId = "test-export-retry"
  private val root = "test-export/"
  private val capacityStatus =
    "CommandType: 'DataExportToFile', Capacity: 1, Origin: 'CapacityPolicy/Export'."
  private val mapper = new ObjectMapper()
  private val namePrefix = """namePrefix="([^"]+)"""".r

  private case class Outcome(
      state: String = "Completed",
      status: String = "",
      submissionError: Option[Exception] = None,
      columns: Seq[String] = Seq("OperationId", "State", "Status"))

  private val throttled = Outcome("Throttled", capacityStatus)

  private def response(columns: Seq[String], values: Seq[String]): KustoOperationResult = {
    val columnJson = columns
      .map(name => s"""{"ColumnName":"$name","DataType":"String","ColumnType":"string"}""")
      .mkString(",")
    val rowJson = values.map(value => mapper.writeValueAsString(value)).mkString(",")
    new KustoOperationResult(
      s"""{"Tables":[{"TableName":"Table_0","Columns":[$columnJson],"Rows":[[$rowJson]]}]}""",
      "v1")
  }

  private def blobStorage: TransientStorageParameters = new TransientStorageParameters(
    Array(
      new TransientStorageCredentials(
        "https://teststorage.blob.core.windows.net/testcontainer?sig=private-sas")))

  private class Fixture(outcomes: Seq[Outcome]) {
    val engine: Client = mock(classOf[Client])
    val clock = new AtomicLong()
    val commands = ArrayBuffer.empty[String]
    val clientRequestIds = ArrayBuffer.empty[String]
    val delays = ArrayBuffer.empty[Long]
    val pollingBudgets = ArrayBuffer.empty[FiniteDuration]
    val results = new ConcurrentLinkedQueue[KustoResultSetTable]()
    val polledOperationIds = new ConcurrentLinkedQueue[String]()
    var submissionDuration: FiniteDuration = Duration.Zero
    var submissionResultOverride: Option[KustoOperationResult] = None
    var pollingDuration: FiniteDuration = Duration.Zero
    var extraWait: FiniteDuration = Duration.Zero
    var waitError: Option[InterruptedException] = None
    var pollError: Option[Exception] = None
    val storage: TransientStorageParameters = blobStorage
    val options: KustoReadOptions = KustoReadOptions(
      partitionOptions = PartitionOptions(1, Some("value"), Some("hash")),
      queryFilterPushDown = None,
      additionalExportOptions = Map("distribution" -> "per_node"))

    val spark: SparkSession =
      mock(classOf[SparkSession], withSettings().mockMaker(MockMakers.SUBCLASS))
    when(spark.version).thenReturn(org.apache.spark.SPARK_VERSION)
    val crp = new ClientRequestProperties()
    crp.setClientRequestId(requestId)
    crp.setParameter("value", "private-parameter")

    def request(timeout: FiniteDuration = 2.minutes): KustoReadRequest = KustoReadRequest(
      spark,
      KustoSchema(StructType(Seq(StructField("value", StringType))), Set.empty),
      KustoCoordinates("https://testcluster.kusto.windows.net", "testcluster", database, None),
      "SensitiveSource | where value == 'private-query'",
      KustoAccessTokenAuthentication("private-access-token"),
      timeout,
      Some(crp),
      requestId)

    private def outcome(index: Int): Outcome = outcomes.lift(index).getOrElse(outcomes.last)

    when(engine.executeMgmt(anyString(), anyString(), any(classOf[ClientRequestProperties])))
      .thenAnswer { invocation =>
        commands += invocation.getArgument[String](1)
        clientRequestIds += invocation.getArgument[ClientRequestProperties](2).getClientRequestId
        clock.addAndGet(submissionDuration.toNanos)
        val current = outcome(commands.size - 1)
        current.submissionError.foreach(error => throw error)
        submissionResultOverride.getOrElse(
          response(Seq("OperationId"), Seq(new UUID(0, commands.size.toLong).toString)))
      }
    when(engine.executeMgmt(anyString(), anyString())).thenAnswer { invocation =>
      val operationId = invocation.getArgument[String](1).stripPrefix(".show operations ")
      val current = outcome(UUID.fromString(operationId).getLeastSignificantBits.toInt - 1)
      polledOperationIds.add(operationId)
      clock.addAndGet(pollingDuration.toNanos)
      val completion = response(
        current.columns,
        current.columns.map {
          case "OperationId" => operationId
          case "State" => current.state
          case "Status" => current.status
        })
      results.add(completion.getPrimaryResults)
      completion
    }

    val client: ExtendedKustoClient = new ExtendedKustoClient(null, null, "testcluster") {
      override lazy val engineClient: Client = engine
    }
    val reader: KustoReader = new KustoReader(client) {
      override private[kusto] def exportTimeNanos(): Long = clock.get()
      override private[kusto] def exportRetryDelayMillis(retryNumber: Int): Long =
        5000L << (retryNumber - 1)
      override private[kusto] def waitBeforeExportRetry(delayMillis: Long): Unit = {
        delays += delayMillis
        waitError.foreach(error => throw error)
        clock.addAndGet(delayMillis.millis.toNanos + extraWait.toNanos)
      }
      override private[kusto] def awaitExportCompletion(
          request: KustoReadRequest,
          commandResult: KustoResultSetTable,
          directory: String,
          timeout: FiniteDuration): Unit = {
        pollingBudgets += timeout
        pollError.foreach(error => throw error)
        super.awaitExportCompletion(request, commandResult, directory, timeout)
      }
    }

    def run(timeout: FiniteDuration = 2.minutes): String =
      reader.exportPartitionToBlobWithRetries(
        KustoPartition(None, 0),
        request(timeout),
        storage,
        root,
        options,
        KustoFiltering())

    def directories: Seq[String] = commands.toSeq.map(command =>
      namePrefix.findFirstMatchIn(command).get.group(1).replaceFirst("part[0-9]+$", ""))
  }

  "exportPartitionToBlobWithRetries" should "resubmit a throttled export and return only its successful directory" in {
    val fixture = new Fixture(Seq(throttled, Outcome()))

    val directory = fixture.run()

    fixture.commands.size shouldBe 2
    fixture.delays.toSeq shouldBe Seq(5000L)
    fixture.directories.distinct.size shouldBe 2
    directory shouldBe fixture.directories.last
    directory should startWith(s"${root}part0/attempt2_")
    fixture.polledOperationIds.asScala.toSeq.distinct.size shouldBe 2
    fixture.clientRequestIds.distinct.size shouldBe 2
    all(fixture.clientRequestIds) should startWith(s"$requestId;")
    fixture.commands
      .map(command => namePrefix.replaceFirstIn(command, "namePrefix=\"attempt\""))
      .distinct
      .size shouldBe 1
    fixture.crp.getClientRequestId shouldBe requestId
    fixture.crp.getParameter("value") shouldBe "private-parameter"
  }

  it should "preserve Spark 3 native-writer defaults on every retry" in {
    Seq("3.0.3" -> true, "3.3.0" -> true, "3.3.1" -> false, "3.5.3" -> false)
      .foreach { case (version, disableNativeWriter) =>
        val fixture = new Fixture(Seq(throttled, Outcome()))
        when(fixture.spark.version).thenReturn(version)

        fixture.run() shouldBe fixture.directories.last

        fixture.commands.size shouldBe 2
        withClue(s"Spark $version: ") {
          fixture.commands.foreach { command =>
            command.contains("useNativeParquetWriter=false") shouldBe disableNativeWriter
          }
        }
      }
  }

  it should "allow success on the fourth attempt without an extra delay" in {
    val fixture = new Fixture(Seq(throttled, throttled, throttled, Outcome()))

    fixture.run() shouldBe fixture.directories.last

    fixture.commands.size shouldBe 4
    fixture.delays.toSeq shouldBe Seq(5000L, 10000L, 20000L)
  }

  it should "stop after four throttles and preserve the final service failure" in {
    val fixture = new Fixture(Seq(throttled))

    val error = intercept[FailedOperationException](fixture.run())

    fixture.commands.size shouldBe 4
    fixture.delays.toSeq shouldBe Seq(5000L, 10000L, 20000L)
    error.getMessage should include("Automatic export retries exhausted after 4 attempts.")
    error.getMessage should include("concurrent export capacity was exceeded")
    error.getMessage should include(requestId)
    error.getMessage should include(new UUID(0, 4).toString)
    error.getResult.get should be theSameInstanceAs fixture.results.asScala.last
    error.getCause
      .asInstanceOf[FailedOperationException]
      .getResult
      .get should be theSameInstanceAs error.getResult.get
    error.getResult.get.getString("State") shouldBe "Throttled"
    error.getMessage should not include "private-query"
    error.getMessage should not include "private-sas"
    error.getMessage should not include "private-access-token"
  }

  it should "retry confirmed throttling regardless of origin or optional status text" in {
    Seq("Origin: 'RequestRateLimitPolicy/WorkloadGroup/default'.", "", null).foreach { status =>
      val fixture = new Fixture(Seq(Outcome("Throttled", status), Outcome()))

      fixture.run() shouldBe fixture.directories.last
      fixture.commands.size shouldBe 2
    }
    val missingStatus =
      new Fixture(Seq(Outcome("Throttled", columns = Seq("OperationId", "State")), Outcome()))
    missingStatus.run() shouldBe missingStatus.directories.last
    missingStatus.commands.size shouldBe 2
  }

  it should "not retry other terminal states even when status mentions throttling" in {
    Seq("Failed", "Canceled", "PartiallySucceeded", "BadInput", "Abandoned", "Skipped")
      .foreach { state =>
        val fixture = new Fixture(Seq(Outcome(state, capacityStatus)))

        intercept[FailedOperationException](fixture.run())

        fixture.commands.size shouldBe 1
        fixture.delays shouldBe empty
      }
  }

  it should "not infer a retry from malformed state metadata" in {
    Seq(
      Outcome("Unknown"),
      Outcome(null),
      Outcome(""),
      Outcome("Throttled", capacityStatus, columns = Seq("Status"))).foreach { outcome =>
      val fixture = new Fixture(Seq(outcome))

      val error = intercept[IllegalStateException](fixture.run())

      error.getMessage should include("Cannot determine the outcome")
      error.getMessage should include(new UUID(0, 1).toString)
      error.getMessage should include(requestId)
      fixture.commands.size shouldBe 1
      fixture.polledOperationIds.size() shouldBe 1
      fixture.delays shouldBe empty
    }
  }

  it should "not replay ambiguous submission failures through the generic command retry policy" in {
    Seq(
      new DataServiceException("testcluster", "connection lost after submission", false),
      new DataServiceException("testcluster", "permission denied", true),
      new DataClientException("testcluster", "response unreadable"),
      new IllegalArgumentException("invalid request")).foreach { original =>
      val fixture = new Fixture(Seq(Outcome(submissionError = Some(original))))

      val error = intercept[Exception](fixture.run())

      error should be theSameInstanceAs original
      fixture.commands.size shouldBe 1
      fixture.polledOperationIds shouldBe empty
      fixture.delays shouldBe empty
    }
  }

  it should "retry an explicit HTTP 429 rejection using the same bounded policy" in {
    val rejected = Outcome(submissionError = Some(new ThrottleException("testcluster")))
    val fixture = new Fixture(Seq(rejected, Outcome()))

    fixture.run() shouldBe fixture.directories.last

    fixture.commands.size shouldBe 2
    fixture.delays.toSeq shouldBe Seq(5000L)
    fixture.polledOperationIds.size() shouldBe 1
  }

  it should "share one attempt limit between HTTP rejections and async throttling" in {
    val original = new ThrottleException("testcluster")
    val rejected = Outcome(submissionError = Some(original))
    val fixture = new Fixture(Seq(throttled, rejected, throttled, rejected))

    val error = intercept[FailedOperationException](fixture.run())

    error.getCause should be theSameInstanceAs original
    error.getResult shouldBe None
    error.getMessage should include("Automatic export retries exhausted after 4 attempts.")
    error.getMessage should not include "concurrent export capacity was exceeded"

    fixture.commands.size shouldBe 4
    fixture.polledOperationIds.size() shouldBe 2
    fixture.delays.toSeq shouldBe Seq(5000L, 10000L, 20000L)
  }

  it should "explain HTTP 429 exhaustion without inferring export-capacity throttling" in {
    val original = new ThrottleException("testcluster")
    val fixture = new Fixture(Seq(Outcome(submissionError = Some(original))))

    val error = intercept[FailedOperationException](fixture.run())

    error.getMessage shouldBe
      "Kusto Spark read failed because its distributed export submission was throttled (HTTP 429)." +
      " Distributed reads export query results to temporary storage before Spark reads them." +
      " Retry the read after backoff. If throttling persists, ask the service administrator to review the applicable limit." +
      " Automatic export retries exhausted after 4 attempts." +
      s" Connector requestId: $requestId. Original error: ${original.getMessage}"
    error.getCause should be theSameInstanceAs original
    error.getResult shouldBe None
    fixture.commands.size shouldBe 4
    fixture.polledOperationIds shouldBe empty
    fixture.delays.toSeq shouldBe Seq(5000L, 10000L, 20000L)
  }

  it should "include HTTP 429 read context when the backoff cannot fit in the budget" in {
    val original = new ThrottleException("testcluster")
    val fixture = new Fixture(Seq(Outcome(submissionError = Some(original))))

    val error = intercept[FailedOperationException](fixture.run(5.seconds))

    error.getMessage should startWith(
      "Kusto Spark read failed because its distributed export submission was throttled (HTTP 429).")
    error.getMessage should include("the next backoff does not fit within timeoutLimit")
    error.getMessage should include(requestId)
    error.getMessage should not include "concurrent export capacity was exceeded"
    error.getCause should be theSameInstanceAs original
    error.getResult shouldBe None
    fixture.commands.size shouldBe 1
    fixture.delays shouldBe empty
  }

  it should "stop immediately if a retry's submission outcome becomes unknown" in {
    val original = new DataServiceException("testcluster", "response lost", false)
    val fixture = new Fixture(Seq(throttled, Outcome(submissionError = Some(original))))

    intercept[DataServiceException](fixture.run()) should be theSameInstanceAs original

    fixture.commands.size shouldBe 2
    fixture.delays.toSeq shouldBe Seq(5000L)
  }

  it should "not retry polling timeouts or unexpected polling errors" in {
    Seq(
      new TimeoutAwaitingPendingOperationException("operation still running"),
      new IllegalStateException("unreadable result")).foreach { original =>
      val fixture = new Fixture(Seq(Outcome()))
      fixture.pollError = Some(original)

      intercept[Exception](fixture.run()) should be theSameInstanceAs original

      fixture.commands.size shouldBe 1
      fixture.delays shouldBe empty
    }
  }

  it should "charge submission, polling, and backoff to one partition timeout budget" in {
    val fixture = new Fixture(Seq(throttled, Outcome()))
    fixture.submissionDuration = 2.seconds
    fixture.pollingDuration = 1.second

    fixture.run(20.seconds)

    fixture.pollingBudgets.toSeq shouldBe Seq(18.seconds, 10.seconds)
    fixture.clock.get() shouldBe 11.seconds.toNanos
  }

  it should "not wait or retry when the next backoff cannot fit in the budget" in {
    val fixture = new Fixture(Seq(throttled))

    val error = intercept[FailedOperationException](fixture.run(5.seconds))

    error.getMessage should include("the next backoff does not fit within timeoutLimit")
    error.getResult.get.getString("State") shouldBe "Throttled"
    fixture.commands.size shouldBe 1
    fixture.delays shouldBe empty
  }

  it should "not poll or resubmit if submission has consumed the remaining budget" in {
    val fixture = new Fixture(Seq(Outcome()))
    fixture.submissionDuration = 11.seconds
    val accepted = response(Seq("OperationId"), Seq(new UUID(0, 1).toString))
    fixture.submissionResultOverride = Some(accepted)

    val error = intercept[TimeoutAwaitingPendingOperationException](fixture.run(10.seconds))

    error.getMessage should include(new UUID(0, 1).toString)
    error.getMessage should include(requestId)
    error.getMessage should include("partition 0")
    error.getCause shouldBe a[TimeoutAwaitingPendingOperationException]
    error.getCause.getCause shouldBe null
    fixture.commands.size shouldBe 1
    fixture.pollingBudgets shouldBe empty
    fixture.polledOperationIds shouldBe empty
    fixture.delays shouldBe empty
    accepted.getPrimaryResults.next() shouldBe true
    accepted.getPrimaryResults.getString("OperationId") shouldBe new UUID(0, 1).toString
  }

  it should "identify the newly accepted operation when a retry submission exhausts the budget" in {
    val fixture = new Fixture(Seq(throttled, Outcome()))
    fixture.submissionDuration = 3.seconds

    val error = intercept[TimeoutAwaitingPendingOperationException](fixture.run(10.seconds))

    error.getMessage should include(new UUID(0, 2).toString)
    error.getMessage should not include new UUID(0, 1).toString
    error.getMessage should include(requestId)
    error.getCause shouldBe a[TimeoutAwaitingPendingOperationException]
    val priorFailure = error.getCause.getCause.asInstanceOf[FailedOperationException]
    priorFailure.getResult.get.getString("OperationId") shouldBe new UUID(0, 1).toString
    fixture.commands.size shouldBe 2
    fixture.pollingBudgets.toSeq shouldBe Seq(7.seconds)
    fixture.polledOperationIds.asScala.toSeq shouldBe Seq(new UUID(0, 1).toString)
    fixture.delays.toSeq shouldBe Seq(5000L)
  }

  it should "preserve post-submission timeout when the accepted operation ID is unavailable" in {
    val fixture = new Fixture(Seq(Outcome()))
    fixture.submissionDuration = 11.seconds
    fixture.submissionResultOverride = Some(response(Seq("OperationId"), Seq.empty))

    val error = intercept[TimeoutAwaitingPendingOperationException](fixture.run(10.seconds))

    error.getMessage should include("OperationId: '<unavailable>'")
    error.getMessage should include(requestId)
    fixture.commands.size shouldBe 1
    fixture.pollingBudgets shouldBe empty
    fixture.polledOperationIds shouldBe empty
    fixture.delays shouldBe empty
  }

  it should "recheck the deadline after backoff before submitting another export" in {
    val fixture = new Fixture(Seq(throttled, Outcome()))
    fixture.extraWait = 20.seconds

    val error = intercept[TimeoutAwaitingPendingOperationException](fixture.run(10.seconds))

    error.getCause shouldBe a[FailedOperationException]
    fixture.commands.size shouldBe 1
    fixture.delays.toSeq shouldBe Seq(5000L)
  }

  it should "fail a zero timeout before any submission" in {
    val fixture = new Fixture(Seq(Outcome()))

    intercept[TimeoutAwaitingPendingOperationException](fixture.run(Duration.Zero))

    fixture.commands shouldBe empty
    fixture.delays shouldBe empty
  }

  it should "preserve unlimited timeout semantics but still cap retry attempts" in {
    val fixture = new Fixture(Seq(throttled))
    fixture.submissionDuration = 1.day

    intercept[FailedOperationException](fixture.run((-1).seconds))

    fixture.commands.size shouldBe 4
    all(fixture.pollingBudgets) shouldBe (-1).seconds
  }

  it should "stop on interruption during backoff and restore the interrupt flag" in {
    val fixture = new Fixture(Seq(throttled, Outcome()))
    val original = new InterruptedException("read canceled")
    fixture.waitError = Some(original)

    try {
      intercept[InterruptedException](fixture.run()) should be theSameInstanceAs original
      Thread.currentThread().isInterrupted shouldBe true
      fixture.commands.size shouldBe 1
    } finally Thread.interrupted()
  }

  it should "not resubmit when the polling caller is interrupted" in {
    val fixture = new Fixture(Seq(Outcome()))
    val original = new InterruptedException("polling canceled")
    fixture.pollError = Some(original)

    try {
      intercept[InterruptedException](fixture.run()) should be theSameInstanceAs original
      Thread.currentThread().isInterrupted shouldBe true
      fixture.commands.size shouldBe 1
      fixture.delays shouldBe empty
    } finally Thread.interrupted()
  }

  it should "not submit if the caller was already interrupted" in {
    val fixture = new Fixture(Seq(Outcome()))

    try {
      Thread.currentThread().interrupt()
      intercept[InterruptedException](fixture.run())
      Thread.currentThread().isInterrupted shouldBe true
      fixture.commands shouldBe empty
    } finally Thread.interrupted()
  }

  "exportPartitionsToBlob" should "retain successful partitions and exclude every abandoned attempt from read paths" in {
    val fixture = new Fixture(Seq(Outcome(), throttled, Outcome(), Outcome()))
    val partitions = (0 to 2).map(id => KustoPartition(Some(s"value == '$id'"), id))

    val directories = fixture.reader.exportPartitionsToBlob(
      partitions,
      fixture.request(),
      fixture.storage,
      root,
      fixture.options,
      KustoFiltering())
    val probed = ArrayBuffer.empty[String]
    val paths = KustoReader.exportedPaths(
      fixture.storage,
      directories,
      "wasbs",
      (_, directory) => {
        probed += directory
        // Model both failed and successful directories as existing; probe only successful leaves.
        fixture.directories.contains(directory)
      })

    fixture.commands.size shouldBe 4
    directories shouldBe Seq(
      fixture.directories(0),
      fixture.directories(2),
      fixture.directories(3))
    directories.map(_.split('/')(1)) shouldBe Seq("part0", "part1", "part2")
    probed.toSeq shouldBe directories
    paths shouldBe directories.map(dir =>
      s"wasbs://testcontainer@teststorage.blob.core.windows.net/$dir")
    paths.exists(_.contains(fixture.directories(1))) shouldBe false
    paths.exists(_.endsWith(root)) shouldBe false
  }

  it should "stop at an exhausted partition without exporting later partitions" in {
    val fixture = new Fixture(Seq(Outcome(), throttled))

    intercept[FailedOperationException] {
      fixture.reader.exportPartitionsToBlob(
        (0 to 2).map(id => KustoPartition(None, id)),
        fixture.request(),
        fixture.storage,
        root,
        fixture.options,
        KustoFiltering())
    }

    fixture.commands.size shouldBe 5
    fixture.directories.count(_.contains("/part0/")) shouldBe 1
    fixture.directories.count(_.contains("/part1/")) shouldBe 4
    fixture.directories.exists(_.contains("/part2/")) shouldBe false
  }

  "exportedPaths" should "preserve storage targets and protocol while filtering empty successful directories" in {
    val storage = new TransientStorageParameters(
      Array(
        new TransientStorageCredentials(
          "https://account1.blob.core.windows.net/container1?sig=a"),
        new TransientStorageCredentials(
          "https://account2.blob.core.windows.net/container2?sig=b")))
    val directories = Seq("read/part0/attempt1/", "read/part1/attempt2/", "read/empty/")

    Seq("wasbs", "abfs", "abfss").foreach { protocol =>
      val paths = KustoReader.exportedPaths(
        storage,
        directories,
        protocol,
        (credential, directory) =>
          (credential.storageAccountName == "account1" && directory == directories.head) ||
            (credential.storageAccountName == "account2" && directory == directories(1)))

      paths shouldBe Seq(
        s"$protocol://container1@account1.blob.core.windows.net/${directories.head}",
        s"$protocol://container2@account2.blob.core.windows.net/${directories(1)}")
    }
    KustoReader.exportedPaths(storage, directories, "wasbs", (_, _) => false) shouldBe empty
    KustoReader.exportedPaths(storage, Seq.empty, "wasbs", (_, _) => true) shouldBe empty
  }

  it should "build OneLake paths from only the selected attempt directories" in {
    val credential = new TransientStorageCredentials()
    credential.parseOneLake(
      "https://onelake.dfs.fabric.microsoft.com/11111111-2222-3333-4444-555555555555/" +
        "66666666-7777-8888-9999-000000000000/Files/kusto-transient")
    val storage = new TransientStorageParameters(Array(credential))
    val directory = "read/part0/attempt2/"

    val paths = KustoReader.exportedPaths(storage, Seq(directory), "abfss", (_, _) => true)

    paths shouldBe Seq(s"${credential.oneLakeAbfssBase}/$directory")
    paths.head should startWith("abfss://")
  }

  "exportRetryDelayMillis" should "use five-second exponential backoff with bounded jitter" in {
    val reader = new KustoReader(null)
    KustoReader.MaxExportAttempts shouldBe 4

    (1 to 3).foreach { retry =>
      val nominal = 5000L << (retry - 1)
      val samples = (1 to 100).map(_ => reader.exportRetryDelayMillis(retry))

      all(samples) should be >= nominal / 2
      all(samples) should be <= nominal * 3 / 2
      all(samples) should be <= 30000L
      samples.distinct.size should be > 1
    }
  }

  "exportPartitionToBlob" should "keep its original caller-supplied directory contract" in {
    val fixture = new Fixture(Seq(Outcome()))

    fixture.reader.exportPartitionToBlob(
      KustoPartition(None, 0),
      fixture.request(),
      fixture.storage,
      root,
      fixture.options,
      KustoFiltering())

    fixture.directories shouldBe Seq(root)
    fixture.commands.size shouldBe 1
    fixture.delays shouldBe empty
  }
}

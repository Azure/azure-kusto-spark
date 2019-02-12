package com.microsoft.kusto.spark.datasink

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}
import java.util.zip.GZIPOutputStream
import java.util.{Timer, TimerTask, UUID}

import com.microsoft.azure.kusto.data.{Client, ClientFactory, ConnectionStringBuilder}
import com.microsoft.azure.kusto.ingest.IngestionProperties.DATA_FORMAT
import com.microsoft.azure.kusto.ingest.result.{IngestionResult, IngestionStatus, OperationStatus}
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.{IngestClientFactory, IngestionProperties}
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature
import com.microsoft.azure.storage.blob.{BlobOutputStream, CloudBlobContainer}
import com.microsoft.kusto.spark.datasource._
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{FutureAction, TaskContext}
import shaded.parquet.org.codehaus.jackson.map.ObjectMapper

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object KustoWriter{
  private val myName = this.getClass.getSimpleName
  val TempIngestionTablePrefix = "_tmpTable"
  var OperationShowCommandResult = 16
  val completedState = "Completed"
  val inProgressState = "InProgress"
  val stateCol = "State"
  val statusCol = "Status"
  val timeOut: FiniteDuration = 30 minutes
  val delayPeriodBetweenCalls: Int = 1000
  val GZIP_BUFFER_SIZE: Int = 16 * 1024
  private[kusto] def write(
                            batchId: Option[Long],
                            data: DataFrame,
                            tableCoordinates: KustoTableCoordinates,
                            appAuthentication: AadApplicationAuthentication,
                            kustoSparkWriteOptions:KustoSparkWriteOptions): Unit = {

    if(kustoSparkWriteOptions.mode != SaveMode.Append)
    {
      KDSU.logWarn(myName, s"Kusto data source supports only append mode. Ignoring '${kustoSparkWriteOptions.mode}' directive")
    }
    val schema = data.schema
    var batchIdIfExists = { if (batchId.isEmpty || batchId == Option(0)) { "" } else { s"${batchId.get}"} }

    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://${tableCoordinates.cluster}.kusto.windows.net", appAuthentication.ID, appAuthentication.password, appAuthentication.authority)
    engineKcsb.setClientVersionForTracing(KDSU.ClientName)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    val appName = data.sparkSession.sparkContext.appName
    val ingestKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://ingest-${tableCoordinates.cluster}.kusto.windows.net", appAuthentication.ID, appAuthentication.password, appAuthentication.authority)
    ingestKcsb.setClientVersionForTracing(KDSU.ClientName)

    try {
      // Try delete temporary tablesToCleanup created and not used
      val tempTablesOld = kustoAdminClient.execute(generateFindOldTempTablesCommand(tableCoordinates.database)).getValues.asScala
      val res = kustoAdminClient.execute(tableCoordinates.database, generateFindCurrentTempTablesCommand(TempIngestionTablePrefix))
      val tempTablesCurr = res.getValues.asScala

      val tablesToCleanup =  tempTablesOld.map(row => row.get(0)).intersect(tempTablesCurr.map(row => row.get(0)))

      if (tablesToCleanup.nonEmpty) {
        kustoAdminClient.execute(tableCoordinates.database, generateDropTablesCommand(tablesToCleanup.mkString(",")))
      }
    }
    catch  {
      case ex: Exception =>
        KDSU.reportExceptionAndThrow(myName, ex, "trying to drop temporary tables", tableCoordinates.cluster, tableCoordinates.database, tableCoordinates.table, isLogDontThrow = true)
    }

    val tmpTableName = KustoQueryUtils.simplifyName(s"$TempIngestionTablePrefix${appName}_${tableCoordinates.table}${batchIdIfExists}_${UUID.randomUUID().toString}")

    if(batchIdIfExists != "") batchIdIfExists = s", batch'$batchIdIfExists'"

    // KustoWriter will create a temporary table ingesting the data to it.
    // Only if all executors succeeded the table will be appended to the original destination table.
    KDSU.createTmpTableWithSameSchema(kustoAdminClient, tableCoordinates, tmpTableName, kustoSparkWriteOptions.tableCreateOptions, schema)

    KDSU.logInfo(myName, s"Successfully created temporary table $tmpTableName, will be deleted after completing the operation")

    val storageUri = TempStorageCache.getNewTempBlobReference(ingestKcsb, tableCoordinates.cluster)

    def ingestRowsIntoKusto(schema: StructType, rows: Iterator[InternalRow]): IngestionResult = {
      val ingestKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://ingest-${tableCoordinates.cluster}.kusto.windows.net", appAuthentication.ID, appAuthentication.password, appAuthentication.authority)
      ingestKcsb.setClientVersionForTracing(KDSU.ClientName)

      val ingestClient = IngestClientFactory.createClient(ingestKcsb)
      val ingestionProperties = new IngestionProperties(tableCoordinates.database, tmpTableName)

      val blobName = s"${tableCoordinates.database}_${tmpTableName}_${UUID.randomUUID.toString}_SparkStreamUpload.gz"
      val container = new CloudBlobContainer(new URI(storageUri))
      val blob = container.getBlockBlobReference(blobName)

      serializeRows(rows, schema, blob.openOutputStream, kustoSparkWriteOptions.timeZone)

      val signature = blob.getServiceClient.getCredentials.asInstanceOf[StorageCredentialsSharedAccessSignature]
      val blobPath = blob.getStorageUri.getPrimaryUri.toString + "?" + signature.getToken
      val blobSourceInfo = new BlobSourceInfo(blobPath)

      ingestionProperties.setDataFormat(DATA_FORMAT.csv.name)
      ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table)
      ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses)
      ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties)
    }

    val rdd = data.queryExecution.toRdd

    if (kustoSparkWriteOptions.isAsync) {
      val asyncWork: FutureAction[Unit] = rdd.foreachPartitionAsync {
        rows => {
          if(rows.isEmpty)
          {
            KDSU.logWarn(myName, s"sink to Kusto table '${tableCoordinates.table}' with no rows to write on partition ${TaskContext.getPartitionId}")
          }
          else {
            ingestToTemporaryTableByWorkers(batchId,tableCoordinates.cluster, tableCoordinates.database, tableCoordinates.table, schema, batchIdIfExists, ingestRowsIntoKusto, rows)
          }
        }
      }
      KDSU.logInfo(myName, s"asynchronous write to Kusto table '${tableCoordinates.table}' in progress")

      // This part runs back on the driver
      asyncWork.onSuccess {
        case _ =>
          finalizeIngestionWhenWorkersSucceeded(tableCoordinates.cluster, tableCoordinates.database, tableCoordinates.table, batchIdIfExists, kustoAdminClient, tmpTableName, isAsync = true)
      }

      asyncWork.onFailure {
        case exception: Exception =>
          tryCleanupIngestionByproducts(tableCoordinates.database, kustoAdminClient, tmpTableName)
          KDSU.reportExceptionAndThrow(myName, exception, "writing data", tableCoordinates.cluster, tableCoordinates.database, tableCoordinates.table, isLogDontThrow = true)
          KDSU.logError(myName, "The exception is not visible in the driver since we're in async mode")
      }
    }
    else
    {
      try {
        rdd.foreachPartition {
          rows => {
            if(rows.isEmpty)
            {
              KDSU.logWarn(myName, s"sink to Kusto table '${tableCoordinates.table}' with no rows to write on partition ${TaskContext.getPartitionId}")
            }
            else
            {
              ingestToTemporaryTableByWorkers(batchId, tableCoordinates.cluster, tableCoordinates.database, tableCoordinates.table, schema, batchIdIfExists, ingestRowsIntoKusto, rows)
            }
          }
        }
      }
      catch{
        case exception: Exception =>
          tryCleanupIngestionByproducts(tableCoordinates.database, kustoAdminClient, tmpTableName)
          throw exception.getCause
      }

      finalizeIngestionWhenWorkersSucceeded(tableCoordinates.cluster, tableCoordinates.database, tableCoordinates.table, batchIdIfExists, kustoAdminClient, tmpTableName)
      KDSU.logInfo(myName, s"write operation to Kusto table '${tableCoordinates.table}' finished successfully")
    }
  }

  private def tryCleanupIngestionByproducts(database: String, kustoAdminClient: Client, tmpTableName: String) = {
    try {
      kustoAdminClient.execute(database, generateTableDropCommand(tmpTableName))
    }
    catch {
      case exception: Exception =>
        KDSU.reportExceptionAndThrow(myName, exception, s"deleting temporary table $tmpTableName", database = database, isLogDontThrow = true)
    }
  }

  private def finalizeIngestionWhenWorkersSucceeded(cluster: String, database: String, table: String, batchIdIfExists: String, kustoAdminClient: Client, tmpTableName: String, isAsync: Boolean = false): Unit = {
    try {
      // Move data to real table
      // Protect tmp table from merge/rebuild and move data to the table requested by customer. This operation is atomic.
      kustoAdminClient.execute(database, generateTableAlterMergePolicyCommand(tmpTableName, allowMerge = false, allowRebuild = false))
      kustoAdminClient.execute(database, generateTableMoveExtentsCommand(tmpTableName, table))
      KDSU.logInfo(myName, s"write to Kusto table '$table' finished successfully $batchIdIfExists")
    }
    catch {
      case exception: Exception =>
        KDSU.reportExceptionAndThrow(myName, exception, "finalizing write operation", cluster, database, table, isLogDontThrow = isAsync)    }
    finally {
      tryCleanupIngestionByproducts(database, kustoAdminClient, tmpTableName)
    }
  }

  private def ingestToTemporaryTableByWorkers(batchId: Option[Long], cluster: String, database: String, table: String, schema: StructType, batchIdIfExists: String, ingestRowsIntoKusto: (StructType, Iterator[InternalRow]) => IngestionResult, rows: Iterator[InternalRow]) = {
    val partitionId = TaskContext.getPartitionId
    KDSU.logInfo(myName, s"Ingesting partition '$partitionId'")

      val ackIngestion: Future[IngestionResult] = Future {
        ingestRowsIntoKusto(schema, rows)
      }

      // We force blocking here, since the driver can only complete the ingestion process
      // once all partitions are ingested into the temporary table
      val ingestionResult = Await.result(ackIngestion, timeOut)

      // Proceed only on success. Will throw on failure for the driver to handle
      runSequentially[IngestionStatus](
        func = () => ingestionResult.GetIngestionStatusCollection().get(0),
        0, delayPeriodBetweenCalls, (timeOut.toMillis / delayPeriodBetweenCalls + 5).toInt,
        res => res.status == OperationStatus.Pending,
        res => res.status match {
          case OperationStatus.Pending =>
            throw new RuntimeException(s"Ingestion to Kusto failed on timeout failure. Cluster: '$cluster', database: '$database', table: '$table'$batchIdIfExists, partition: '$partitionId'")
          case OperationStatus.Succeeded =>
            KDSU.logInfo(myName, s"Ingestion to Kusto succeeded. Cluster: '$cluster', database: '$database', table: '$table'$batchIdIfExists, partition: '$partitionId'")
          case otherStatus =>
            throw new RuntimeException(s"Ingestion to Kusto failed with status '$otherStatus'." +
              s" Cluster: '$cluster', database: '$database', table: '$table'$batchIdIfExists, partition: '$partitionId'. Ingestion info: '${readIngestionResult(res)}'")
        }).await(timeOut.toMillis, TimeUnit.MILLISECONDS)
    }

  /**
    * A function to run sequentially async work on TimerTask using a Timer.
    * The function passed is scheduled sequentially by the timer, until last calculated returned value by func does not
    * satisfy the condition of doWhile or a given number of times has passed.
    * After one of these conditions was held true the finalWork function is called over the last returned value by func.
    * Returns a CountDownLatch object use to countdown times and await on it synchronously if needed
    * @param func - the function to run
    * @param delay - delay before first job
    * @param runEvery - delay between jobs
    * @param numberOfTimesToRun - stop jobs after numberOfTimesToRun.
    *                            set negative value to run infinitely
    * @param doWhile - stop jobs if condition holds for the func.apply output
    * @param finalWork - do final work with the last func.apply output
    */
  def runSequentially[A](func: () => A, delay: Int, runEvery: Int, numberOfTimesToRun: Int, doWhile: A => Boolean, finalWork: A => Unit): CountDownLatch = {
    val latch = new CountDownLatch(if (numberOfTimesToRun > 0) numberOfTimesToRun else 1)
    val t = new Timer()
    val task = new TimerTask() {
      def run(): Unit = {
        val res = func.apply()
        if(numberOfTimesToRun > 0){
          latch.countDown()
        }

        if (latch.getCount == 0)
        {
          throw new TimeoutException(s"runSequentially: Reached maximal allowed repetitions ($numberOfTimesToRun), aborting")
        }

        if (!doWhile.apply(res)){
          t.cancel()
          finalWork.apply(res)
          while (latch.getCount > 0){
            latch.countDown()
          }
        }
      }
    }
    t.schedule(task, delay, runEvery)
    latch
  }

  def All(list: util.ArrayList[Boolean]) : Boolean = {
    val it = list.iterator
    var res = true
    while (it.hasNext && res)
    {
      res = it.next()
    }
    res
  }

  @throws[IOException]
  private[kusto] def serializeRows(rows: Iterator[InternalRow], schema: StructType, bos: BlobOutputStream, timeZone: String): Unit = {
    val gzip = new GZIPOutputStream(bos)
    val csvSerializer = new KustoCsvSerializationUtils(schema, timeZone)

    val writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)
    val buffer:BufferedWriter = new BufferedWriter(writer, GZIP_BUFFER_SIZE)
    val csvWriter = new CsvWriter(buffer, new CsvWriterSettings)

    for (row <- rows) {
      csvWriter.writeRow(csvSerializer.convertRow(row))
    }

    buffer.flush()
    gzip.flush()

    buffer.close()
    gzip.close()
  }

  private def readIngestionResult(statusRecord: IngestionStatus): String = {
    val objectMapper = new ObjectMapper()
    val resultAsJson = objectMapper.writerWithDefaultPrettyPrinter.writeValueAsString (statusRecord)

    resultAsJson
  }
}

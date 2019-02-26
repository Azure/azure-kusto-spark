package com.microsoft.kusto.spark.datasink

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPOutputStream
import java.util.UUID

import com.microsoft.azure.kusto.data.{Client, ConnectionStringBuilder}
import com.microsoft.azure.kusto.ingest.IngestionProperties.DATA_FORMAT
import com.microsoft.azure.kusto.ingest.result.{IngestionResult, IngestionStatus, OperationStatus}
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.IngestionProperties
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature
import com.microsoft.azure.storage.blob.{BlobOutputStream, CloudBlobContainer}
import com.microsoft.kusto.spark.datasource._
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KeyVaultUtils, KustoClient, KustoQueryUtils, KustoDataSourceUtils => KDSU}
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.{FutureAction, TaskContext}
import shaded.parquet.org.codehaus.jackson.map.ObjectMapper

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

object KustoWriter{
  private val myName = this.getClass.getSimpleName
  val TempIngestionTablePrefix = "_tmpTable"
  var OperationShowCommandResult = 16
  val completedState = "Completed"
  val inProgressState = "InProgress"
  val stateCol = "State"
  val statusCol = "Status"
  val delayPeriodBetweenCalls: Int = KDSU.DefaultPeriodicSamplePeriod.toMillis.toInt
  val GZIP_BUFFER_SIZE: Int = 16 * 1024
  private[kusto] def write(batchId: Option[Long],
                           data: DataFrame,
                           tableCoordinates: KustoCoordinates,
                           authentication: KustoAuthentication,
                           writeOptions: WriteOptions): Unit = {
    val schema = data.schema
    var batchIdIfExists = { if (batchId.isEmpty || batchId == Option(0)) { "" } else { s"${batchId.get}"} }

    val kustoAdminClient = KustoClient.getAdmin(authentication, tableCoordinates.cluster)
    val appName = data.sparkSession.sparkContext.appName
    val table = tableCoordinates.table.get

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
        KDSU.reportExceptionAndThrow(myName, ex, "trying to drop temporary tables", tableCoordinates.cluster, tableCoordinates.database, table, isLogDontThrow = true)
    }

    val tmpTableName = KustoQueryUtils.simplifyName(s"$TempIngestionTablePrefix${appName}_$table${batchIdIfExists}_${UUID.randomUUID().toString}")

    if(batchIdIfExists != "") batchIdIfExists = s", batch'$batchIdIfExists'"

    // KustoWriter will create a temporary table ingesting the data to it.
    // Only if all executors succeeded the table will be appended to the original destination table.
    KDSU.createTmpTableWithSameSchema(kustoAdminClient, tableCoordinates, tmpTableName, writeOptions.tableCreateOptions, schema)

    KDSU.logInfo(myName, s"Successfully created temporary table $tmpTableName, will be deleted after completing the operation")


    val ingestKcsb = KustoClient.getKcsb(authentication, s"https://ingest-${tableCoordinates.cluster}.kusto.windows.net")
    val storageUri = TempStorageCache.getNewTempBlobReference(tableCoordinates.cluster, ingestKcsb)
    def ingestRowsIntoKusto(schema: StructType, rows: Iterator[InternalRow]): IngestionResult = {
      val ingestClient = KustoClient.getIngest(authentication, tableCoordinates.cluster)
      val ingestionProperties = new IngestionProperties(tableCoordinates.database, tmpTableName)

      val blobName = s"${tableCoordinates.database}_${tmpTableName}_${UUID.randomUUID.toString}_SparkStreamUpload.gz"
      val container = new CloudBlobContainer(new URI(storageUri))
      val blob = container.getBlockBlobReference(blobName)

      serializeRows(rows, schema, blob.openOutputStream, writeOptions.timeZone)

      val signature = blob.getServiceClient.getCredentials.asInstanceOf[StorageCredentialsSharedAccessSignature]
      val blobPath = blob.getStorageUri.getPrimaryUri.toString + "?" + signature.getToken
      val blobSourceInfo = new BlobSourceInfo(blobPath)

      ingestionProperties.setDataFormat(DATA_FORMAT.csv.name)
      ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table)
      ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses)
      ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties)
    }

    val rdd = data.queryExecution.toRdd

    if (writeOptions.isAsync) {
      val asyncWork: FutureAction[Unit] = rdd.foreachPartitionAsync {
        rows => {
          if(rows.isEmpty)
          {
            KDSU.logWarn(myName, s"sink to Kusto table '$table' with no rows to write on partition ${TaskContext.getPartitionId}")
          }
          else {
            ingestToTemporaryTableByWorkers(batchId,tableCoordinates.cluster, tableCoordinates.database, table, schema, batchIdIfExists, ingestRowsIntoKusto, rows)
          }
        }
      }
      KDSU.logInfo(myName, s"asynchronous write to Kusto table '$table' in progress")

      // This part runs back on the driver
      asyncWork.onSuccess {
        case _ =>
          finalizeIngestionWhenWorkersSucceeded(tableCoordinates.cluster, tableCoordinates.database, table, batchIdIfExists, kustoAdminClient, tmpTableName, isAsync = true)
      }

      asyncWork.onFailure {
        case exception: Exception =>
          tryCleanupIngestionByproducts(tableCoordinates.database, kustoAdminClient, tmpTableName)
          KDSU.reportExceptionAndThrow(myName, exception, "writing data", tableCoordinates.cluster, tableCoordinates.database, table, isLogDontThrow = true)
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
              KDSU.logWarn(myName, s"sink to Kusto table '$table' with no rows to write on partition ${TaskContext.getPartitionId}")
            }
            else
            {
              ingestToTemporaryTableByWorkers(batchId, tableCoordinates.cluster, tableCoordinates.database, table, schema, batchIdIfExists, ingestRowsIntoKusto, rows)
            }
          }
        }
      }
      catch{
        case exception: Exception =>
          tryCleanupIngestionByproducts(tableCoordinates.database, kustoAdminClient, tmpTableName)
          throw exception.getCause
      }

      finalizeIngestionWhenWorkersSucceeded(tableCoordinates.cluster, tableCoordinates.database, table, batchIdIfExists, kustoAdminClient, tmpTableName)
      KDSU.logInfo(myName, s"write operation to Kusto table '$table' finished successfully")
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
      val ingestionResult = Await.result(ackIngestion, KDSU.DefaultTimeoutLongRunning)
      val timeoutMillis = KDSU.DefaultTimeoutLongRunning.toMillis

      // Proceed only on success. Will throw on failure for the driver to handle
      KDSU.runSequentially[IngestionStatus](
        func = () => ingestionResult.GetIngestionStatusCollection().get(0),
        0, delayPeriodBetweenCalls, (timeoutMillis / delayPeriodBetweenCalls + 5).toInt,
        res => res.status == OperationStatus.Pending,
        res => res.status match {
          case OperationStatus.Pending =>
            throw new RuntimeException(s"Ingestion to Kusto failed on timeout failure. Cluster: '$cluster', database: '$database', table: '$table'$batchIdIfExists, partition: '$partitionId'")
          case OperationStatus.Succeeded =>
            KDSU.logInfo(myName, s"Ingestion to Kusto succeeded. Cluster: '$cluster', database: '$database', table: '$table'$batchIdIfExists, partition: '$partitionId'")
          case otherStatus =>
            throw new RuntimeException(s"Ingestion to Kusto failed with status '$otherStatus'." +
              s" Cluster: '$cluster', database: '$database', table: '$table'$batchIdIfExists, partition: '$partitionId'. Ingestion info: '${readIngestionResult(res)}'")
        }).await(timeoutMillis, TimeUnit.MILLISECONDS)
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

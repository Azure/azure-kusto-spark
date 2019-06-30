package com.microsoft.kusto.spark.utils

import java.util.StringJoiner
import java.util.concurrent.TimeUnit

import com.microsoft.azure.kusto.data.Client
import com.microsoft.azure.kusto.ingest.IngestClient
import com.microsoft.azure.kusto.ingest.result.{IngestionStatus, OperationStatus}
import com.microsoft.kusto.spark.datasink.{PartitionResult, SinkTableCreationMode}
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.KustoWriter.delayPeriodBetweenCalls
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils.extractSchemaFromResultTable
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator
import org.joda.time.{DateTime, DateTimeZone, Period}
import shaded.parquet.org.codehaus.jackson.map.ObjectMapper

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}

class KustoClient(val clusterAlias: String, val engineClient: Client, val dmClient: Client, val ingestClient: IngestClient) {

  private val myName = this.getClass.getSimpleName

  def createTmpTableWithSameSchema(tableCoordinates: KustoCoordinates,
                                   tmpTableName: String,
                                   tableCreation: SinkTableCreationMode = SinkTableCreationMode.FailIfNotExist,
                                   schema: StructType): Unit = {

    val schemaShowCommandResult = engineClient.execute(tableCoordinates.database, generateTableGetSchemaAsRowsCommand(tableCoordinates.table.get)).getValues
    var tmpTableSchema: String = ""
    val database = tableCoordinates.database
    val table = tableCoordinates.table.get

    if (schemaShowCommandResult.size() == 0) {
      // Table Does not exist
      if (tableCreation == SinkTableCreationMode.FailIfNotExist) {
        throw new RuntimeException("Table '" + table + "' doesn't exist in database '" + database + "', in cluster '" + tableCoordinates.cluster + "'")
      } else {
        // Parse dataframe schema and create a destination table with that schema
        val tableSchemaBuilder = new StringJoiner(",")
        schema.fields.foreach { field =>
          val fieldType = DataTypeMapping.getSparkTypeToKustoTypeMap(field.dataType)
          tableSchemaBuilder.add(s"['${field.name}']:$fieldType")
        }
        tmpTableSchema = tableSchemaBuilder.toString
        engineClient.execute(database, generateTableCreateCommand(table, tmpTableSchema))
      }
    } else {
      // Table exists. Parse kusto table schema and check if it matches the dataframes schema
      tmpTableSchema = extractSchemaFromResultTable(schemaShowCommandResult)
    }

    //  Create a temporary table with the kusto or dataframe parsed schema with 1 day retention
    engineClient.execute(database, generateTableCreateCommand(tmpTableName, tmpTableSchema))
    engineClient.execute(database, generateTableAlterRetentionPolicy(tmpTableName, "001:00:00:00", recoverable = false))
  }

  private var roundRobinIdx = 0
  private var storageUris: Seq[String] = Seq.empty
  private var lastRefresh: DateTime = new DateTime(DateTimeZone.UTC)

  def getNewTempBlobReference: String = {
    // Refresh if storageExpiryMinutes have passed since last refresh for this cluster as SAS should be valid for at least 120 minutes
    if (storageUris.isEmpty ||
      new Period(new DateTime(DateTimeZone.UTC), lastRefresh).getMinutes > KustoConstants.storageExpiryMinutes) {

      val res = dmClient.execute(generateCreateTmpStorageCommand())
      val storage = res.getValues.asScala.map(row => row.get(0))

      if (storage.isEmpty) {
        KDSU.reportExceptionAndThrow(myName, new RuntimeException("Failed to allocate temporary storage"), "writing to Kusto", clusterAlias)
      }

      lastRefresh = new DateTime(DateTimeZone.UTC)
      storageUris = storage
      roundRobinIdx = 0
      storage(roundRobinIdx)
    }
    else {
      roundRobinIdx = (roundRobinIdx + 1) % storageUris.size
      storageUris(roundRobinIdx)
    }
  }

  private[kusto] def finalizeIngestionWhenWorkersSucceeded(coordinates: KustoCoordinates,
                                                           batchIdIfExists: String,
                                                           kustoAdminClient: Client,
                                                           tmpTableName: String,
                                                           partitionsResults: CollectionAccumulator[PartitionResult],
                                                           timeout: FiniteDuration,
                                                           isAsync: Boolean = false): Unit = {
    import coordinates._

    val mergeTask = Future {
      try {
        partitionsResults.value.asScala.foreach {
          partitionResult =>
            KDSU.runSequentially[IngestionStatus](
              func = () => partitionResult.ingestionResult.getIngestionStatusCollection.get(0),
              0,
              delayPeriodBetweenCalls,
              (timeout.toMillis / delayPeriodBetweenCalls + 5).toInt,
              res => res.status == OperationStatus.Pending,
              res => res.status match {
                case OperationStatus.Pending =>
                  throw new RuntimeException(s"Ingestion to Kusto failed on timeout failure. Cluster: '${coordinates.cluster}', " +
                    s"database: '${coordinates.database}', table: '$tmpTableName', batch: '$batchIdIfExists', partition: '${partitionResult.partitionId}'")
                case OperationStatus.Succeeded =>
                  KDSU.logInfo(myName, s"Ingestion to Kusto succeeded. " +
                    s"Cluster: '${coordinates.cluster}', " +
                    s"database: '${coordinates.database}', " +
                    s"table: '$tmpTableName', batch: '$batchIdIfExists', partition: '${partitionResult.partitionId}'', from: '${res.ingestionSourcePath}', Operation ${res.operationId}")
                case otherStatus =>
                  throw new RuntimeException(s"Ingestion to Kusto failed with status '$otherStatus'." +
                    s" Cluster: '${coordinates.cluster}', database: '${coordinates.database}', " +
                    s"table: '$tmpTableName', batch: '$batchIdIfExists', partition: '${partitionResult.partitionId}''. Ingestion info: '${readIngestionResult(res)}'")
              }).await(timeout.toMillis, TimeUnit.MILLISECONDS)
        }

        // Move data to real table
        // Protect tmp table from merge/rebuild and move data to the table requested by customer. This operation is atomic.
        kustoAdminClient.execute(database, generateTableAlterMergePolicyCommand(tmpTableName, allowMerge = false, allowRebuild = false))
        kustoAdminClient.execute(database, generateTableMoveExtentsCommand(tmpTableName, table.get))
        KDSU.logInfo(myName, s"write to Kusto table '${table.get}' finished successfully $batchIdIfExists")
      } catch {
        case ex: Exception =>
          KDSU.reportExceptionAndThrow(
            myName,
            ex,
            "Trying to poll on pending ingestions", coordinates.cluster, coordinates.database, coordinates.table.getOrElse("Unspecified table name"),
            shouldNotThrow = true
          )
      } finally {
        cleanupIngestionByproducts(database, kustoAdminClient, tmpTableName)
      }
    }

    if(!isAsync)
    {
      Await.result(mergeTask, timeout)
    }
  }

  private[kusto] def cleanupIngestionByproducts(database: String, kustoAdminClient: Client, tmpTableName: String): Unit = {
    try {
      kustoAdminClient.execute(database, generateTableDropCommand(tmpTableName))
    }
    catch {
      case exception: Exception =>
        KDSU.reportExceptionAndThrow(myName, exception, s"deleting temporary table $tmpTableName", database = database, shouldNotThrow = true)
    }
  }

  private def readIngestionResult(statusRecord: IngestionStatus): String = {
    new ObjectMapper()
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(statusRecord)
  }
}

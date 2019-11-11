package com.microsoft.kusto.spark.utils

import java.util.StringJoiner
import java.util.concurrent.TimeUnit

import com.microsoft.azure.kusto.data.{Client, ClientFactory, ConnectionStringBuilder}
import com.microsoft.azure.kusto.ingest.result.{IngestionStatus, OperationStatus}
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestClientFactory, IngestionProperties}
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.KustoWriter.delayPeriodBetweenCalls
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.datasink.{PartitionResult, SinkTableCreationMode}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils.extractSchemaFromResultTable
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator
import org.joda.time.{DateTime, DateTimeZone, Period}
import shaded.parquet.org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}

class KustoClient(val clusterAlias: String, val engineKcsb: ConnectionStringBuilder, val ingestKcsb: ConnectionStringBuilder) {
  val engineClient: Client = ClientFactory.createClient(engineKcsb)

  // Reading process does not require ingest client to start working
  lazy val dmClient: Client = ClientFactory.createClient(ingestKcsb)
  lazy val ingestClient: IngestClient = IngestClientFactory.createClient(ingestKcsb)

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
  private var storageUris: Seq[ContainerAndSas] = Seq.empty
  private var lastRefresh: DateTime = new DateTime(DateTimeZone.UTC)

  def getNewTempBlobReference: ContainerAndSas = {
    // Refresh if storageExpiryMinutes have passed since last refresh for this cluster as SAS should be valid for at least 120 minutes
    if (storageUris.isEmpty ||
      new Period(new DateTime(DateTimeZone.UTC), lastRefresh).getMinutes > KustoConstants.storageExpiryMinutes) {

      val res = dmClient.execute(generateCreateTmpStorageCommand())
      val storage = res.getValues.asScala.map(row => {
        val parts = row.get(0).split('?'); ContainerAndSas(parts(0), '?' + parts(1))
      })

      if (storage.isEmpty) {
        KDSU.reportExceptionAndThrow(myName, new RuntimeException("Failed to allocate temporary storage"), "writing to Kusto", clusterAlias)
      }

      lastRefresh = new DateTime(DateTimeZone.UTC)
      storageUris = scala.util.Random.shuffle(storage)
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
      KDSU.logInfo(myName, s"Polling on ingestion results, will merge data to destination table when finished")

      try {
        partitionsResults.value.asScala.foreach {
          partitionResult => {
            var finalRes: IngestionStatus = null
            KDSU.doWhile[IngestionStatus](
              () => {
                finalRes = partitionResult.ingestionResult.getIngestionStatusCollection.get(0); finalRes
              },
              0,
              delayPeriodBetweenCalls,
              (timeout.toMillis / delayPeriodBetweenCalls + 5).toInt,
              res => res.status == OperationStatus.Pending,
              res => finalRes = res).await(timeout.toMillis, TimeUnit.MILLISECONDS)

            finalRes.status match {
              case OperationStatus.Pending =>
                throw new RuntimeException(s"Ingestion to Kusto failed on timeout failure. Cluster: '${coordinates.cluster}', " +
                  s"database: '${coordinates.database}', table: '$tmpTableName', batch: '$batchIdIfExists', partition: '${partitionResult.partitionId}'")
              case OperationStatus.Succeeded =>
                KDSU.logInfo(myName, s"Ingestion to Kusto succeeded. " +
                  s"Cluster: '${coordinates.cluster}', " +
                  s"database: '${coordinates.database}', " +
                  s"table: '$tmpTableName', batch: '$batchIdIfExists', partition: '${partitionResult.partitionId}'', from: '${finalRes.ingestionSourcePath}', Operation ${finalRes.operationId}")
              case otherStatus =>
                throw new RuntimeException(s"Ingestion to Kusto failed with status '$otherStatus'." +
                  s" Cluster: '${coordinates.cluster}', database: '${coordinates.database}', " +
                  s"table: '$tmpTableName', batch: '$batchIdIfExists', partition: '${partitionResult.partitionId}''. Ingestion info: '${readIngestionResult(finalRes)}'")
            }
          }
        }

        if (partitionsResults.value.size > 0) {
          // Move data to real table
          // Protect tmp table from merge/rebuild and move data to the table requested by customer. This operation is atomic.
          kustoAdminClient.execute(database, generateTableAlterMergePolicyCommand(tmpTableName, allowMerge = false, allowRebuild = false))
          kustoAdminClient.execute(database, generateTableMoveExtentsCommand(tmpTableName, table.get))
          KDSU.logInfo(myName, s"write to Kusto table '${table.get}' finished successfully $batchIdIfExists")
        } else {
          KDSU.logWarn(myName, s"write to Kusto table '${table.get}' finished with no data written")
        }
      } catch {
        case ex: Exception =>
          KDSU.reportExceptionAndThrow(
            myName,
            ex,
            "Trying to poll on pending ingestions", coordinates.cluster, coordinates.database, coordinates.table.getOrElse("Unspecified table name")
          )
      } finally {
        cleanupIngestionByproducts(database, kustoAdminClient, tmpTableName)
      }
    }

    if (!isAsync) {
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

  private[kusto] def setMappingOnStagingTableIfNeeded(ingestionProperties: IngestionProperties, originalTable: String): Unit = {
    val mapping = ingestionProperties.getIngestionMapping
    val mappingReferenceName = mapping.getIngestionMappingReference
    if (StringUtils.isNotBlank(mappingReferenceName)) {
      val mappingKind = mapping.getIngestionMappingKind.toString
      val cmd = generateShowTableMappingsCommand(originalTable, mappingKind)
      val mappings = engineClient.execute(ingestionProperties.getDatabaseName, cmd).getValues
      val it = mappings.iterator()
      var found = false
      while (it.hasNext && !found){
        val policy = it.next()
        if(policy.get(0).equals(mappingReferenceName)){
          val policyJson = policy.get(2).replace("\"","'")
          val c = generateCreateTableMappingCommand(ingestionProperties.getTableName, mappingKind, mappingReferenceName, policyJson)
          engineClient.execute(ingestionProperties.getDatabaseName, c)
          found = true
        }
      }
    }
  }

  private def readIngestionResult(statusRecord: IngestionStatus): String = {
    new ObjectMapper()
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(statusRecord)
  }
}

case class ContainerAndSas(containerUrl: String, sas: String)

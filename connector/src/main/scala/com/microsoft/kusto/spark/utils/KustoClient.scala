package com.microsoft.kusto.spark.utils

import java.time.Instant
import java.util
import java.util.StringJoiner
import java.util.concurrent.TimeUnit

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.data.{Client, ClientFactory, ClientRequestProperties, KustoResultSetTable}
import com.microsoft.azure.kusto.ingest.result.{IngestionStatus, OperationStatus}
import com.microsoft.azure.kusto.ingest.{IngestClient, IngestClientFactory, IngestionProperties}
import com.microsoft.azure.storage.StorageException
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.KustoWriter.DelayPeriodBetweenCalls
import com.microsoft.kusto.spark.datasink.{PartitionResult, SinkTableCreationMode, SparkIngestionProperties, WriteOptions}
import com.microsoft.kusto.spark.datasource.KustoStorageParameters
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils.extractSchemaFromResultTable
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator
import org.json.{JSONArray, JSONObject}
import shaded.parquet.org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class KustoClient(val clusterAlias: String, val engineKcsb: ConnectionStringBuilder, val ingestKcsb: ConnectionStringBuilder) {
  lazy val engineClient: Client = ClientFactory.createClient(engineKcsb)

  // Reading process does not require ingest client to start working
  lazy val dmClient: Client = ClientFactory.createClient(ingestKcsb)
  lazy val ingestClient: IngestClient = IngestClientFactory.createClient(ingestKcsb)
  private val exportProviderEntryCreator = (c: ContainerAndSas) => KDSU.parseSas(c.containerUrl + c.sas)
  private val ingestProviderEntryCreator = (c: ContainerAndSas) => c
  private lazy val ingestContainersContainerProvider = new ContainerProvider(dmClient, clusterAlias, generateCreateTmpStorageCommand(), ingestProviderEntryCreator)
  private lazy val exportContainersContainerProvider = new ContainerProvider(dmClient, clusterAlias, generateGetExportContainersCommand(), exportProviderEntryCreator)

  private val myName = this.getClass.getSimpleName
  private val durationFormat = "dd:HH:mm:ss"

  def initializeTablesBySchema(tableCoordinates: KustoCoordinates,
                               tmpTableName: String,
                               schema: StructType,
                               schemaShowCommandResult: KustoResultSetTable,
                               writeOptions: WriteOptions,
                               crp: ClientRequestProperties): Unit = {

    var tmpTableSchema: String = ""
    val database = tableCoordinates.database
    val table = tableCoordinates.table.get

    if (schemaShowCommandResult.count() == 0) {
      // Table Does not exist
      if (writeOptions.tableCreateOptions == SinkTableCreationMode.FailIfNotExist) {
        throw new RuntimeException(s"Table '$table' doesn't exist in database '$database', cluster '${tableCoordinates.clusterAlias} and tableCreateOptions is set to FailIfNotExist.")
      } else {
        // Parse dataframe schema and create a destination table with that schema
        val tableSchemaBuilder = new StringJoiner(",")
        schema.fields.foreach { field =>
          val fieldType = DataTypeMapping.getSparkTypeToKustoTypeMap(field.dataType)
          tableSchemaBuilder.add(s"['${field.name}']:$fieldType")
        }
        tmpTableSchema = tableSchemaBuilder.toString
        engineClient.execute(database, generateTableCreateCommand(table, tmpTableSchema), crp)
      }
    } else {
      // Table exists. Parse kusto table schema and check if it matches the dataframes schema
      tmpTableSchema = extractSchemaFromResultTable(schemaShowCommandResult.getData.asScala.map(c => c.get(0).asInstanceOf[JSONObject]))
    }

    // Create a temporary table with the kusto or dataframe parsed schema with retention and delete set to after the
    // write operation times out. Engine recommended keeping the retention although we use auto delete.
    engineClient.execute(database, generateTempTableCreateCommand(tmpTableName, tmpTableSchema), crp)
    engineClient.execute(database, generateTableAlterRetentionPolicy(tmpTableName,
      DurationFormatUtils.formatDuration(writeOptions.autoCleanupTime.toMillis, durationFormat, true),
      recoverable = false), crp)
    val instant = Instant.now.plusSeconds(writeOptions.autoCleanupTime.toSeconds)
    engineClient.execute(database, generateTableAlterAutoDeletePolicy(tmpTableName, instant), crp)
  }

  def getTempBlobForIngestion: ContainerAndSas = {
    ingestContainersContainerProvider.getContainer
  }

  def getTempBlobsForExport: Seq[KustoStorageParameters] = {
    exportContainersContainerProvider.getAllContainers
  }

  private[kusto] def finalizeIngestionWhenWorkersSucceeded(coordinates: KustoCoordinates,
                                                           batchIdIfExists: String,
                                                           kustoAdminClient: Client,
                                                           tmpTableName: String,
                                                           partitionsResults: CollectionAccumulator[PartitionResult],
                                                           writeOptions: WriteOptions,
                                                           ingestIfNotExistsTags: util.ArrayList[String],
                                                           crp: ClientRequestProperties
                                                          ): Unit = {
    import coordinates._

    val mergeTask = Future {
      KDSU.logInfo(myName, s"Polling on ingestion results for requestId: ${writeOptions.requestId}, will move data to " +
        s"destination table when finished")

      try {
        partitionsResults.value.asScala.foreach {
          partitionResult => {
            var finalRes: Option[IngestionStatus] = None
            KDSU.doWhile[Option[IngestionStatus]](
              () => {
                try {
                  finalRes = Some(partitionResult.ingestionResult.getIngestionStatusCollection.get(0));
                  finalRes
                } catch {
                  case _: StorageException =>
                    KDSU.logWarn(myName, s"Failed to fetch operation status transiently - will keep polling. RequestId: ${writeOptions.requestId}")
                    None
                  case e: Exception => KDSU.reportExceptionAndThrow(myName, e, s"Failed to fetch operation status. RequestId: ${writeOptions.requestId}"); None
                }
              },
              0,
              DelayPeriodBetweenCalls,
              (writeOptions.timeout.toMillis / DelayPeriodBetweenCalls + 5).toInt,
              res => res.isDefined && res.get.status == OperationStatus.Pending,
              res => finalRes = res,
              maxWaitTimeBetweenCalls = KDSU.WriteMaxWaitTime.toMillis.toInt)
               .await(writeOptions.timeout.toMillis, TimeUnit.MILLISECONDS)

            if (finalRes.isDefined) {
              finalRes.get.status match {
                case OperationStatus.Pending =>
                  throw new RuntimeException(s"Ingestion to Kusto failed on timeout failure. Cluster: '${coordinates.clusterAlias}', " +
                    s"database: '${coordinates.database}', table: '$tmpTableName'$batchIdIfExists, partition: '${partitionResult.partitionId}'")
                case OperationStatus.Succeeded =>
                  KDSU.logInfo(myName, s"Ingestion to Kusto succeeded. " +
                    s"Cluster: '${coordinates.clusterAlias}', " +
                    s"database: '${coordinates.database}', " +
                    s"table: '$tmpTableName'$batchIdIfExists, partition: '${partitionResult.partitionId}'', from: '${finalRes.get.ingestionSourcePath}', Operation ${finalRes.get.operationId}")
                case otherStatus =>
                  throw new RuntimeException(s"Ingestion to Kusto failed with status '$otherStatus'." +
                    s" Cluster: '${coordinates.clusterAlias}', database: '${coordinates.database}', " +
                    s"table: '$tmpTableName'$batchIdIfExists, partition: '${partitionResult.partitionId}'. Ingestion info: '${readIngestionResult(finalRes.get)}'")
              }
            } else {
              throw new RuntimeException("Failed to poll on ingestion status.")
            }
          }
        }

        if (partitionsResults.value.size > 0) {
          // Move data to real table
          // Protect tmp table from merge/rebuild and move data to the table requested by customer. This operation is atomic.
          // We are using the ingestIfNotExists Tags here too (on top of the check at the start of the flow) so that if
          // several flows started together only one of them would ingest
          kustoAdminClient.execute(database, generateTableAlterMergePolicyCommand(tmpTableName, allowMerge = false,
            allowRebuild = false), crp)
          val res = kustoAdminClient.execute(database, generateTableMoveExtentsCommand(tmpTableName, table.get,
            ingestIfNotExistsTags), crp).getPrimaryResults
          if (!res.next()) {
            // Extents that were moved are returned by move extents command
            KDSU.logInfo(myName, s"Ingestion skipped: Provided ingest-by tags are present in the destination table '$table'")
          }
          KDSU.logInfo(myName, s"write to Kusto table '${table.get}' finished successfully requestId: ${writeOptions.requestId} $batchIdIfExists")
        } else {
          KDSU.logWarn(myName, s"write to Kusto table '${table.get}' finished with no data written requestId: ${writeOptions.requestId} $batchIdIfExists")
        }
      } catch {
        case ex: Exception =>
          KDSU.reportExceptionAndThrow(
            myName,
            ex,
            "Trying to poll on pending ingestions", coordinates.clusterUrl, coordinates.database, coordinates.table.getOrElse("Unspecified table name")
          )
      } finally {
        cleanupIngestionByproducts(database, kustoAdminClient, tmpTableName, crp)
      }
    }

    if (!writeOptions.isAsync) {
      Await.result(mergeTask, writeOptions.timeout)
    }
  }

  private[kusto] def cleanupIngestionByproducts(database: String, kustoAdminClient: Client, tmpTableName: String,
                                                crp: ClientRequestProperties): Unit = {
    try {
      kustoAdminClient.execute(database, generateTableDropCommand(tmpTableName), crp)
      KDSU.logInfo(myName, s"Temporary table '$tmpTableName' deleted successfully")
    }
    catch {
      case exception: Exception =>
        KDSU.reportExceptionAndThrow(myName, exception, s"deleting temporary table $tmpTableName", database, shouldNotThrow = false)
    }
  }

  private[kusto] def setMappingOnStagingTableIfNeeded(stagingTableIngestionProperties: IngestionProperties,
                                                      originalTable: String, crp: ClientRequestProperties): Unit = {
    val mapping = stagingTableIngestionProperties.getIngestionMapping
    val mappingReferenceName = mapping.getIngestionMappingReference
    if (StringUtils.isNotBlank(mappingReferenceName)) {
      val mappingKind = mapping.getIngestionMappingKind.toString
      val cmd = generateShowTableMappingsCommand(originalTable, mappingKind)
      val mappings = engineClient.execute(stagingTableIngestionProperties.getDatabaseName, cmd).getPrimaryResults

      var found = false
      while (mappings.next && !found) {
        if (mappings.getString(0).equals(mappingReferenceName)) {
          val policyJson = mappings.getString(2).replace("\"", "'")
          val cmd = generateCreateTableMappingCommand(stagingTableIngestionProperties.getTableName, mappingKind,
            mappingReferenceName, policyJson)
          engineClient.execute(stagingTableIngestionProperties.getDatabaseName, cmd, crp)
          found = true
        }
      }
    }
  }

  def fetchTableExtentsTags(database: String, table: String, crp: ClientRequestProperties): KustoResultSetTable = {
    engineClient.execute(database, CslCommandsGenerator.generateFetchTableIngestByTagsCommand(table)).getPrimaryResults
  }

  def shouldIngestData(tableCoordinates: KustoCoordinates, ingestionProperties: Option[String],
                       tableExists: Boolean, crp: ClientRequestProperties): Boolean = {
    var shouldIngest = true

    if (tableExists && ingestionProperties.isDefined) {
      val ingestIfNotExistsTags = SparkIngestionProperties.fromString(ingestionProperties.get).ingestIfNotExists
      if (ingestIfNotExistsTags != null && !ingestIfNotExistsTags.isEmpty) {
        val ingestIfNotExistsTagsSet = ingestIfNotExistsTags.asScala.toSet

        val res = fetchTableExtentsTags(tableCoordinates.database, tableCoordinates.table.get, crp)
        if (res.next()) {
          val tagsArray = res.getObject(0).asInstanceOf[JSONArray]
          for (i <- 0 until tagsArray.length) {
            if (ingestIfNotExistsTagsSet.contains(tagsArray.getString(i))) {
              shouldIngest = false
            }
          }
        }
      }
    }

    shouldIngest
  }

  private def readIngestionResult(statusRecord: IngestionStatus): String = {
    new ObjectMapper()
      .writerWithDefaultPrettyPrinter
      .writeValueAsString(statusRecord)
  }
}


case class ContainerAndSas(containerUrl: String, sas: String)

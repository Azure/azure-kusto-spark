package com.microsoft.kusto.spark.utils

import java.util.StringJoiner

import com.microsoft.azure.kusto.data.Client
import com.microsoft.azure.kusto.ingest.IngestClient
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode
import com.microsoft.kusto.spark.datasink.SinkTableCreationMode.SinkTableCreationMode
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils.extractSchemaFromResultTable
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.types.StructType
import org.joda.time.{DateTime, DateTimeZone, Period}

import scala.collection.JavaConverters._

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
        throw new RuntimeException("Table '" + table + "' doesn't exist in database " + database + "', in cluster '" + tableCoordinates.cluster + "'")
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
    engineClient.execute(database, generateTableAlterRetentionPolicy(tmpTableName, "100:00:00:00", recoverable = false))
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
}

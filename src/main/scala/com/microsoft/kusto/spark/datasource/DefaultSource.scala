package com.microsoft.kusto.spark.datasource

import com.microsoft.kusto.spark.datasink.KustoWriter
import com.microsoft.kusto.spark.utils.KustoQueryUtils
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.concurrent.duration._

class DefaultSource extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {

  val timeout: FiniteDuration = 10 minutes
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val (isAsync,tableCreation) = KustoDataSourceUtils.validateSinkParameters(parameters)

    KustoWriter.write(
      None,
      data,
      parameters.getOrElse(KustoOptions.KUSTO_CLUSTER, ""),
      parameters.getOrElse(KustoOptions.KUSTO_DATABASE, ""),
      parameters.getOrElse(KustoOptions.KUSTO_TABLE, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_ID, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com"),
      isAsync,
      tableCreation,
      mode)

    // We only read back the first row
    createRelation(sqlContext, adjustParametersForBaseRelation(parameters))
  }

  def adjustParametersForBaseRelation(parameters: Map[String, String]): Map[String, String] = {
    val table = parameters.get(KustoOptions.KUSTO_DATABASE)

    if (table.isEmpty){
      throw new RuntimeException("Cannot read from Kusto: table name is missing")
    }

    parameters + (KustoOptions.KUSTO_TABLE -> KustoQueryUtils.getQuerySchemaQuery(table.get)) + (KustoOptions.KUSTO_NUM_PARTITIONS -> "1")
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    if (!parameters.getOrElse(KustoOptions.KUSTO_NUM_PARTITIONS, "1").equals("1")) {
      throw new NotImplementedException()
    }

    KustoRelation(
      parameters.getOrElse(KustoOptions.KUSTO_CLUSTER, ""),
      parameters.getOrElse(KustoOptions.KUSTO_DATABASE, ""),
      parameters.getOrElse(KustoOptions.KUSTO_TABLE, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_ID, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com"),
      parameters.getOrElse(KustoOptions.KUSTO_QUERY, ""),
      parameters.get(KustoOptions.KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES))(sqlContext)
  }

  override def shortName(): String = "kusto"
}

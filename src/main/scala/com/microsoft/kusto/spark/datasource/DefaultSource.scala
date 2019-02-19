package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException

import com.microsoft.kusto.spark.datasink.KustoWriter
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils, KustoQueryUtils}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.concurrent.duration._

class DefaultSource extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {

  val timeout: FiniteDuration = 10 minutes
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val (writeOptions, authentication, tableCoordinates) = KustoDataSourceUtils.parseSinkParameters(parameters, mode)

    KustoWriter.write(
      None,
      data,
      tableCoordinates,
      authentication,
      writeOptions)

    val limit = if (writeOptions.writeResultLimit.equalsIgnoreCase(KustoOptions.NONE_RESULT_LIMIT)) None else {
        try{
          Some(writeOptions.writeResultLimit.toInt)
        }
        catch {
          case _: Exception => throw new InvalidParameterException(s"KustoOptions.KUSTO_WRITE_RESULT_LIMIT is set to '${writeOptions.writeResultLimit}'. Must be either 'none' or integer value")
        }
      }

    createRelation(sqlContext, adjustParametersForBaseRelation(parameters, limit))
  }

  def adjustParametersForBaseRelation(parameters: Map[String, String], limit: Option[Int]): Map[String, String] = {
    if (limit.isEmpty) {
      parameters + (KustoOptions.KUSTO_NUM_PARTITIONS -> "1")
    }
    else {
      parameters + (KustoOptions.KUSTO_QUERY -> KustoQueryUtils.limitQuery(parameters(KustoOptions.KUSTO_TABLE), limit.get)) + (KustoOptions.KUSTO_NUM_PARTITIONS -> "1")
    }
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    if (!parameters.getOrElse(KustoOptions.KUSTO_NUM_PARTITIONS, "1").equals("1")) {
      throw new NotImplementedException()
    }

    if(!KustoOptions.supportedReadModes.contains(parameters.getOrElse(KustoOptions.KUSTO_READ_MODE, "lean").toLowerCase)) {
      throw new InvalidParameterException(s"Kusto read mode must be one of ${KustoOptions.supportedReadModes.mkString(", ")}")
    }

    KustoRelation(
      parameters.getOrElse(KustoOptions.KUSTO_CLUSTER, ""),
      parameters.getOrElse(KustoOptions.KUSTO_DATABASE, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_ID, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com"),
      parameters.getOrElse(KustoOptions.KUSTO_QUERY, ""),
      parameters.getOrElse(KustoOptions.KUSTO_READ_MODE, "lean").equalsIgnoreCase("lean"),
      parameters.get(KustoOptions.KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES))(sqlContext)
  }

  override def shortName(): String = "kusto"
}
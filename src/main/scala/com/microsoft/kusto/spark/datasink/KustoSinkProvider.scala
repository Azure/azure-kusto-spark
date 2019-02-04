package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.datasource.KustoOptions
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class KustoSinkProvider extends StreamSinkProvider with DataSourceRegister{

  override def shortName(): String = "KustoSink"

  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode): Sink = {
    val (isAsync,tableCreation) = KustoDataSourceUtils.validateSinkParameters(parameters)

    new KustoSink(
      sqlContext,
      parameters.getOrElse(KustoOptions.KUSTO_CLUSTER, ""),
      parameters.getOrElse(KustoOptions.KUSTO_DATABASE, ""),
      parameters.getOrElse(KustoOptions.KUSTO_TABLE, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_ID, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com"),
      isAsync,
      tableCreation,
      parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, "UTC")
    )
  }
}



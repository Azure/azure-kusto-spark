package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.datasource._
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
    val (isAsync,tableCreation, kustoAuthentication) = KustoDataSourceUtils.validateSinkParameters(parameters)
    val tableCoordinates = KustoTableCoordinates(parameters.getOrElse(KustoOptions.KUSTO_CLUSTER, ""), parameters.getOrElse(KustoOptions.KUSTO_DATABASE, ""),parameters.getOrElse(KustoOptions.KUSTO_TABLE, ""))
    val writeOptions = KustoSparkWriteOptions(tableCreation, isAsync, parameters.getOrElse(KustoOptions.KUSTO_WRITE_RESULT_LIMIT, "1"), parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, "UTC"))

    new KustoSink(
      sqlContext,
      tableCoordinates,
      KustoDataSourceUtils.getAadParamsFromKeyVaultIfNeeded(kustoAuthentication),
      writeOptions
    )
  }
}



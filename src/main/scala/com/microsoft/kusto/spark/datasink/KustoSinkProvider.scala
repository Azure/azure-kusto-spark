package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.datasource._
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class KustoSinkProvider extends StreamSinkProvider with DataSourceRegister {

  override def shortName(): String = "KustoSink"

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    val (writeOptions, authentication, tableCoordinates) = KustoDataSourceUtils.parseSinkParameters(parameters)

    new KustoSink(
      sqlContext,
      tableCoordinates,
      authentication,
      writeOptions
    )
  }
}

package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.utils.{KeyVaultUtils, KustoDataSourceUtils}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class KustoSinkProvider extends StreamSinkProvider with DataSourceRegister {

  override def shortName(): String = "KustoSink"

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    val sinkParameters = KustoDataSourceUtils.parseSinkParameters(parameters)

    new KustoSink(
      sqlContext,
      sinkParameters.sourceParametersResults.kustoCoordinates,
      if(sinkParameters.sourceParametersResults.keyVaultAuth.isDefined){
        val paramsFromKeyVault = KeyVaultUtils.getAadAppParametersFromKeyVault(sinkParameters.sourceParametersResults.keyVaultAuth.get)
        KustoDataSourceUtils.mergeKeyVaultAndOptionsAuthentication(paramsFromKeyVault, Some(sinkParameters.sourceParametersResults.authenticationParameters))
      } else sinkParameters.sourceParametersResults.authenticationParameters,
      sinkParameters.writeOptions,
      sinkParameters.sourceParametersResults.clientRequestProperties
    )
  }
}

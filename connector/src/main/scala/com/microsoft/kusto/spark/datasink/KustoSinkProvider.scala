//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.utils.{KeyVaultUtils, KustoDataSourceUtils}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class KustoSinkProvider extends StreamSinkProvider with DataSourceRegister {

  override def shortName(): String = "KustoSink"

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val sinkParameters = KustoDataSourceUtils.parseSinkParameters(parameters)

    new KustoSink(
      sqlContext,
      sinkParameters.sourceParametersResults.kustoCoordinates,
      if (sinkParameters.sourceParametersResults.keyVaultAuth.isDefined) {
        val paramsFromKeyVault = KeyVaultUtils.getAadAppParametersFromKeyVault(
          sinkParameters.sourceParametersResults.keyVaultAuth.get)
        KustoDataSourceUtils.mergeKeyVaultAndOptionsAuthentication(
          paramsFromKeyVault,
          Some(sinkParameters.sourceParametersResults.authenticationParameters))
      } else sinkParameters.sourceParametersResults.authenticationParameters,
      sinkParameters.writeOptions,
      sinkParameters.sourceParametersResults.clientRequestProperties)
  }
}

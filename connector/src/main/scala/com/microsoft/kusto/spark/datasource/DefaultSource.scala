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

package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.concurrent.TimeUnit
import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.{KeyVaultAuthentication, KustoAuthentication}
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, KustoWriter}
import com.microsoft.kusto.spark.utils.{
  KeyVaultUtils,
  KustoQueryUtils,
  KustoConstants => KCONST,
  KustoDataSourceUtils => KDSU
}
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils.SourceParameters
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.sources.{
  BaseRelation,
  CreatableRelationProvider,
  DataSourceRegister,
  RelationProvider
}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.concurrent.duration.FiniteDuration

class DefaultSource
    extends CreatableRelationProvider
    with RelationProvider
    with DataSourceRegister {
  var authenticationParameters: Option[KustoAuthentication] = None
  var kustoCoordinates: KustoCoordinates = _
  var keyVaultAuthentication: Option[KeyVaultAuthentication] = None
  var clientRequestProperties: Option[ClientRequestProperties] = None
  var requestId: Option[String] = None
  val myName: String = this.getClass.getSimpleName

  def initCommonParams(sourceParams: SourceParameters): Unit = {
    keyVaultAuthentication = sourceParams.keyVaultAuth
    kustoCoordinates = sourceParams.kustoCoordinates
    authenticationParameters = Some(sourceParams.authenticationParameters)
    requestId = Some(sourceParams.requestId)
    clientRequestProperties = Some(sourceParams.clientRequestProperties)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val sinkParameters = KDSU.parseSinkParameters(parameters, mode)
    initCommonParams(sinkParameters.sourceParametersResults)

    if (keyVaultAuthentication.isDefined) {
      val paramsFromKeyVault =
        KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultAuthentication.get)
      authenticationParameters = Some(
        KDSU.mergeKeyVaultAndOptionsAuthentication(paramsFromKeyVault, authenticationParameters))
    }

    KustoWriter.write(
      None,
      data,
      kustoCoordinates,
      authenticationParameters.get,
      sinkParameters.writeOptions,
      clientRequestProperties.get)

    val limit =
      if (sinkParameters.writeOptions.writeResultLimit.equalsIgnoreCase(
          KustoSinkOptions.NONE_RESULT_LIMIT)) None
      else {
        try {
          Some(sinkParameters.writeOptions.writeResultLimit.toInt)
        } catch {
          case _: Exception =>
            throw new InvalidParameterException(
              s"KustoOptions.KUSTO_WRITE_RESULT_LIMIT is set to '${sinkParameters.writeOptions.writeResultLimit}'. Must be either 'none' or an integer value")
        }
      }

    createRelation(sqlContext, adjustParametersForBaseRelation(parameters, limit))
  }

  def adjustParametersForBaseRelation(
      parameters: Map[String, String],
      limit: Option[Int]): Map[String, String] = {
    if (limit.isDefined) {
      parameters + (KustoSourceOptions.KUSTO_QUERY -> KustoQueryUtils.limitQuery(
        parameters(KustoSinkOptions.KUSTO_TABLE),
        limit.get))
    } else {
      parameters
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val readOptions = KDSU.getReadParameters(parameters, sqlContext)
    if (authenticationParameters.isEmpty) {
      // Parse parameters if haven't got parsed before
      val sourceParameters = KDSU.parseSourceParameters(parameters, true)
      initCommonParams(sourceParameters)
    }

    val storageOption = parameters.get(KustoSourceOptions.KUSTO_TRANSIENT_STORAGE)
    val transientStorageParams: Option[TransientStorageParameters] =
      if (storageOption.isDefined) {
        Some(TransientStorageParameters.fromString(storageOption.get))
      } else {
        None
      }
    val (kustoAuthentication, mergedStorageParameters)
        : (Option[KustoAuthentication], Option[TransientStorageParameters]) = {
      if (keyVaultAuthentication.isDefined) {
        // Get params from keyVault
        authenticationParameters = Some(
          KDSU.mergeKeyVaultAndOptionsAuthentication(
            KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultAuthentication.get),
            authenticationParameters))

        (
          authenticationParameters,
          KDSU.mergeKeyVaultAndOptionsStorageParams(
            transientStorageParams,
            keyVaultAuthentication.get))
      } else if (transientStorageParams.isDefined) {
        // If any of the storage parameters defined a SAS we will take endpoint suffix from there
        transientStorageParams.get.storageCredentials.foreach(st => {
          st.validate()
          if (StringUtils.isNoneBlank(st.domainSuffix)) {
            transientStorageParams.get.endpointSuffix = st.domainSuffix
          }
        })
        // Params passed from options
        (authenticationParameters, transientStorageParams)
      } else {
        (authenticationParameters, None)
      }
    }

    val timeout = new FiniteDuration(
      parameters
        .getOrElse(
          KustoSourceOptions.KUSTO_TIMEOUT_LIMIT,
          KCONST.DefaultWaitingIntervalLongRunning)
        .toLong,
      TimeUnit.SECONDS)

    KDSU.logInfo(
      myName,
      s"Finished serializing parameters for reading: {requestId: $requestId, timeout: $timeout, readMode: ${readOptions.readMode
          .getOrElse("Default")}, clientRequestProperties: $clientRequestProperties")
    KustoRelation(
      kustoCoordinates,
      kustoAuthentication.get,
      parameters.getOrElse(KustoSourceOptions.KUSTO_QUERY, ""),
      readOptions,
      timeout,
      parameters.get(KustoSourceOptions.KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES),
      mergedStorageParameters,
      clientRequestProperties,
      requestId.get)(sqlContext.sparkSession)
  }

  override def shortName(): String = "kusto"
}

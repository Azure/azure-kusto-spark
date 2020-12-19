package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.concurrent.TimeUnit

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.{KeyVaultAuthentication, KustoAuthentication}
import com.microsoft.kusto.spark.common.{KustoCoordinates, KustoDebugOptions}
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, KustoWriter}
import com.microsoft.kusto.spark.datasource.ReadMode.ReadMode
import com.microsoft.kusto.spark.utils.{KeyVaultUtils, KustoQueryUtils, KustoConstants => KCONST, KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.concurrent.duration.FiniteDuration

class DefaultSource extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {
  var authenticationParameters: Option[KustoAuthentication] = None
  var kustoCoordinates: KustoCoordinates = _
  var keyVaultAuthentication: Option[KeyVaultAuthentication] = None
  var clientRequestProperties: Option[ClientRequestProperties] = None
  val myName: String = this.getClass.getSimpleName

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val sinkParameters = KDSU.parseSinkParameters(parameters, mode)
    keyVaultAuthentication = sinkParameters.sourceParametersResults.keyVaultAuth
    kustoCoordinates = sinkParameters.sourceParametersResults.kustoCoordinates
    authenticationParameters = Some(sinkParameters.sourceParametersResults.authenticationParameters)

    if (keyVaultAuthentication.isDefined) {
      val paramsFromKeyVault = KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultAuthentication.get)
      authenticationParameters = Some(KDSU.mergeKeyVaultAndOptionsAuthentication(paramsFromKeyVault, authenticationParameters))
    }

    KustoWriter.write(
      None,
      data,
      kustoCoordinates,
      authenticationParameters.get,
      sinkParameters.writeOptions)

    val limit = if (sinkParameters.writeOptions.writeResultLimit.equalsIgnoreCase(KustoSinkOptions.NONE_RESULT_LIMIT)) None else {
      try {
        Some(sinkParameters.writeOptions.writeResultLimit.toInt)
      }
      catch {
        case _: Exception => throw new InvalidParameterException(s"KustoOptions.KUSTO_WRITE_RESULT_LIMIT is set to '${sinkParameters.writeOptions.writeResultLimit}'. Must be either 'none' or an integer value")
      }
    }

    createRelation(sqlContext, adjustParametersForBaseRelation(parameters, limit))
  }

  def adjustParametersForBaseRelation(parameters: Map[String, String], limit: Option[Int]): Map[String, String] = {
    if (limit.isDefined) {
      parameters + (KustoSourceOptions.KUSTO_QUERY -> KustoQueryUtils.limitQuery(parameters(KustoSinkOptions.KUSTO_TABLE), limit.get))
    } else {
      parameters
    }
  }

  private[kusto] def blobStorageAttributesProvided(parameters: Map[String, String]) = {
    parameters.get(KustoSourceOptions.KUSTO_BLOB_STORAGE_SAS_URL).isDefined || (parameters.get(KustoSourceOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME).isDefined &&
      parameters.get(KustoSourceOptions.KUSTO_BLOB_CONTAINER).isDefined && parameters.get(KustoSourceOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY).isDefined)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val requestedPartitions = parameters.get(KustoDebugOptions.KUSTO_NUM_PARTITIONS)
    val partitioningMode = parameters.get(KustoDebugOptions.KUSTO_READ_PARTITION_MODE)
    val shouldCompressOnExport = parameters.getOrElse(KustoDebugOptions.KUSTO_DBG_BLOB_COMPRESS_ON_EXPORT, "true").trim.toBoolean
    // Set default export split limit as 1GB, maximal allowed
    val exportSplitLimitMb = parameters.getOrElse(KustoDebugOptions.KUSTO_DBG_BLOB_FILE_SIZE_LIMIT_MB, "1024").trim.toInt

    val numPartitions = setNumPartitions(sqlContext, requestedPartitions, partitioningMode)
    var storageSecretIsAccountKey = true
    var storageSecret = parameters.get(KustoSourceOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY)

    if (storageSecret.isEmpty) {
      storageSecret = parameters.get(KustoSourceOptions.KUSTO_BLOB_STORAGE_SAS_URL)
      if (storageSecret.isDefined) storageSecretIsAccountKey = false
    }

    if (authenticationParameters.isEmpty) {
      // Parse parameters if haven't got parsed before
      val sourceParameters = KDSU.parseSourceParameters(parameters)
      authenticationParameters = Some(sourceParameters.authenticationParameters)
      kustoCoordinates = sourceParameters.kustoCoordinates
      keyVaultAuthentication = sourceParameters.keyVaultAuth
      clientRequestProperties = KDSU.getClientRequestProperties(parameters)
    }

    val (kustoAuthentication, storageParameters): (Option[KustoAuthentication], Option[KustoStorageParameters]) =
      if (keyVaultAuthentication.isDefined) {
        // Get params from keyVault
        authenticationParameters = Some(KDSU.mergeKeyVaultAndOptionsAuthentication(KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultAuthentication.get), authenticationParameters))

        (authenticationParameters, KDSU.mergeKeyVaultAndOptionsStorageParams(
          parameters.get(KustoSourceOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME),
          parameters.get(KustoSourceOptions.KUSTO_BLOB_CONTAINER),
          storageSecret,
          storageSecretIsAccountKey,
          keyVaultAuthentication.get))
      } else {
        // Params passed from options
        (authenticationParameters, KDSU.getAndValidateTransientStorageParametersIfExist(
          parameters.get(KustoSourceOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME),
          parameters.get(KustoSourceOptions.KUSTO_BLOB_CONTAINER),
          storageSecret,
          storageSecretIsAccountKey,
          parameters.get(KustoSourceOptions.KUSTO_BLOB_STORAGE_ENDPOINT_SUFFIX)))
      }

    val timeout = new FiniteDuration(parameters.getOrElse(KustoSourceOptions.KUSTO_TIMEOUT_LIMIT, KCONST.defaultWaitingIntervalLongRunning).toLong, TimeUnit.SECONDS)
    val readModeOption = parameters.get(KustoSourceOptions.KUSTO_READ_MODE)
    val readMode: Option[ReadMode]  = if (readModeOption.isDefined){
      Some(ReadMode.withName(readModeOption.get))
    } else {
      None
    }

    KDSU.logInfo(myName, "Finished serializing parameters for reading")

    val exportQueryOnlyOnce = java.lang.Boolean.parseBoolean(parameters.getOrElse(KustoSourceOptions.KUSTO_DISTRIBUTED_MODE_EXPORT_QUERY_ONLY_ONCE, "false"))

    KustoRelation(
      kustoCoordinates,
      kustoAuthentication.get,
      parameters.getOrElse(KustoSourceOptions.KUSTO_QUERY, ""),
      KustoReadOptions(readMode, shouldCompressOnExport, exportSplitLimitMb, exportQueryOnlyOnce),
      timeout,
      numPartitions,
      parameters.get(KustoDebugOptions.KUSTO_PARTITION_COLUMN),
      partitioningMode,
      parameters.get(KustoSourceOptions.KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES),
      storageParameters,
      clientRequestProperties
    )(sqlContext.sparkSession)
  }

  private def setNumPartitions(sqlContext: SQLContext, requestedNumPartitions: Option[String], partitioningMode: Option[String]): Int = {
    if (requestedNumPartitions.isDefined) requestedNumPartitions.get.toInt else {
      partitioningMode match {
        case Some("hash") => sqlContext.getConf("spark.sql.shuffle.partitions", "10").toInt
        // In "auto" mode we don't explicitly partition the data:
        // The data is exported and split to multiple files if required by Kusto 'export' command
        // The data is then read from the base directory for parquet files and partitioned by the parquet data source
        case _ => 1
      }
    }
  }

  override def shortName(): String = "kusto"
}
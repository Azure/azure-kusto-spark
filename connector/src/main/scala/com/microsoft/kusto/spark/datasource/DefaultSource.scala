package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.microsoft.kusto.spark.datasink.KustoWriter
import com.microsoft.kusto.spark.utils.{KeyVaultUtils, KustoQueryUtils, KustoDataSourceUtils => KDSU, KustoConstants => KCONST}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.concurrent.duration.FiniteDuration

class DefaultSource extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {
  var authenticationParameters: Option[KustoAuthentication] = None
  var kustoCoordinates: KustoCoordinates = _
  var keyVaultAuthentication: Option[KeyVaultAuthentication] = None

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val sinkParameters = KDSU.parseSinkParameters(parameters, mode)
    keyVaultAuthentication = sinkParameters.sourceParametersResults.keyVaultAuth
    kustoCoordinates = sinkParameters.sourceParametersResults.kustoCoordinates
    authenticationParameters = Some(sinkParameters.sourceParametersResults.authenticationParameters)

    if(keyVaultAuthentication.isDefined){
      val paramsFromKeyVault = KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultAuthentication.get)
      authenticationParameters = Some(KDSU.mergeKeyVaultAndOptionsAuthentication(paramsFromKeyVault, authenticationParameters))
    }

    KustoWriter.write(
      None,
      data,
      kustoCoordinates,
      authenticationParameters.get,
      sinkParameters.writeOptions)

    val limit = if (sinkParameters.writeOptions.writeResultLimit.equalsIgnoreCase(KustoOptions.NONE_RESULT_LIMIT)) None else {
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
      parameters + (KustoOptions.KUSTO_QUERY -> KustoQueryUtils.limitQuery(parameters(KustoOptions.KUSTO_TABLE), limit.get))
    } else {
      parameters
    }
  }

  private[kusto] def blobStorageAttributesProvided(parameters: Map[String, String]) = {
    parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_SAS_URL).isDefined || (parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME).isDefined &&
      parameters.get(KustoOptions.KUSTO_BLOB_CONTAINER).isDefined && parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY).isDefined)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val requestedPartitions = parameters.get(KustoOptions.KUSTO_NUM_PARTITIONS)
    val partitioningMode = parameters.get(KustoOptions.KUSTO_READ_PARTITION_MODE)
    val isCompressOnExport =  parameters.getOrElse(KustoDebugOptions.KUSTO_DBG_BLOB_COMPRESS_ON_EXPORT, "true").trim.toBoolean
    // Set default export split limit as 1GB, maximal allowed
    val exportSplitLimitMb = parameters.getOrElse(KustoDebugOptions.KUSTO_DBG_BLOB_FILE_SIZE_LIMIT_MB, "1024").trim.toInt

    val numPartitions = setNumPartitions(sqlContext, requestedPartitions, partitioningMode)
    var storageSecretIsAccountKey = true
    var storageSecret = parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY)

    if (storageSecret.isEmpty) {
      storageSecret = parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_SAS_URL)
      if (storageSecret.isDefined) storageSecretIsAccountKey = false
    }

    if(authenticationParameters.isEmpty){
      val sourceParameters = KDSU.parseSourceParameters(parameters)
      authenticationParameters = Some(sourceParameters.authenticationParameters)
      kustoCoordinates = sourceParameters.kustoCoordinates
      keyVaultAuthentication = sourceParameters.keyVaultAuth
    }

    val (kustoAuthentication, storageParameters): (Option[KustoAuthentication], Option[KustoStorageParameters]) =
      if (keyVaultAuthentication.isDefined) {
        // Get params from keyVault
        authenticationParameters = Some(KDSU.mergeKeyVaultAndOptionsAuthentication(KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultAuthentication.get), authenticationParameters))

        (authenticationParameters, KDSU.mergeKeyVaultAndOptionsStorageParams(
          parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME),
          parameters.get(KustoOptions.KUSTO_BLOB_CONTAINER),
          storageSecret,
          storageSecretIsAccountKey,
          keyVaultAuthentication.get))
      } else {
        // Params passed from options
        (authenticationParameters, KDSU.tryGetAndValidateTransientStorageParameters(
          parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME),
          parameters.get(KustoOptions.KUSTO_BLOB_CONTAINER),
          storageSecret,
          storageSecretIsAccountKey))
      }

    val timeout = new FiniteDuration(parameters.getOrElse(KustoOptions.KUSTO_TIMEOUT_LIMIT, KCONST.defaultTimeoutAsString).toLong, TimeUnit.SECONDS)

    KustoRelation(
      kustoCoordinates,
      kustoAuthentication.get,
      parameters.getOrElse(KustoOptions.KUSTO_QUERY, ""),
      KustoReadOptions(parameters.getOrElse(KustoDebugOptions.KUSTO_DBG_FORCE_READ_MODE, ""), isCompressOnExport, exportSplitLimitMb),
      timeout,
      numPartitions,
      parameters.get(KustoOptions.KUSTO_PARTITION_COLUMN),
      partitioningMode,
      parameters.get(KustoOptions.KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES),
      storageParameters
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
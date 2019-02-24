package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.Locale

import com.microsoft.kusto.spark.datasink.KustoWriter
import com.microsoft.kusto.spark.utils.{KeyVaultUtils, KustoDataSourceUtils, KustoQueryUtils}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {
  var authentication: KustoAuthentication = _
  var kustoCoordinates: KustoCoordinates = _
//  var writeAuthentication: KustoAuthentication = _
  var keyVaultAuthentication: KeyVaultAuthentication = _

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val parsedParams: (WriteOptions, KustoAuthentication, KustoCoordinates, KeyVaultAuthentication) = KustoDataSourceUtils.parseSinkParameters(parameters, mode)

    val writeOptions = parsedParams._1
    authentication = parsedParams._2
    kustoCoordinates = parsedParams._3
    keyVaultAuthentication = parsedParams._4
    if(keyVaultAuthentication != null){
      val paramsFromKeyVault = KeyVaultUtils.getAadAppParamsFromKeyVault(keyVaultAuthentication)
      combineKeyVaultAndOptionsAuthentication(paramsFromKeyVault)
    }

    KustoWriter.write(
      None,
      data,
      kustoCoordinates,
      authentication,
      writeOptions)

    val limit = if (writeOptions.writeResultLimit.equalsIgnoreCase(KustoOptions.NONE_RESULT_LIMIT)) None else {
      try {
        Some(writeOptions.writeResultLimit.toInt)
      }
      catch {
        case _: Exception => throw new InvalidParameterException(s"KustoOptions.KUSTO_WRITE_RESULT_LIMIT is set to '${writeOptions.writeResultLimit}'. Must be either 'none' or an integer value")
      }
    }

    createRelation(sqlContext, adjustParametersForBaseRelation(parameters, limit))
  }

  def adjustParametersForBaseRelation(parameters: Map[String, String], limit: Option[Int]): Map[String, String] = {
    val readMode = parameters.get(KustoOptions.KUSTO_READ_MODE)
    val limitIsSmall = limit.isDefined && limit.get <= 200
    var adjustedParams = parameters

    if (readMode.isEmpty && limitIsSmall) {
      adjustedParams = parameters + (KustoOptions.KUSTO_READ_MODE -> "lean") + (KustoOptions.KUSTO_NUM_PARTITIONS -> "1")
    }
    else if (parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_SAS_URL).isEmpty && (parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME).isEmpty ||
      parameters.get(KustoOptions.KUSTO_BLOB_CONTAINER).isEmpty ||
      parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY).isEmpty)
    ) {
      if (readMode.isDefined && !readMode.get.equalsIgnoreCase("lean")) {
        throw new InvalidParameterException(s"Read mode is set to '${readMode.get}', but transient storage parameters are not provided")
      }
      adjustedParams = parameters + (KustoOptions.KUSTO_READ_MODE -> "lean") + (KustoOptions.KUSTO_NUM_PARTITIONS -> "1")
    }

    if (limit.isDefined) {
      adjustedParams + (KustoOptions.KUSTO_QUERY -> KustoQueryUtils.limitQuery(parameters(KustoOptions.KUSTO_TABLE), limit.get))
    } else {
      adjustedParams
    }
  }


  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val requestedPartitions = parameters.get(KustoOptions.KUSTO_NUM_PARTITIONS)
    val readMode = parameters.getOrElse(KustoOptions.KUSTO_READ_MODE, "scale").toLowerCase(Locale.ROOT)
    val partitioningMode = parameters.get(KustoOptions.KUSTO_READ_PARTITION_MODE)
    val isLeanMode = readMode.equals("lean")

    val numPartitions = setNumPartitionsPerMode(sqlContext, requestedPartitions, isLeanMode, partitioningMode)
    if (!KustoOptions.supportedReadModes.contains(readMode)) {
      throw new InvalidParameterException(s"Kusto read mode must be one of ${KustoOptions.supportedReadModes.mkString(", ")}")
    }

    if (numPartitions != 1 && isLeanMode) {
      throw new InvalidParameterException(s"Reading in lean mode cannot be done on multiple partitions. Requested number of partitions: $numPartitions")
    }

    var storageSecretIsAccountKey = true
    var storageSecret = parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY)
    if (storageSecret.isEmpty) {
      storageSecret = parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_SAS_URL)
      if (storageSecret.isDefined) storageSecretIsAccountKey = false
    }

    if(authentication == null){
      val parsedParams: (KustoAuthentication, KustoCoordinates, KeyVaultAuthentication) = KustoDataSourceUtils.parseSourceParameters(parameters)
      authentication = parsedParams._1
      kustoCoordinates = parsedParams._2
      keyVaultAuthentication = parsedParams._3
    }

    val (kustoAuthentication, storageParameters): (KustoAuthentication, StorageParameters) = if (keyVaultAuthentication != null) {
      // Get params from keyVault
      if(isLeanMode){
        (KeyVaultUtils.getAadAppParamsFromKeyVault(keyVaultAuthentication), null)
      }
      combineKeyVaultAndOptionsAuthentication(KeyVaultUtils.getAadAppParamsFromKeyVault(keyVaultAuthentication))
      (authentication, combineKeyVaultAndOptionsStorageParams(
        parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME),
        parameters.get(KustoOptions.KUSTO_BLOB_CONTAINER),
        storageSecret,
        storageSecretIsAccountKey
        ,keyVaultAuthentication))
    } else {
      if(isLeanMode) {
        (authentication, null)
      } else {
        // Params passed from options
        (authentication, getTransientStorageParameters(parameters.get(KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME),
          parameters.get(KustoOptions.KUSTO_BLOB_CONTAINER),
          storageSecret,
          storageSecretIsAccountKey))
      }
    }

    KustoRelation(
      kustoCoordinates,
      kustoAuthentication,
      parameters.getOrElse(KustoOptions.KUSTO_QUERY, ""),
      isLeanMode,
      numPartitions,
      parameters.get(KustoOptions.KUSTO_PARTITION_COLUMN),
      partitioningMode,
      parameters.get(KustoOptions.KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES),
      storageParameters
    )(sqlContext.sparkSession)
  }

  private def setNumPartitionsPerMode(sqlContext: SQLContext, requestedNumPartitions: Option[String], isLeanMode: Boolean, partitioningMode: Option[String]): Int = {
    if (requestedNumPartitions.isDefined) requestedNumPartitions.get.toInt else {
      if (isLeanMode) 1 else {
        partitioningMode match {
          case Some("hash") => sqlContext.getConf("spark.sql.shuffle.partitions", "10").toInt
          // In "auto" mode we don't explicitly partition the data:
          // The data is exported and split to multiple files if required by Kusto 'export' command
          // The data is then read from the base directory for parquet files and partitioned by the parquet data source
          case _ => 1
        }
      }
    }
  }

  private def getTransientStorageParameters(storageAccount: Option[String],
                                            storageContainer: Option[String],
                                            storageAccountSecret: Option[String],
                                            storageSecretIsAccountKey: Boolean): StorageParameters = {
    if (storageAccount.isEmpty) {
      throw new InvalidParameterException("Storage account name is empty. Reading in 'Scale' mode requires a transient blob storage")
    }

    if (storageContainer.isEmpty) {
      throw new InvalidParameterException("Storage container name is empty.")
    }

    if (storageAccountSecret.isEmpty) {
      throw new InvalidParameterException("Storage account secret is empty. Please provide a storage account key or a SAS key")
    }

    StorageParameters(storageAccount.get, storageAccountSecret.get, storageContainer.get, storageSecretIsAccountKey)
  }

  override def shortName(): String = "kusto"

  private def combineKeyVaultAndOptionsAuthentication(paramsFromKeyVault: AadApplicationAuthentication): Unit = {
    if(authentication != null){
      // We have both keyVault and aad application params, take from options first and throw if both are empty
      try{
        val auth = authentication.asInstanceOf[AadApplicationAuthentication]
        authentication = AadApplicationAuthentication(
          if(auth.ID == "") {
            if(paramsFromKeyVault.ID == "")
              throw new InvalidParameterException("")
            paramsFromKeyVault.ID
          } else auth.ID,
          if(auth.password == "") {
            if (paramsFromKeyVault.password == "AADApplication key is empty. Please pass it in keyVault or options")
              throw new InvalidParameterException("")
            paramsFromKeyVault.password
          } else auth.password,
          if(auth.authority == "microsoft.com") paramsFromKeyVault.authority else auth.authority
        )}
      catch {
        case _: ClassCastException => throw new UnsupportedOperationException("keyVault authentication can be combined only with AADAplicationAuthentication")
      }
    } else {
      authentication = paramsFromKeyVault
    }
  }

  private def combineKeyVaultAndOptionsStorageParams(storageAccount: Option[String],
                                                     storageContainer: Option[String],
                                                     storageSecret: Option[String],
                                                     storageSecretIsAccountKey: Boolean,
                                                     keyVaultAuthentication: KeyVaultAuthentication): StorageParameters = {
    var keyVaultParameters = KeyVaultUtils.getStorageParamsFromKeyVault(keyVaultAuthentication)
    if(!storageSecretIsAccountKey){
      // If SAS option defined - take sas
      KustoDataSourceUtils.parseSas(storageSecret.get)
    } else {
      if (storageAccount.isEmpty || storageContainer.isEmpty || storageSecret.isEmpty){
        // If KeyVault contains sas take it
        if(keyVaultParameters.storageSecretIsAccountKey) {
          keyVaultParameters
        } else {
          // Try combine
          val combined = StorageParameters(if(storageAccount.isEmpty){
              keyVaultParameters.account
            } else storageAccount.get,
              if(storageSecret.isEmpty){
                keyVaultParameters.secret
              } else storageSecret.get,
              if(storageContainer.isEmpty){
                keyVaultParameters.container
              } else storageContainer.get, storageSecretIsAccountKey = true)
          if(combined.container == null){
            throw new InvalidParameterException("Storage container name is empty.")
          }
          if(combined.secret == null){
            throw new InvalidParameterException("Storage account secret is empty.")
          }
          if(combined.account == null){
            throw new InvalidParameterException("Storage account name is empty.")
          }
          combined
        }
      } else {
        StorageParameters(storageAccount.get, storageSecret.get, storageContainer.get, storageSecretIsAccountKey)
      }
    }
  }

}
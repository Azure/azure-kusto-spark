package com.microsoft.kusto.spark.datasource

import java.util.Locale

object KustoOptions {
  private val kustoOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    kustoOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val KEY_VAULT_URI = "keyVaultUri"
  val KEY_VAULT_CREDENTIALS = "keyVaultCredentials"
  val KEY_VAULT_PEM_FILE_PATH = "keyVaultPemFilePath"
  val KEY_VAULT_CERTIFICATE_KEY = "keyVaultPemFileKey"
  val KEY_VAULT_APP_ID = "keyVaultAppId"
  val KEY_VAULT_APP_KEY = "keyVaultAppKey"

  // AAD application identifier of the client
  val KUSTO_AAD_CLIENT_ID: String = newOption("kustoAADClientID")
  // AAD authentication authority
  val KUSTO_AAD_AUTHORITY_ID: String = newOption("kustoAADAuthorityID")
  // AAD application key for the client
  val KUSTO_AAD_CLIENT_PASSWORD: String = newOption("kustoClientAADClientPassword")
  // Target/source Kusto cluster for writing/reading the data.
  val KUSTO_CLUSTER: String = newOption("kustoCluster")
  val KUSTO_CONNECTION_TIMEOUT: String = newOption("kustoConnectionTimeout")
  val KUSTO_CREATE_TABLE_COLUMN_TYPES: String = newOption("createTableColumnTypes")
  val KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES: String = newOption("customSchema")

  // Target/source Kusto database for writing/reading the data. See KustoSink.md/KustoSource.md for
  // required permissions
  val KUSTO_DATABASE: String = newOption("kustoDatabase")
  val KUSTO_QUERY: String = newOption("kustoQuery")
  val KUSTO_QUERY_RETRY_TIMES: String = newOption("kustoQueryRetryTimes")
  // Target/source Kusto table for writing/reading the data. See KustoSink.md/KustoSource.md for
  // required permissions
  val KUSTO_TABLE: String = newOption("kustoTable")
  // If set to 'FailIfNotExist', the operation will fail if the table is not found
  // in the requested cluster and database.
  // If set to 'CreateIfNotExist' and the table is not found in the requested cluster and database,
  // it will be created, with a schema matching the DataFrame that is being written.
  // Default: 'FailIfNotExist'
  val KUSTO_TABLE_CREATE_OPTIONS: String = newOption("tableCreateOptions")
  val KUSTO_TRUNCATE: String = newOption("truncate")
  val KUSTO_USER_TOKEN = newOption("userToken")
  // When writing to Kusto, allows the driver to complete operation asynchronously.  See KustoSink.md for
  // details and limitations. Default: 'false'
  val KUSTO_WRITE_ENABLE_ASYNC: String = newOption("writeEnableAsync")
  // When writing to Kusto, limits the number of rows read back as BaseRelation. Default: '1'.
  // To read back all rows, set as 'none' (NONE_RESULT_LIMIT)
  val KUSTO_WRITE_RESULT_LIMIT: String = newOption("writeResultLimit")
  // Select either 'scale' or 'lean' read mode. Default: 'scale'
  val KUSTO_READ_MODE: String = newOption("readMode")

  // Partitioning parameters
  val KUSTO_READ_PARTITION_MODE: String = newOption("partitionMode")
  // Note: for 'read', this allows to partition export from Kusto to blob (default is '1')
  // It does not affect the number of partitions created when reading from blob.
  // Therefore it is not recommended to use this option when reading from Kusto, left here for experimentation
  val KUSTO_NUM_PARTITIONS: String = newOption("numPartitions")
  val KUSTO_PARTITION_COLUMN: String = newOption("partitionColumn")

  object SinkTableCreationMode extends Enumeration {
    type SinkTableCreationMode = Value
    val CreateIfNotExist, FailIfNotExist = Value
  }

  // Blob Storage access parameters for source connector when working in 'scale' mode (read)

  // Transient storage account when reading from Kusto
  val KUSTO_BLOB_STORAGE_ACCOUNT_NAME: String = newOption("blobStorageAccountName")
  // Storage account key. Use either this or SAS key to access the storage account
  val KUSTO_BLOB_STORAGE_ACCOUNT_KEY: String = newOption("blobStorageAccountKey")
  // SAS access key: a complete query string of the SAS as a container
  // Use either this or storage account key to access the storage account
  val KUSTO_BLOB_STORAGE_SAS_KEY: String = newOption("blobStorageSasKey")
  // Blob container name
  val KUSTO_BLOB_CONTAINER: String = newOption("blobContainer")

  val NONE_RESULT_LIMIT = "none"
  val supportedReadModes: Set[String] = Set("lean", "scale")
  // Partitioning modes allow to export data from Kusto to separate folders within the blob container per-partition
  // In current implementation this is not exploited by Kusto read connector, and is not recommended.
  // Left for future experimentation
  val supportedPartitioningModes: Set[String] = Set("hash")
}

abstract class KustoAuthentication
abstract class KeyVaultAuthentication(uri: String) extends KustoAuthentication

case class KustoCoordinates(cluster: String, database: String, table:String = "")
case class AadApplicationAuthentication(ID: String, password: String, authority: String) extends KustoAuthentication
case class KeyVaultAppAuthentication(uri: String, keyVaultAppID: String, keyVaultAppKey: String) extends KeyVaultAuthentication(uri)
case class KeyVaultCertificateAuthentication(uri: String, pemFilePath: String, pemFilePassword: String) extends KeyVaultAuthentication(uri)
case class WriteOptions(tableCreateOptions: KustoOptions.SinkTableCreationMode.SinkTableCreationMode = KustoOptions.SinkTableCreationMode.FailIfNotExist,
                        isAsync: Boolean = false, writeResultLimit: String, timeZone: String = "UTC")
case class KustoUserTokenAuthentication(token: String) extends KustoAuthentication

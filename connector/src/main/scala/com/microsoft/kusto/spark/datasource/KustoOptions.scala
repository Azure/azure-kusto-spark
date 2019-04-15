package com.microsoft.kusto.spark.datasource

import java.util.Locale

import com.microsoft.kusto.spark.datasource.KustoOptions.newOption

import scala.concurrent.duration.FiniteDuration


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
  val KUSTO_ACCESS_TOKEN: String = newOption("accessToken")
  // When writing to Kusto, allows the driver to complete operation asynchronously.  See KustoSink.md for
  // details and limitations. Default: 'false'
  val KUSTO_WRITE_ENABLE_ASYNC: String = newOption("writeEnableAsync")
  // When writing to Kusto, limits the number of rows read back as BaseRelation. Default: '1'.
  // To read back all rows, set as 'none' (NONE_RESULT_LIMIT)
  val KUSTO_WRITE_RESULT_LIMIT: String = newOption("writeResultLimit")
  // An integer number corresponding to the period in seconds after which the operation will timeout. Default: '5400' (90 minutes)
  val KUSTO_TIMEOUT_LIMIT: String = newOption("timeoutLimit")

  // Partitioning parameters
  val KUSTO_READ_PARTITION_MODE: String = newOption("partitionMode")
  // Note: for 'read', this allows to partition export from Kusto to blob (default is '1')
  // It does not affect the number of partitions created when reading from blob.
  // Therefore it is not recommended to use this option when reading from Kusto
  // CURRENTLY NOT USED, left here for experimentation
  val KUSTO_NUM_PARTITIONS: String = newOption("numPartitions")
  // CURRENTLY NOT USED, left here for experimentation
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
  val KUSTO_BLOB_STORAGE_SAS_URL: String = newOption("blobStorageSasUrl")
  // Blob container name
  val KUSTO_BLOB_CONTAINER: String = newOption("blobContainer")
  // When reading in 'scale' mode, sets Spark configuration to read from Azure blob.
  // The following configuration parameters are set:
  // 1. Blob access secret:
  //    a. If storage account key is provided, the following parameter is set:
  //       fs.azure.account.key.<storage-account-name>.blob.core.windows.net, <storage-account-key>
  //    b. If SAS key is provided, the following parameter is set:
  //       fs.azure.sas.<blob-container-name>.<storage-account-name>.blob.core.windows.net, <sas-key>
  // 2. File system specifier property is set as follows:
  //     "fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem"
  // If set to 'false' (default), the user must set up these values prior to using read connector in "scale" mode.
  // If set to 'true', the connector will update these parameters on every 'read' operation
  // Default: 'false'
  val KUSTO_BLOB_SET_FS_CONFIG: String = newOption("blobSetFsConfig")

  val NONE_RESULT_LIMIT = "none"
  // Partitioning modes allow to export data from Kusto to separate folders within the blob container per-partition
  // Note! In current implementation this is not exploited by Kusto read connector, and is not recommended.
  // Left for future experimentation
  val supportedPartitioningModes: Set[String] = Set("hash")
}

abstract class KustoAuthentication
abstract class KeyVaultAuthentication(uri: String) extends KustoAuthentication

case class KustoCoordinates(cluster: String, database: String, table: Option[String] = None)
case class AadApplicationAuthentication(ID: String, password: String, authority: String) extends KustoAuthentication
case class KeyVaultAppAuthentication(uri: String, keyVaultAppID: String, keyVaultAppKey: String) extends KeyVaultAuthentication(uri)
case class KeyVaultCertificateAuthentication(uri: String, pemFilePath: String, pemFilePassword: String) extends KeyVaultAuthentication(uri)
case class WriteOptions(tableCreateOptions: KustoOptions.SinkTableCreationMode.SinkTableCreationMode = KustoOptions.SinkTableCreationMode.FailIfNotExist,
                        isAsync: Boolean = false, writeResultLimit: String = KustoOptions.NONE_RESULT_LIMIT, timeZone: String = "UTC", timeout: FiniteDuration)
case class KustoAccessTokenAuthentication(token: String) extends KustoAuthentication

/************************************************************************************/
/*                                    NOTE!!!                                       */
/* These options are intended for testing, experimentation and debug.               */
/* They may not be used in a production environment                                 */
/* Interface stability is not guaranteed: options may be removed or changed freely  */
/************************************************************************************/
object KustoDebugOptions {
  private val kustoOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    kustoOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }
  // Reading method is determined internally by the connector
  // This option allows to override connector heuristics and force a specific mode.
  // Recommended to use only for debug and testing purposes
  // Supported values: Empty string (""), 'lean' (direct query), 'scale' (via blob). Default: empty
  val KUSTO_DBG_FORCE_READ_MODE: String = newOption("readMode")
  // When reading via blob storage, compresses the data upon export from Kusto to Blob
  // This feature is experimental, in order to measure performance impact w/wo compression
  // Default: 'true'
  val KUSTO_DBG_BLOB_COMPRESS_ON_EXPORT: String = newOption("dbgBlobCompressOnExport")
  // The size limit in MB (uncompressed) after which the export to blob command will create another file (split)
  // Setting negative or zero value results in applying export command default
  val KUSTO_DBG_BLOB_FILE_SIZE_LIMIT_MB: String = newOption("dbgBlobFileSizeLimitMb")
}


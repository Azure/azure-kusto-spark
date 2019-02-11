package com.microsoft.kusto.spark.datasource

import java.util.Locale

import org.apache.spark.sql.SaveMode


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
  val KUSTO_LOWER_BOUND: String = newOption("lowerBound")
  val KUSTO_NUM_PARTITIONS: String = newOption("numPartitions")
  val KUSTO_PARTITION_COLUMN: String = newOption("partitionColumn")
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
  val KUSTO_UPPER_BOUND: String = newOption("upperBound")
  // When writing to Kusto, allows the driver to complete operation asynchronously.  See KustoSink.md for
  // details and limitations. Default: 'false'
  val KUSTO_WRITE_ENABLE_ASYNC: String = newOption("writeEnableAsync")
  // When writing to Kusto, limits the number of rows read back as BaseRelation. Default: '1'.
  // To read back all rows, set as 'none' (NONE_RESULT_LIMIT)
  val KUSTO_WRITE_RESULT_LIMIT: String = newOption("writeResultLimit")
  val KUSTO_READ_MODE: String = newOption("readMode")

  object SinkTableCreationMode extends Enumeration {
    type SinkTableCreationMode = Value
    val CreateIfNotExist, FailIfNotExist = Value
  }

  val NONE_RESULT_LIMIT = "none"
  val supportedReadModes: Set[String] = Set("lean", "scale")
}

abstract class KustoAuthentication
abstract class KeyVaultAuthentication(uri: String) extends KustoAuthentication

case class KustoTableCoordinates(cluster: String, database: String, table:String)
case class AadApplicationAuthentication(ID: String, password: String, authority: String) extends KustoAuthentication
case class KeyVaultAppAuthentiaction(uri: String, keyVaultAppID:String, keyVaultAppKey: String) extends KeyVaultAuthentication(uri)
case class KeyVaultCertificateAuthentication(uri: String, pemFilePath: String, pemFilePassword: String) extends KeyVaultAuthentication(uri)
case class KustoSparkWriteOptions(tableCreateOptions: KustoOptions.SinkTableCreationMode.SinkTableCreationMode = KustoOptions.SinkTableCreationMode.FailIfNotExist,
                                  isAsync: Boolean = false, writeResultLimit: String, timeZone: String = "UTC", mode: SaveMode = SaveMode.Append)

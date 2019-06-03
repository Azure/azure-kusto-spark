package com.microsoft.kusto.spark.datasource

object KustoSourceOptions extends KustoOptions {
  val KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES: String = newOption("customSchema")

  val KUSTO_QUERY: String = newOption("kustoQuery")
  val KUSTO_QUERY_RETRY_TIMES: String = newOption("kustoQueryRetryTimes")

  // A json representation of the ClientRequestProperties object used for reading from Kusto
  var KUSTO_CLIENT_REQUEST_PROPERTIES_JSON: String = newOption("clientRequestPropertiesJson")

  // Blob Storage access parameters for source connector when working in 'scale' mode (read)
  // These parameters will not be needed once we move to automatic blob provisioning

  // Transient storage account when reading from Kusto
  val KUSTO_BLOB_STORAGE_ACCOUNT_NAME: String = newOption("blobStorageAccountName")
  // Storage account key. Use either this or SAS key to access the storage account
  val KUSTO_BLOB_STORAGE_ACCOUNT_KEY: String = newOption("blobStorageAccountKey")
  // SAS access key: a complete query string of the SAS as a container
  // Use either this or storage account key to access the storage account
  val KUSTO_BLOB_STORAGE_SAS_URL: String = newOption("blobStorageSasUrl")
  // Blob container name
  val KUSTO_BLOB_CONTAINER: String = newOption("blobContainer")
}

package com.microsoft.kusto.spark.common

import java.util.Locale

trait KustoOptions {
  private val kustoOptionNames = collection.mutable.Set[String]()

  protected def newOption(name: String): String = {
    kustoOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  // KeyVault options. Relevant only if credentials need to be retrieved from Key Vault
  val KEY_VAULT_URI = "keyVaultUri"
  val KEY_VAULT_APP_ID = "keyVaultAppId"
  val KEY_VAULT_APP_KEY = "keyVaultAppKey"


  // AAD application identifier of the client
  val KUSTO_AAD_APP_ID: String = newOption("kustoAadAppId")
  // AAD authentication authority
  val KUSTO_AAD_AUTHORITY_ID: String = newOption("kustoAadAuthorityID")
  // AAD application key for the client
  val KUSTO_AAD_APP_SECRET: String = newOption("kustoAadAppSecret")

  val KUSTO_ACCESS_TOKEN: String = newOption("accessToken")

  // Use only for local runs where one can open the browser
  val KUSTO_USER_PROMPT: String = newOption("userPrompt")
  // AAD application pfx certificate path
  val KUSTO_AAD_APP_CERTIFICATE_PATH: String = newOption("kutoAadAppCertPath")
  // AAD application certificate password
  val KUSTO_AAD_APP_CERTIFICATE_PASSWORD: String = newOption("kutoAadAppCertPassword")

  // Classpath to a class that its constructor accepts one argument of type CaseInsensitiveMap[String] which will contain
  // the options provided to the connector. The class should extend Callable[String] with Serializeable and is expected
  // to return an AAD token upon invoking the call method.
  // The provider will be called for every request to the kusto service
  val KUSTO_TOKEN_PROVIDER_CALLBACK_CLASSPATH: String = newOption("tokenProviderCallbackClasspath")

  // Target/source Kusto cluster for writing/reading the data.
  val KUSTO_CLUSTER: String = newOption("kustoCluster")
  // Target/source Kusto database for writing/reading the data. See KustoSink.md/KustoSource.md for
  // required permissions
  val KUSTO_DATABASE: String = newOption("kustoDatabase")

  // An integer number corresponding to the period in seconds after which the operation will timeout.
  // For write operations this limit starts ticking only after the data was processed by the connector and it starts
  // polling on the ingestion results. Default 2 days
  val KUSTO_TIMEOUT_LIMIT: String = newOption("timeoutLimit")


  // A json representation of the ClientRequestProperties object used for reading from Kusto
  var KUSTO_CLIENT_REQUEST_PROPERTIES_JSON: String = newOption("clientRequestPropertiesJson")

  // An id of the source used for tracing of the write operation. Will override the clientRequestPropertiesJson
  // request id if provided
  val KUSTO_REQUEST_ID: String = newOption("requestId")
}

case class KustoCoordinates(clusterUrl: String, clusterAlias:String, database: String, table: Option[String] = None)

/** ******************************************************************************* */
/*                                    NOTE!!!                                       */
/* These options are intended for testing, experimentation and debug.               */
/* They may not be used in a production environment                                 */
/* Interface stability is not guaranteed: options may be removed or changed freely  */
/** ******************************************************************************* */
private[kusto] object KustoDebugOptions {
  private val kustoOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    kustoOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  // When reading via blob storage, compresses the data upon export from Kusto to Blob
  // This feature is experimental, in order to measure performance impact w/wo compression
  // Default: 'true'
  val KUSTO_DBG_BLOB_COMPRESS_ON_EXPORT: String = newOption("dbgBlobCompressOnExport")
  // The size limit in MB (uncompressed) after which the export to blob command will create another file (split)
  // Setting negative or zero value results in applying export command default
  val KUSTO_DBG_BLOB_FILE_SIZE_LIMIT_MB: String = newOption("dbgBlobFileSizeLimitMb")

  // Partitioning parameters, CURRENTLY NOT USED
  // CURRENTLY NOT USED
  val KUSTO_READ_PARTITION_MODE: String = newOption("partitionMode")
  // Note: for 'read', this allows to partition export from Kusto to blob (default is '1')
  // It does not affect the number of partitions created when reading from blob.
  // Therefore it is not recommended to use this option when reading from Kusto
  // CURRENTLY NOT USED, left here for experimentation
  val KUSTO_NUM_PARTITIONS: String = newOption("numPartitions")
  // CURRENTLY NOT USED, left here for experimentation
  val KUSTO_PARTITION_COLUMN: String = newOption("partitionColumn")

  // Partitioning modes allow to export data from Kusto to separate folders within the blob container per-partition
  // Note! In current implementation this is not exploited by Kusto read connector
  // Left for future experimentation
  val supportedPartitioningModes: Set[String] = Set("hash")

  val KEY_VAULT_PEM_FILE_PATH = "keyVaultPemFilePath" // Not yet supported
  val KEY_VAULT_CERTIFICATE_KEY = "keyVaultPemFileKey" // Not yet supported

  val KUSTO_MAXIMAL_EXTENTS_COUNT_FOR_SPLIT_MERGE_PER_NODE
    :String =  newOption("maximalExtentsCountForSplitMergePerNode")
  val KUSTO_MAX_RETRIES_ON_MOVR_EXTENTS: String = newOption("maxRetriesOnMoveExtents")
}

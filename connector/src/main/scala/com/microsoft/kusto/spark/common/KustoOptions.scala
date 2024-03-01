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

package com.microsoft.kusto.spark.common

import java.util.Locale

trait KustoOptions {
  // TODO validate for each option given by user that it exists in the set
  private val kustoOptionNames = collection.mutable.Set[String]()
  protected def newOption(name: String): String = {
    kustoOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  /** Required options */
  // Target/source Kusto cluster for writing/reading the data.
  val KUSTO_CLUSTER: String = newOption("kustoCluster")

  // Target/source Kusto database for writing/reading the data. See KustoSink.md/KustoSource.md for
  // required permissions
  val KUSTO_DATABASE: String = newOption("kustoDatabase")

  /**
   * Authentication parameters - If not provided - device code would show on console. If console
   * is not available use DeviceAuthentication
   * class-https://github.com/Azure/azure-kusto-spark/blob/56667c54dfd43455113a9b48725b236a8d92ccd4/samples/src/main/python/pyKusto.py#L147
   */
  // AAD application identifier of the client
  val KUSTO_AAD_APP_ID: String = newOption("kustoAadAppId")
  // AAD authentication authority
  val KUSTO_AAD_AUTHORITY_ID: String = newOption("kustoAadAuthorityID")
  // AAD application key for the client
  val KUSTO_AAD_APP_SECRET: String = newOption("kustoAadAppSecret")

  val KUSTO_ACCESS_TOKEN: String = newOption("accessToken")

  // Use only for local runs where one can open the browser - set to true to enable
  val KUSTO_USER_PROMPT: String = newOption("userPrompt")
  // AAD application pfx certificate path
  val KUSTO_AAD_APP_CERTIFICATE_PATH: String = newOption("kutoAadAppCertPath")
  // AAD application certificate password
  val KUSTO_AAD_APP_CERTIFICATE_PASSWORD: String = newOption("kutoAadAppCertPassword")

  // KeyVault options. Relevant only if credentials need to be retrieved from Key Vault
  val KEY_VAULT_URI = "keyVaultUri"
  val KEY_VAULT_APP_ID = "keyVaultAppId"
  val KEY_VAULT_APP_KEY = "keyVaultAppKey"

  // Classpath to a class that its constructor accepts one argument of type CaseInsensitiveMap[String] which will contain
  // the options provided to the connector. The class should extend Callable[String] with Serializeable and is expected
  // to return an AAD token upon invoking the call method.
  // The provider will be called for every request to the kusto service
  val KUSTO_TOKEN_PROVIDER_CALLBACK_CLASSPATH: String = newOption(
    "tokenProviderCallbackClasspath")

  val KUSTO_MANAGED_IDENTITY_AUTH: String = newOption("managedIdentityAuth")
  val KUSTO_MANAGED_IDENTITY_CLIENT_ID: String = newOption("managedIdentityClientId")

  /** Optional parameters */
  // it merge origin/aKusto ingestion cluster URL for reading data - provide this if ingestion URL cannot be deduced
  // from adding
  // "ingest-" prefix to the KUSTO_CLUSTER provided. i.e when using proxies.
  val KUSTO_INGESTION_URI: String = newOption("kustoIngestionUri")

  // An integer number corresponding to the period in seconds after which the operation will timeout.
  // For write operations this limit starts ticking only after the data was processed by the connector and it starts
  // polling on the ingestion results. Default 2 days
  val KUSTO_TIMEOUT_LIMIT: String = newOption("timeoutLimit")

  // A json representation of the ClientRequestProperties object used for reading from Kusto
  var KUSTO_CLIENT_REQUEST_PROPERTIES_JSON: String = newOption("clientRequestPropertiesJson")

  // An id of the source used for tracing of the write operation. Will override the clientRequestPropertiesJson
  // request id if provided. Should be Unique per run.
  val KUSTO_REQUEST_ID: String = newOption("requestId")
}

case class KustoCoordinates(
    clusterUrl: String,
    clusterAlias: String,
    database: String,
    table: Option[String] = None,
    ingestionUrl: Option[String] = None)

/**
 * *******************************************************************************
 */
/*                                    NOTE!!!                                       */
/* These options are intended for testing, experimentation and debug.               */
/* They may not be used in a production environment                                 */
/* Interface stability is not guaranteed: options may be removed or changed freely  */
/**
 * *******************************************************************************
 */
private[kusto] object KustoDebugOptions {
  private val kustoOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    kustoOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  // When reading via blob storage, compresses the data upon export from Kusto to Blob
  // This feature is experimental, to measure performance impact w/wo compression
  // Default: 'true'
  val KUSTO_DBG_BLOB_COMPRESS_ON_EXPORT: String = newOption("dbgBlobCompressOnExport")

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

  val KUSTO_MAXIMAL_EXTENTS_COUNT_FOR_SPLIT_MERGE_PER_NODE: String = newOption(
    "maximalExtentsCountForSplitMergePerNode")
  val KUSTO_MAX_RETRIES_ON_MOVE_EXTENTS: String = newOption("maxRetriesOnMoveExtents")

  val KUSTO_DISABLE_FLUSH_IMMEDIATELY: String = newOption("disableFlushImmediately")

  // Needed only if your task produce big blobs in high volume
  val KUSTO_ENSURE_NO_DUPLICATED_BLOBS: String = newOption("ensureNoDuplicatedBlobs")

  val KUSTO_DISABLE_QUEUE_REQUEST_OPTIONS_OVERRIDE: String = newOption(
    "disableQueueRequestOptionsOverride")
}

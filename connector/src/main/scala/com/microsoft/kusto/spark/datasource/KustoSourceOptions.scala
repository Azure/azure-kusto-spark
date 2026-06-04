// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import com.microsoft.kusto.spark.common.KustoOptions

object KustoSourceOptions extends KustoOptions {

  /** Required options */
  val KUSTO_QUERY: String = newOption("kustoQuery")

  /** Optional options */
  val KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES: String = newOption("customSchema")

  // Blob Storage / OneLake access parameters for source connector when working in 'distributed' mode (read)
  // These parameters are not required as the service supplies it by default.
  // A JSON of the form { "storageCredentials": [ ... ], "endpointSuffix": "<suffix>" } where each entry is either:
  //   - blob SAS:        { "sasUrl": "https://<account>.blob.<suffix>/<container>?<SAS>" }
  //   - account+key:     { "storageAccountName": "...", "storageAccountKey": "...", "blobContainer": "..." }
  //   - blob impersonate:{ "sasUrl": "https://<account>.blob.<suffix>/<container>;impersonate" }
  //   - OneLake (Fabric Lakehouse), with caller AAD impersonation (no SAS required):
  //       { "sasUrl": "https://onelake.dfs.fabric.microsoft.com/<workspace>/<lakehouse>.Lakehouse/Files/<dir>" }
  //       or { "sasUrl": "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Files/<dir>" }
  //     For OneLake the connector forces storageProtocol=abfss and uses ambient AAD configured by Spark
  //     (Fabric notebooks). The caller must have at least Workspace Contributor / item-level Write on the
  //     target Lakehouse Files path. Useful with DEP / OAP environments where Kusto-managed export blobs
  //     are unreachable from executors.
  val KUSTO_TRANSIENT_STORAGE: String = newOption("transientStorage")

  // Blob domain endpoint suffix - default: core.windows.net - needed for non-public clouds
  val KUSTO_BLOB_STORAGE_ENDPOINT_SUFFIX: String = newOption("blobStorageEndpointSuffix")
  // By default an estimation of the rows count is first being made, if the count is lower than 5000 records a simple
  // query is made, else - if storage params were provided they are used for 'distributed' reading and if not - the connector
  // tries to use storage from the kusto ingest service.
  // This option allows to override these connector heuristics.
  // By default if the single mode was chosen and failed - there is a fallback to 'distributed' mode
  // See https://docs.microsoft.com/azure/kusto/concepts/querylimits#limit-on-result-set-size-result-truncation
  // for hard limit on query size using single mode
  val KUSTO_READ_MODE: String = newOption("readMode")
  // set to 'true' to export request Query only once and cache the exported path to for reuse
  val KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE: String = newOption(
    "distributedReadModeTransientCache")
  // if 'true', query executed on Kusto cluster will include the selected columns and filters. Set to 'false' to
  // execute request query on kusto cluster as is, columns and filters will be applied by spark on the data read from cluster.
  // Defaults to 'true' if KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE=false
  // Defaults to 'false' if KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE=true
  val KUSTO_QUERY_FILTER_PUSH_DOWN: String = newOption("queryFilterPushDown")
  // When a large dataset has to be exported with Kusto as a source (or) when forcing a distributed mode read (or) when
  // query limits are hit the connector uses the export option to export data (.export data).With newer options being
  // rolled-out for export, this additional parameter can be used as options for the export.
  // Setting useNativeParquetWriter=true will fail for Spark versions < 3.3.0
  val KUSTO_EXPORT_OPTIONS_JSON: String = newOption("kustoExportOptionsJson")

  val STORAGE_PROTOCOL: String = newOption("storageProtocol")
}

object ReadMode extends Enumeration {
  type ReadMode = Value
  val ForceSingleMode, ForceDistributedMode = Value
}

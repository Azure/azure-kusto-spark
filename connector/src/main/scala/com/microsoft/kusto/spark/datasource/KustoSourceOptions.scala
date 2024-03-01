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

package com.microsoft.kusto.spark.datasource

import com.microsoft.kusto.spark.common.KustoOptions

object KustoSourceOptions extends KustoOptions {

  /** Required options */
  val KUSTO_QUERY: String = newOption("kustoQuery")

  /** Optional options */
  val KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES: String = newOption("customSchema")

  // Blob Storage access parameters for source connector when working in 'distributed' mode (read)
  // These parameters are not be required as the service supply it by default
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
}

object ReadMode extends Enumeration {
  type ReadMode = Value
  val ForceSingleMode, ForceDistributedMode = Value
}

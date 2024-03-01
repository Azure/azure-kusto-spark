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

package com.microsoft.kusto.spark.datasink

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.microsoft.kusto.spark.common.KustoOptions
import com.microsoft.kusto.spark.utils.KustoConstants

import scala.concurrent.duration.FiniteDuration

object KustoSinkOptions extends KustoOptions {

  /** Required options */
  val KUSTO_TABLE: String = newOption("kustoTable")

  /** Optional options */
  // IMPORTANT: If set to true -> polling will not block on worker node and will be executed on a driver pool thread
  // 'true' is recommended for production.
  val KUSTO_POLLING_ON_DRIVER: String = newOption("pollingOnDriver")

  // If set to 'FailIfNotExist', the operation will fail if the table is not found
  // in the requested cluster and database.
  // If set to 'CreateIfNotExist' and the table is not found in the requested cluster and database,
  // it will be created, with a schema matching the DataFrame that is being written.
  // Default: 'FailIfNotExist'
  val KUSTO_TABLE_CREATE_OPTIONS: String = newOption("tableCreateOptions")
  // When writing to Kusto, allows the driver to complete operation asynchronously.  See KustoSink.md for
  // details and limitations. Default: 'false'
  val KUSTO_WRITE_ENABLE_ASYNC: String = newOption("writeEnableAsync")
  // When writing to Kusto, limits the number of rows read back as BaseRelation. Default: '1'.
  // To read back all rows, set as 'none' (NONE_RESULT_LIMIT)
  val KUSTO_WRITE_RESULT_LIMIT: String = newOption("writeResultLimit")

  // A json representation of the SparkIngestionProperties object used for writing to Kusto
  val KUSTO_SPARK_INGESTION_PROPERTIES_JSON: String = newOption("sparkIngestionPropertiesJson")

  val NONE_RESULT_LIMIT = "none"

  // A limit indicating the size in MB of the aggregated data before ingested to Kusto. Note that this is done for each
  // partition. Kusto's ingestion also aggregates data, default suggested by Kusto is 1GB but here we suggest to cut
  // it at 100MB to adjust it to spark pulling of data.
  val KUSTO_CLIENT_BATCHING_LIMIT: String = newOption("clientBatchingLimit")

  // If set 'GenerateDynamicCsvMapping', dynamically generates csv mapping based on DataFrame and target Kusto table column names when writing to Kusto.
  // If some Kusto table fields are missing in the DataFrame, they will be ingested as empty. If some DataFrame fields are missing in target table, fails.
  // If SparkIngestionProperties.csvMappingNameReference exists, fails.
  // If set 'FailIfNotMatch' adjust schemas equality (column name and order) for DataFrame and target Kusto table, fails if schemas don't match.
  // If set 'NoAdjustment' do nothing.
  // Default: 'NoAdjustment'
  val KUSTO_ADJUST_SCHEMA: String = newOption("adjustSchema")

  // An integer number corresponding to the period in seconds after which the staging resources used for the writing
  // are cleaned if they weren't cleaned at the end of the run
  val KUSTO_STAGING_RESOURCE_AUTO_CLEANUP_TIMEOUT: String = newOption(
    "stagingResourcesAutoCleanupTimeout")

  // If set to 'Transactional' - guarantees write operation to either completely succeed or fail together
  // but includes additional steps - it creates a temporary table and after processing the data it polls on ingestion result
  // after which it will move the data to the destination table (the last part is a metadata operation only)
  // If set to 'Queued', the write operation finishes after data is processed by the workers, the data may not be completely
  // available up until the service finishes loading it and failures on the service side will not propagate to Spark.
  val KUSTO_WRITE_MODE: String = newOption("writeMode")

  // Provide a temporary table name that will be used for this write operation to achieve transactional write and move
  // data to destination table on success. Table is expected to exist and unique per run (as we delete the table
  // at the end of the process and therefore should be per write operation). In case of success, the table will be
  // deleted; in case of failure, it's up to the user to delete. It is most recommended to alter the table auto-delete
  // policy so as to not get stuck with 'ghost' tables -
  // https://docs.microsoft.com/azure/data-explorer/kusto/management/auto-delete-policy
  // Use this option if you want to persist partial write results (as the failure could be of a single partition)
  val KUSTO_TEMP_TABLE_NAME: String = newOption("tempTableName")
}

object SinkTableCreationMode extends Enumeration {
  type SinkTableCreationMode = Value
  val CreateIfNotExist, FailIfNotExist = Value
}

object SchemaAdjustmentMode extends Enumeration {
  type SchemaAdjustmentMode = Value
  val NoAdjustment, FailIfNotMatch, GenerateDynamicCsvMapping = Value
}

object WriteMode extends Enumeration {
  type WriteMode = Value
  val Transactional, Queued = Value
}

case class WriteOptions(
    pollingOnDriver: Boolean = false,
    tableCreateOptions: SinkTableCreationMode.SinkTableCreationMode =
      SinkTableCreationMode.FailIfNotExist,
    isAsync: Boolean = false,
    writeResultLimit: String = KustoSinkOptions.NONE_RESULT_LIMIT,
    timeZone: String = "UTC",
    timeout: FiniteDuration = new FiniteDuration(
      KustoConstants.DefaultWaitingIntervalLongRunning.toInt,
      TimeUnit.SECONDS),
    ingestionProperties: Option[String] = None,
    batchLimit: Int = KustoConstants.DefaultBatchingLimit,
    requestId: String = UUID.randomUUID().toString,
    autoCleanupTime: FiniteDuration =
      new FiniteDuration(KustoConstants.DefaultCleaningInterval.toInt, TimeUnit.SECONDS),
    maxRetriesOnMoveExtents: Int = 10,
    minimalExtentsCountForSplitMerge: Int = 400,
    adjustSchema: SchemaAdjustmentMode.SchemaAdjustmentMode = SchemaAdjustmentMode.NoAdjustment,
    isTransactionalMode: Boolean = true,
    userTempTableName: Option[String] = None,
    disableFlushImmediately: Boolean = false,
    ensureNoDupBlobs: Boolean = false)

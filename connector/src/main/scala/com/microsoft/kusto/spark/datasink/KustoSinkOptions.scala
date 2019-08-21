package com.microsoft.kusto.spark.datasink

import com.microsoft.kusto.spark.common.KustoOptions

import scala.concurrent.duration.FiniteDuration

object KustoSinkOptions extends KustoOptions{
  val KUSTO_TABLE: String = newOption("kustoTable")

  val KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES: String = newOption("customSchema")

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

  val KUSTO_OMIT_NULLS: String = newOption("omitNulls")

  val NONE_RESULT_LIMIT = "none"
}

object SinkTableCreationMode extends Enumeration {
  type SinkTableCreationMode = Value
  val CreateIfNotExist, FailIfNotExist = Value
}

case class WriteOptions(tableCreateOptions: SinkTableCreationMode.SinkTableCreationMode = SinkTableCreationMode.FailIfNotExist,
                        isAsync: Boolean = false,
                        writeResultLimit: String = KustoSinkOptions.NONE_RESULT_LIMIT,
                        timeZone: String = "UTC", timeout: FiniteDuration,
                        IngestionProperties: Option[String] = None,
                        omitNulls: Boolean = true)

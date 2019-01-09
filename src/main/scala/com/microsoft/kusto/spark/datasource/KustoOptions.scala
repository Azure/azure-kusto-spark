package com.microsoft.kusto.spark.datasource

import java.util.Locale

object KustoOptions {
  private val kustoOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    kustoOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val KUSTO_AAD_CLIENT_ID: String = newOption("kustoAADClientID")
  val KUSTO_AAD_AUTHORITY_ID: String = newOption("kustoAADAuthorityID")
  val KUSTO_AAD_CLIENT_PASSWORD: String = newOption("kustoClientAADClientPassword")
  val KUSTO_CLUSTER: String = newOption("kustoCluster")
  val KUSTO_CONNECTION_TIMEOUT: String = newOption("kustoConnectionTimeout")
  val KUSTO_CREATE_TABLE_COLUMN_TYPES: String = newOption("createTableColumnTypes")
  val KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES: String = newOption("customSchema")
  val KUSTO_DATABASE: String = newOption("kustoDatabase")
  val KUSTO_LOWER_BOUND: String = newOption("lowerBound")
  val KUSTO_NUM_PARTITIONS: String = newOption("numPartitions")
  val KUSTO_PARTITION_COLUMN: String = newOption("partitionColumn")
  val KUSTO_QUERY: String = newOption("kustoQuery")
  val KUSTO_QUERY_RETRY_TIMES: String = newOption("kustoQueryRetryTimes")
  val KUSTO_SESSION_INIT_STATEMENT: String = newOption("sessionInitStatement")
  val KUSTO_TABLE: String = newOption("kustoTable")
  val KUSTO_TABLE_CREATE_OPTIONS: String = newOption("tableCreateOptions")
  val KUSTO_TXN_ISOLATION_LEVEL: String = newOption("isolationLevel")
  val KUSTO_TRUNCATE: String = newOption("truncate")
  val KUSTO_UPPER_BOUND: String = newOption("upperBound")
  val KUSTO_WRITE_ENABLE_ASYNC: String = newOption("writeEnableAsync")

  object SinkTableCreationMode extends Enumeration {
    type SinkTableCreationMode = Value
    val CreateIfNotExist, FailIfNotExist = Value
  }
}

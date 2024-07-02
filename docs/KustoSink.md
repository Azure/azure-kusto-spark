# Kusto Sink Connector

Kusto Sink Connector allows writing data from a Spark DataFrame to a table
in the specified Kusto cluster and database.

## Authentication

The connector uses **Azure Active Directory (AAD)** to authenticate the client application
that is using it. Please verify the following first:
* Client application is registered in AAD
* Client application has 'user' privileges or above on the target database
* When writing to an existing table, client application has 'admin' privileges on the target table

For details on Kusto principal roles, please refer to [Role-based Authorization](https://docs.microsoft.com/azure/kusto/management/access-control/role-based-authorization)
section in [Kusto Documentation](https://docs.microsoft.com/azure/kusto/).

For managing security roles, please refer to [Security Roles Management](https://docs.microsoft.com/azure/kusto/management/security-roles)
section in [Kusto Documentation](https://docs.microsoft.com/azure/kusto/).

## Batch Sink: 'write' command

Kusto connector implements Spark 'Datasource V1' API.
Kusto data source identifier is "com.microsoft.kusto.spark.datasource".
Dataframe schema is translated into kusto schema as explained in [DataTypes](Spark-Kusto%20DataTypes%20mapping.md).

### Command Syntax
 ```scala
 <dataframe-object>
  .write
  .format("com.microsoft.kusto.spark.datasource")
  .option(KustoSinkOptions.<option-name-1>, <option-value-1>
    ...
    .option(KustoSinkOptions.<option-name-n>, <option-value-n>
      .mode(SaveMode.Append)
      .save()
 ```
### Logging
Main logs are found on driver log4j logs. Workers data serialization logs and ingest queuing is found on workers stderr.
To set driver logs verbosity use com.microsoft.kusto.spark.utils.KustoDataSourceUtils.setLoggingLevel("debug")
* Note - open debug to see polling job process report - otherwise nothing is printed throughout this job.

### Supported Options

All the options that can be used in the Kusto Sink can be found in KustoSinkOptions.

**Mandatory Parameters:**

* **KUSTO_CLUSTER**:
  'kustoCluster' - Target Kusto cluster to which the data will be written.
  Use either cluster profile name for global clusters, or <profile-name.region> for regional clusters.
  For example: if the cluster URL is 'https://testcluster.eastus.kusto.windows.net', set this property
  as 'testcluster.eastus' or the URL.

* **KUSTO_DATABASE**:
  'kustoDatabase' - Target Kusto database to which the data will be written. The client must have 'user' and 'ingestor'
  privileges on this database.

* **KUSTO_TABLE**:
  'kustoTable' - Target Kusto table to which the data will be written. If _KUSTO_CREATE_TABLE_OPTIONS_ is
  set to "FailIfNotExist" (default), the table must already exist, and the client must have
  'admin' privileges on the table.

**Authentication Parameters** can be found here - [AAD Application Authentication](Authentication.md).

**Important Optional Parameters:**
* **KUSTO_WRITE_MODE**
  'writeMode' - For production big loads it is most suggested to move to Queued mode !    
  'Transactional' mode (default) - guarantees write operation to either completely succeed or fail together
  this will include the following additional work: create a temporary table and after processing the data - poll on the ingestion result
  after which the operation move the data to the destination table (the last part is a metadata operation only).  
  'Queued' mode - The write operation finishes after data is processed by the workers, the data may not be completely
  available up until the service finishes loading it, failures on the service side will not propagate to Spark but can still be seen.
  'Queued' mode scales better than the Transactional mode as it doesn't need to do track each individual ingestion created by the workers.
  This can also solve many problems faced when using Transactional mode intermediate table and better work with Materialized views.
  *Note - Both modes above are using Kusto native queued ingestion as described [here](https://learn.microsoft.com/azure/data-explorer/kusto/api/netfx/about-kusto-ingest#queued-ingestion).  
  'Stream' mode - uses [stream ingestion](https://learn.microsoft.com/azure/data-explorer/ingest-data-streaming?tabs=azure-portal%2Cjava) 
  to load data into Kusto. Streaming ingestion is useful for loading data when you need low latency between ingestion and query.
  *Note - [Streaming ingestion policy](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/management/streamingingestionpolicy) must be enabled on the destination table or database and enabled on cluster configuration (see [documentation](https://learn.microsoft.com/azure/data-explorer/ingest-data-streaming?tabs=azure-portal%2Cjava) for details).
  As ADX Streaming ingestion has a [data size limit](https://learn.microsoft.com/azure/data-explorer/ingest-data-streaming) of 4 MB, for each partition over a batched rdd the connector will ingest 4MB chunks of data, ingesting each individually. For each such batch - The connector will try 3 times to stream the data and if fails it will fallback to uploading it to blob storage and queue the ingestion. 
  It is therefore recommended to configure the rate of Spark stream to produce around 10mb of data per batch and avoid using Stream.
  It is also recommended to tune the target table [ingestion batching policy](https://learn.microsoft.com/azure/data-explorer/kusto/management/batching-policy) as this will effect the fallback flow latency.
  **note : Do not use Stream mode for the sake of streaming as ADX streaming has additional cost and may not be what you need. Spark streaming goes well with Queued mode when developing continuous integration.
  If a request exceeds this size, it will be broken into multiple appropriately sized chunks.

* **KUSTO_POLLING_ON_DRIVER**:
  'pollingOnDriver' - If set to false (default) Kusto Spark will create a new job for the final two ingestion steps done after processing the data, so that the write operation doesn't seem to 'hang' on the Spark UI.
  It's recommended to set this flag to true in production scenarios, so that the worker node doesn't occupy a core while completing the final ingestion steps.
  This is irrelevant for 'Queued' mode
  >Note:By default (or if polling is false) the logs for progress of the polling operations are available on worker nodes logs, else (if true) these are part of the driver log4j logs and are on debug verbosity.

* **KUSTO_TABLE_CREATE_OPTIONS**:
  'tableCreateOptions' - If set to 'FailIfNotExist' (default), the operation will fail if the table is not found
  in the requested cluster and database.  
  If set to 'CreateIfNotExist' and the table is not found in the requested cluster and database,
  it will be created, with a schema matching the DataFrame that is being written.

* **KustoSinkOptions.KUSTO_SPARK_INGESTION_PROPERTIES_JSON**:
  'sparkIngestionPropertiesJson' - A json representation of a `SparkIngestionProperties` (use `toString` to make a json of an instance).

  Properties:

    - dropByTags, ingestByTags, additionalTags, ingestIfNotExists: util.ArrayList[String] -
      Tags list to add to the extents. Read [kusto docs - extents](https://docs.microsoft.com/azure/kusto/management/extents-overview#ingest-by-extent-tags)

    - creationTime: DateTime - sets the extents creationTime value to this date

    - csvMapping: String - a full json representation of a csvMapping (the connector always uploads csv files to Kusto),
      see here [kusto docs - mappings](https://docs.microsoft.com/azure/kusto/management/mappings)

    - csvMappingNameReference: String - a reference to the name of a csvMapping pre-created for the table

    - flushImmediately: Boolean - use with caution - flushes the data immediately upon ingestion without aggregation.

**Advanced Users Parameters:**

* **KUSTO_TIMEOUT_LIMIT**:
  'timeoutLimit' - After the dataframe is processed, a polling operation begins. This integer corresponds to the period in seconds after which the polling
  process will timeout, eventually deleting the staging resources and fail the command. This is an upper limit that may coexist with addition timeout limits as configured on Spark or Kusto clusters.  
  Default: '172000' (2 days)

* **KUSTO_STAGING_RESOURCE_AUTO_CLEANUP_TIMEOUT**:
  'stagingResourcesAutoCleanupTimeout' - An integer number corresponding to the period in seconds after which the staging resources used for the writing
  operations are cleaned if they weren't cleaned gracefully at the end of the run.
  Default: '172000' (7 days)

* **KUSTO_ADJUST_SCHEMA**:
  'adjustSchema' If set to 'NoAdjustment' (default), it does nothing.
  If set to 'GenerateDynamicCsvMapping', dynamically generates csv mapping based on DataFrame schema and target Kusto
  table column names. If some Kusto table fields are missing in the DataFrame,
  they will be ingested as empty. If some DataFrame fields are missing in target table, it fails.
  If SparkIngestionProperties.csvMappingNameReference exists, it fails.
  If set to 'FailIfNotMatch' - fails if schemas don't agree on names and order.

* **KUSTO_CLIENT_BATCHING_LIMIT**:
  'clientBatchingLimit' - A limit indicating the size in MB of the aggregated data before ingested to Kusto. Note that
  this is done for each partition. The Kusto ingestion endpoint also aggregates data with a default of 1GB, but here
  we suggest a maximum of 100MB to adjust it to Spark pulling of data.

* **KUSTO_REQUEST_ID**:
  'requestId' - A unique identifier UUID for this ingestion command. Will be used as part of the staging table name as well.

* **KUSTO_TEMP_TABLE_NAME**:
  "tempTableName" - Provide a temporary table name that will be used for this write operation to achieve transactional write and move
  data to destination table on success. Table is expected to exist and unique per run (as we delete the table
  at the end of the process and therefore should be per write operation). In case of success, the table will be
  deleted; in case of failure, it's up to the user to delete. It is most recommended altering the table [auto-delete
  policy](https://docs.microsoft.com/azure/data-explorer/kusto/management/auto-delete-policy) to not get stuck with 'ghost' tables -
  Use this option if you want to persist partial write results as result of error (as the failure could be of a single partition that exhausted its retries)
  This is not relevant for 'Queued' mode
### Performance Considerations

Write performance depends on multiple factors, such as scale of both Spark and Kusto clusters.
Regarding Kusto target cluster configuration, one of the factors that impacts performance and latency
is the table's [Ingestion Batching Policy](https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy). The default policy
works well for typical scenarios, especially when writing large amounts of data as batch. For reduced latency,
consider altering the policy to a relatively low value (minimal allowed is 10 seconds).
For more details and command reference, please see [Ingestion Batching Policy command reference](https://docs.microsoft.com/azure/kusto/management/batching-policy).

### Examples

Synchronous mode, table already exists:
```scala
df.write
  .format("com.microsoft.kusto.spark.datasource")
  .option(KustoSinkOptions.KUSTO_CLUSTER, "MyCluster.RegionName")
  .option(KustoSinkOptions.KUSTO_DATABASE, "MyDatabase")
  .option(KustoSinkOptions.KUSTO_TABLE, "MyTable")
  .option(KustoSinkOptions.KUSTO_AAD_APP_ID, "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
  .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, "MyPassword") 
  .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, "AAD Authority Id") // "microsoft.com"
  .mode(SaveMode.Append)
  .save()
``` 

IngestionProperties and short scala usage:
```scala
val sp = new SparkIngestionProperties
var tags = new java.util.ArrayList[String]()
tags.add("newTag")
sp.ingestByTags = tags
sp.creationTime = new DateTime().minusDays(1)
df.write.kusto(cluster,
             database,
             table, 
             conf, // optional
             Some(sp)) // optional
```

Open verbose logging:
```scala
com.microsoft.kusto.spark.utils.KustoDataSourceUtils.setLoggingLevel("debug")
```
Asynchronous mode, table may not exist and will be created:
```scala
df.write
  .format("com.microsoft.kusto.spark.datasource")
  .option(KustoSinkOptions.KUSTO_CLUSTER, "MyCluster.RegionName")
  .option(KustoSinkOptions.KUSTO_DATABASE, "MyDatabase")
  .option(KustoSinkOptions.KUSTO_TABLE, "MyTable")
  .option(KustoSinkOptions.KUSTO_AAD_APP_ID, "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
  .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, "MyPassword")
  .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, "AAD Authority Id") // "microsoft.com"
  .option(KustoSinkOptions.KUSTO_WRITE_ENABLE_ASYNC, true)
  .option(KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS, "CreateIfNotExist")
  .mode(SaveMode.Append)
  .save()
```

## Streaming Sink: 'writeStream' command

Kusto Sink Connector was adapted to support writing from a streaming source to a Kusto table.

### Command Syntax
  ```scala
  <query-name> = <streaming-source-name>
  .writeStream
  .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
  .options(Map(
  KustoSinkOptions.<option-name-1>, <option-value-1>,
    ...,
    KustoSinkOptions.<option-name-n>, <option-value-n>))
      .trigger(Trigger.Once) // Or use ProcessingTime
  ```
### Example
 ```scala
 var customSchema = new StructType().add("colA", StringType, nullable = true).add("colB", IntegerType, nullable = true)

// Read data to stream 
val csvDf = spark
  .readStream
  .schema(customSchema)
  .csv("/FileStore/tables")

spark.conf.set("spark.sql.streaming.checkpointLocation", "/FileStore/temp/checkpoint")

val kustoQ = csvDf
  .writeStream
  .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
  .options(Map(
    KustoSinkOptions.KUSTO_CLUSTER -> cluster,
    KustoSinkOptions.KUSTO_TABLE -> table,
    KustoSinkOptions.KUSTO_DATABASE -> database,
    KustoSinkOptions.KUSTO_AAD_APP_ID -> appId,
    KustoSinkOptions.KUSTO_AAD_APP_SECRET -> appKey,
    KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID -> authorityId))
  .trigger(Trigger.Once)

kustoQ.start().awaitTermination(TimeUnit.MINUTES.toMillis(8))      
 ```

For more reference code examples, please see:

[SimpleKustoDataSink](../samples/src/main/scala/SimpleKustoDataSink.scala)

[KustoConnectorDemo](../samples/src/main/scala/KustoConnectorDemo.scala)

[Python samples](../samples/src/main/python)

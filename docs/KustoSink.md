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
 
 ### Supported Options
 
 All the options that can be used in the Kusto Sink can be found in KustoSinkOptions.
 
**Mandatory Parameters:** 
 
* **KUSTO_CLUSTER**:
 'kustoCluster' - Target Kusto cluster to which the data will be written.
 Use either cluster profile name for global clusters, or <profile-name.region> for regional clusters.
 For example: if the cluster URL is 'https://testcluster.eastus.kusto.windows.net', set this property 
 as 'testcluster.eastus' 
  
 * **KUSTO_DATABASE**: 
 'kustoDatabase' - Target Kusto database to which the data will be written. The client must have 'user' and 'ingestor' 
 privileges on this database.
 
 * **KUSTO_TABLE**: 
 'kustoTable' - Target Kusto table to which the data will be written. If _KUSTO_CREATE_TABLE_OPTIONS_ is 
 set to "FailIfNotExist" (default), the table must already exist, and the client must have 
 'admin' privileges on the table.
 
 **Authentication Parameters** can be found here - [AAD Application Authentication](Authentication.md). 
 
 **Optional Parameters:** 
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
    
    - csvMappingNameReference: String - a reference to the preexisting csvMapping for the table
    
    - flushImmediately: Boolean - use with caution - flushes the data immidiatly upon ingestion without aggregation.

 * **KUSTO_TIMEOUT_LIMIT**:
   'timeoutLimit' - After the dataframe is processed, a polling operation begins. This integer corresponds to the period in seconds after which the polling
   process will timeout, eventually deleting the staging resources and fail the command. This is an upper limit that may coexist with addition timeout limits as configured on Spark or Kusto clusters.  
   Default: '172000' (2 days)

 * **KUSTO_STAGING_RESOURCE_AUTO_CLEANUP_TIMEOUT**:
  'stagingResourcesAutoCleanupTimeout' - An integer number corresponding to the period in seconds after which the staging resources used for the writing
   operation are cleaned if they weren't cleaned gracefully at the end of the run.
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
    
 >**Note:**
 For both synchronous and asynchronous operation, 'write' is an atomic transaction, i.e. 
 either all data is written to Kusto, or no data is written. 
 
### Performance Considerations

Write performance depends on multiple factors, such as scale of both Spark and Kusto clusters.
Regarding Kusto target cluster configuration, one of the factors that impacts performance and latency 
is [Ingestion Batching Policy](https://docs.microsoft.com/azure/kusto/concepts/batchingpolicy). The default policy 
works well for typical scenarios, especially when writing large amounts of data as batch. For reduced latency,
consider altering the policy to a relatively low value (minimal allowed is 10 seconds).
**This is mostly relevant when writing to Kusto in streaming mode**.
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
   
   [Python samples](../samples/src/main/python/pyKusto.py)

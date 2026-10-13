# Kusto Source Connector

Kusto Source Connector allows reading data from a table in the specified Kusto cluster and database 
to a Spark DataFrame 

## Authentication

The connector uses **Azure Active Directory (AAD)** to authenticate the client application 
that is using it. Please verify the following first:
 * Client application is registered in AAD
 * Client application has 'viewer' privileges or above on the target database, 
 or 'admin' privileges on the target table (either one is sufficient)
 
 For details on Kusto principal roles, please refer to [Role-based Authorization](https://docs.microsoft.com/azure/kusto/management/access-control/role-based-authorization) 
 section in [Kusto Documentation](https://docs.microsoft.com/azure/kusto/).
 
 For managing security roles, please refer to [Security Roles Management](https://docs.microsoft.com/azure/kusto/management/security-roles) 
 section in [Kusto Documentation](https://docs.microsoft.com/azure/kusto/).
 
 ## Source: 'read' command
 
 The connector implements Spark 'Datasource V1' API. 
 Kusto data source identifier is "com.microsoft.kusto.spark.datasource". 
 Kusto table schema is translated into a spark schema as explained in [Spark-Kusto DataTypes](Spark-Kusto%20DataTypes%20mapping.md).
 The reading is done in one of two modes: 'Single' and 'Distributed'. By default, the connector estimates the query's row count.
 An estimate of 5,000 rows or fewer selects 'Single' mode; larger estimates, or a failure to obtain a row count, select 'Distributed' mode.
 A failed automatic 'Single' read can also fall back to 'Distributed' mode. In 'Distributed' mode, the connector runs an asynchronous
 [export command](https://learn.microsoft.com/en-us/kusto/management/data-export/export-data-to-storage) to temporary storage, followed by a
 distributed Spark Parquet read. This export can happen even when the user only requested a read and no query-result limit was exceeded.
 The connector implements TableScan, PrunedFilteredScan, and InsertableRelation.
 
 ### Command Syntax
 
 There are two command flavors for reading from Kusto:
 
 **Simplified Command Syntax**: 
  ```
 <dataframe-name> = 
 spark.read.kusto(<cluster-name.region-name>, <database-name>, <kusto-query>, <parameters map>, <ClientRequestProperties>)
  ```
 where:
 * **Kusto-query** is any valid Kusto query. For details, please refer to [Query statements documentation](https://docs.microsoft.com/azure/kusto/query/statements). To read the whole table, just provide the table name as query.
 >Note:
 Kusto admin commands cannot be executed via the connector.
 * **Parameters map** is a set of key-value pairs, where the key is the parameter name. See [Supported Options](#supported-options)
 section for details
  
**Elaborate Command Syntax**: 
```
<dataframe-name> = 
sqlContext.read
.format("com.microsoft.kusto.spark.datasource")
.option(KustoSourceOptions.KUSTO_CLUSTER, <cluster-name.region-name>)
.option(KustoSourceOptions.KUSTO_DATABASE, <database-name>)
.option(KustoSourceOptions.KUSTO_QUERY, <kusto-query>)
.load()
```
Where **parameters map** is identical for both syntax flavors.
      
 ### Supported Options
   
All the options that can be used in the Kusto Source can be found in KustoSourceOptions.

 **Mandatory Parameters:** 
  
 * **KUSTO_CLUSTER**:
  'kustoCluster' - Kusto cluster from which the data will be read.
  Use either cluster profile name for global clusters, or <profile-name.region> for regional clusters.
  For example: if the cluster URL is 'https://testcluster.eastus.kusto.windows.net', set this property 
  as either 'testcluster.eastus', or 'https://testcluster.eastus.kusto.windows.net'.
   
  * **KUSTO_DATABASE**: 
  'kustoDatabase' - Kusto database from which the data will be read. The client must have 'viewer' 
  privileges on this database, unless it has 'admin' privileges on the table.
  
  **Authentication Parameters** can be found [AAD Application Authentication](Authentication.md). 

  * **KUSTO_QUERY**: 
  'kustoQuery' - A flexible Kusto query (can simply be a table name). The schema of the resulting dataframe will match the schema of the query result. 
 
 
 **Optional Parameters:** 
 
 * **KUSTO_TIMEOUT_LIMIT**:
 'timeoutLimit' - An integer corresponding to the period in seconds after which the operation will timeout.
 This is an upper limit that may coexist with addition timeout limits as configured on Spark or Kusto clusters.
 
    **Default:** '172000' (2 days)    
    
* **KUSTO_CLIENT_REQUEST_PROPERTIES_JSON**:
  'clientRequestPropertiesJson' - A json representation for [ClientRequestProperties](https://github.com/Azure/azure-kusto-java/blob/master/data/src/main/java/com/microsoft/azure/kusto/data/ClientRequestProperties.java)
   used in the call for reading from Kusto (used in the single query for 'single' mode or for the export command for 'distributed' mode). Use toString to create the json.

* **KUSTO_REQUEST_ID**:
    'requestId' - A unique identifier UUID for this reading operation. Setting this will override the ClientRequestId on the
    ClientRequestProperties object if set.
    
* **KUSTO_READ_MODE**
'readMode' - Override the connector heuristic to choose between 'Single' and 'Distributed' mode.
Options are - 'ForceSingleMode', 'ForceDistributedMode'.
Scala and Java users may take these options from com.microsoft.kusto.spark.datasource.ReadMode.

  The following is the behavior of the connector in either of these read modes

  * **Single mode**: Executes a direct query and materializes its results on the Spark driver. Both driver memory and Kusto
    [query-result limits](https://learn.microsoft.com/en-us/kusto/concepts/query-limits#limit-on-result-set-size-result-truncation)
    apply (by default, 500,000 rows or 64 MB). `ForceSingleMode` overrides the initial selection, not the service limits or
    driver-memory requirements. Do not rely on automatic fallback when a forced single-mode read exceeds query limits.

  * **Distributed mode**: Exports query results to temporary storage, then reads the exported Parquet data into a DataFrame.
    This mode can be selected automatically, explicitly requested, or used after a direct-read failure; it does not require
    the query to exceed the service's query-result limits.

  * **Default selection of the mode**: Estimates of 5,000 rows or fewer select 'Single' mode. Larger estimates or a failure to
    obtain a row count select 'Distributed' mode. A zero count returns an empty DataFrame. The 5,000-row selection threshold is
    a connector heuristic, not the service's 500,000-row query-result limit.

  **Note**: Distributed mode will use an external storage hop and will add to COGS because of the storage that is used in between.

  **Export-capacity throttling during a read**

  A failure with `State: 'Throttled'`, `CommandType: 'DataExportToFile'`, and `Origin: 'CapacityPolicy/Export'` means the
  distributed read's export was aborted because concurrent export capacity was exceeded. Concurrent notebooks and other
  exports share this capacity; this is not the same as query-concurrency throttling or a query-memory error. Spark evaluates
  reads lazily, so this source-read failure can surface at a downstream action such as `.save()`.

  The connector automatically resubmits a distributed-read partition when its asynchronous operation reports
  `State: 'Throttled'`, or submission is explicitly rejected with the SDK's HTTP 429 `ThrottleException`.
  There are **at most four attempts total (the initial attempt plus three retries)** per partition. HTTP rejections and
  asynchronous throttling share this limit. Before retries, the nominal delays are **5, 10, and 20 seconds**, each
  independently randomized by ±50%: **2.5–7.5, 5–15, and 10–30 seconds**, respectively. Jitter prevents competing reads
  from retrying in lockstep. These are bounded defaults for transient contention, not a guarantee that capacity will
  become available. The asynchronous status response and the SDK's throttling exception do not expose a `Retry-After`
  delay to this retry path.

  Each partition shares its original `timeoutLimit` budget across submissions, polling, and retry backoff; retries do
  not reset that budget. If the next delay does not fit, the latest throttling failure is returned immediately. Deadline
  expiry and interruption stop polling/retries. This is not a hard deadline for an in-flight synchronous SDK call and
  does not cancel an export already running on the service. Increasing `timeoutLimit` does not increase the attempt limit.

  Every attempt uses a separate temporary directory. Only directories from successfully completed attempts are read
  by Spark and stored in the transient read cache; successful partitions are not re-exported when another partition is
  throttled. `Failed`, `PartiallySucceeded`, authorization errors, cancellations, and unknown submission outcomes are
  **not** retried by this path. In particular, the generic command-submission retry policy is disabled for these exports:
  losing a response does not prove an export was rejected, and blindly replaying it could duplicate output. Transient
  status-lookup failures continue polling the same operation instead of starting another export.

  Reduce or stagger concurrent reads/exports if throttling persists, and ask the service administrator to review
  effective export capacity. Retries add latency and some service load; they do not solve sustained capacity shortages.
  Retried queries can observe newer source data, so this is not a point-in-time snapshot guarantee. Failed exports can
  leave artifacts in temporary storage; the connector excludes them from readback but does not delete them or change
  the caller's storage lifecycle policy. See
  [export failure behavior](https://learn.microsoft.com/en-us/kusto/management/data-export/export-data-to-storage#failures-during-export-commands).
  If retrying an entire pipeline outside the connector, ensure its downstream writes are safe to repeat.

  **Storage security**
  In Distributed mode by default the internal Kusto storage accounts are queried and used for ease of the connector. This is not recommended in production scenarios. The storage to be used can be customized by passing [Transient storage parameters](#transient-storage-parameters) parameter specified below. This gives the caller of the read a more granular control on blob security policies as opposed to default blob security policies used by the connector and Kusto. This will also come in useful when Kusto administrators restrict security by applying callout policies on what hosts can traffic be let onto. If traffic is not permitted by administrators to the default internal storage, the read operation will fail for Distributed mode.

* **KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE**
When 'Distributed' read mode is used and this is set to 'true', the request query is exported only once and exported data is reused.

* **KUSTO_QUERY_FILTER_PUSH_DOWN**
If set to 'true', query executed on kusto cluster will include the filters.
  'false' by default if KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE=true, and
  'true' by default if KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE=false

* **KUSTO_EXPORT_OPTIONS_JSON**:
  'kustoExportOptionsJson' - JSON that provides the list of [export options](https://learn.microsoft.com/azure/data-explorer/kusto/management/data-export/export-data-to-storage) in case of distributed read (either because of query limits getting hit or user request for distributed mode). 
  The export options do not support the _OutputDataFormat_ which is defaulted to _parquet_, _namePrefix_ which is a new directory specifically for the current read,
   _compressionType_ is defaulted to snappy and the command also specifies _compressed_ (to create .snappy.gz files), to turn extra compression off - it can be set to _none_ (**not recommended**)
  i.e .option("kustoExportOptionsJson", "{\"distribution\":\"per_node\"}")
>Note: Connector versions >= 3.1.10 will automatically set useNativeParquetWriter=false if Spark version < 3.3.0 as Kusto service uses now vectorized parquet writer introduced in this version. Do not set to true for lower versions as it will fail. Users are advised to move to Spark 3.3.0 to leverage this new great performance enhancement on both Spark side and Kusto service side.

* **STORAGE_PROTOCOL**:
  'storageProtocol' - Specifies the storage protocol to use when reading from blob storage in distributed mode. 
  Valid values are:
  - 'wasbs' (Windows Azure Storage Blob Service) - default
  - 'abfs' (Azure Blob File System over HTTP)
  - 'abfss' (Azure Blob File System over HTTPS - recommended for secure connections)
  
  Defaults to 'wasbs' if not specified. The default should work in most cases.
  This option is only valid when using 'ForceDistributedMode' as the read mode.
  Example: .option("storageProtocol", "abfss")
  
  **Important**: 
  - This option can only be used with KUSTO_READ_MODE='ForceDistributedMode'. If specified with any other read mode, an error will be thrown.
  - ABFS/ABFSS protocols require the platform to support Azure Hadoop libraries version >= 3.4.2
  - If your platform does not support WASB (legacy blob storage), migrating to ABFS is recommended
  - ABFS with Account Key authentication is not currently supported; use SAS authentication instead
 
#### Transient Storage Parameters
When reading data from Kusto in 'distributed' mode, the data is exported from Kusto into a blob storage every time the corresponding RDD is materialized.
If the user doesn't specify storage parameters and a 'Distributed' read mode is chosen - the storage used will be provided by Kusto ingest service.

>Note: If the user provides the blob storage - the blobs created are the caller's responsibility. This includes provisioning the storage, rotating access keys,
deleting transient artifacts etc. KustoBlobStorageUtils module contains helper functions for deleting blobs based on either account and container
coordinates and account credentials, or a full SAS URL with write, read and list permissions once the corresponding RDD is no longer needed. Each transaction stores transient blob
artifacts in a separate directory. This directory is captured as part of read-transaction information logs reported on the Spark Driver node.

* **KUSTO_TRANSIENT_STORAGE**:
 'transientStorage' KustoSourceOptions.KUSTO_TRANSIENT_STORAGE -> new TransientStorageParameters(Array(new TransientStorageCredentials(blobSas)))

 ```python
transientStorage = "{ \"storageCredentials\" : [ { \
    \"storageAccountName\": \"1jdldsdke2etestcluster01\",\
    \"blobContainer\": \"20221225-exportresults-0\",\
    \"sasUrl\" : \"https://1jdldsdke2etestcluster01.blob.core.windows.net/20221225-exportresults-0\", \
  \"sasKey\" : \"?sas\"\,
  } ],"endpointSuffix" : "core.windows.net" }"
  ...
  option("transientStorage", transientStorage). \
 ```
>Note:
Creating `transientStorage` string by using `TransientStorageParameters.toString` will not work. Either it can be created manually or by using `TransientStorageParameters.toInsecureString`.

>Note:
`transientStorage` string now supports [impersonate](https://learn.microsoft.com/en-us/kusto/api/connection-strings/storage-connection-strings?view=microsoft-fabric#impersonation) access to storage. The application id needs to have __Blob Storage Access Contributor__ for the impersonate access to work.

```scala
val transientStorage = new TransientStorageCredentials(
            "https://ateststorage.blob.core.windows.net/kusto;impersonate")
```

```python
transientStorage = "{\"storageCredentials\": [{\"storageAccountName\": \"ateststorage\",\"blobContainer\": \"kusto\",\"sasUrl\": \"https://ateststorage.blob.core.windows.net/kusto;impersonate\"},{\"storageAccountName\": \"ateststorage2\",\"blobContainer\": \"kusto2\",\"sasUrl\": \"https://ateststorage2.blob.core.windows.net/kusto2;impersonate\"}],  \"endpointSuffix\": \"core.windows.net\"}"
  ...
  option("transientStorage", transientStorage). \
```

##### OneLake Transient Storage (Fabric)

For Fabric environments, you can use **OneLake** as transient storage instead of Azure Blob/ADLS2. Kusto exports data using `;impersonate` (writes as the calling user) and Spark reads back over `abfss://` using ambient AAD.

**Requirements:**
- Workspace Contributor access to the OneLake lakehouse
- Kusto cluster must be able to reach the OneLake endpoint
- Spark runtime must have AAD configured for ABFSS (automatic in Fabric notebooks)

**JSON format** — use the `oneLakeUrl` field (not `sasUrl`):

```python
import json

transient_storage = json.dumps({
    "storageCredentials": [
        {"oneLakeUrl": "https://onelake.dfs.fabric.microsoft.com/<workspace-id>/<lakehouse-id>/Files/kusto-transient"}
    ]
})

df = spark.read \
    .format("com.microsoft.kusto.spark.datasource") \
    .option("kustoCluster", "https://mycluster.kusto.fabric.microsoft.com") \
    .option("kustoDatabase", "mydb") \
    .option("kustoQuery", "MyTable | take 1000") \
    .option("accessToken", token) \
    .option("readMode", "ForceDistributedMode") \
    .option("transientStorage", transient_storage) \
    .load()
```

**Supported URL formats:**

```
# HTTPS form:
https://<endpoint>/<workspace>/<lakehouse>/Files/<subpath>

# ABFSS form:
abfss://<workspace>@<endpoint>/<lakehouse>/Files/<subpath>
```

### Examples
 
 **Using simplified syntax**
 
 Create a DataFrame based on a query accessing 'MyKustoTable' table
 ```
 val conf: Map[String, String] = Map(
       KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
       KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey
     )
     
 val df = spark.read.kusto(cluster, database, "MyKustoTable | where (ColB % 1000 == 0) | distinct ColA ", conf)
 // spark.read.kusto(cluster, database, table, conf, Some(clientRequestProperties))
 ``` 
 
 **Using elaborate syntax**
  
  Create a DataFrame by reading all of 'MyKustoTable' table
  ```
 val conf: Map[String, String] = Map(
       KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
       KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
       KustoSourceOptions.KUSTO_QUERY -> "MyKustoTable"
     )
 
 val df = sqlContext.read
  .format("com.microsoft.kusto.spark.datasource")
  .option(KustoSourceOptions.KUSTO_CLUSTER, "MyCluster.RegionName")
  .option(KustoSourceOptions.KUSTO_DATABASE, "MyDatabase")
  .options(conf)
  .load()
  ```
  
>Note:
 Kusto uses case-sensitive column names while Spark does not, therefore the following Kusto table will fail reading (cola:string,ColA:string).
   
 For more reference code examples, please see: 
    
 [SimpleKustoDataSink](../samples/src/main/scala/SimpleKustoDataSink.scala)
 
 [KustoConnectorDemo](../samples/src/main/scala/KustoConnectorDemo.scala)
 
 [Python samples](../samples/src/main/python/pyKusto.py)

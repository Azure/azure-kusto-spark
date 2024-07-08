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
 The reading is done in one of two modes: 'Single' and 'Distributed'. By default, if the user did not override the read mode - an estimated
 count for the query is made; for small row count - a 'Single' mode is chosen with a fallback to the 'Distributed' mode (as the first might
 fail due to [kusto limit policy](https://docs.microsoft.com/azure/kusto/concepts/querylimits)). In 'Distributed' mode, a distributed
 [export command](https://docs.microsoft.com/azure/kusto/management/data-export/export-data-to-storage) is called followed by a
 distributed spark Parquet read.  
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

  * **Single mode** : In force single mode Direct query and get the results

  * **Distributed mode** : When you hit Kusto [query limits](https://aka.ms/kustoquerylimits) (memory / number of records), data is [exported](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/management/data-export/export-data-to-storage) to Blob storage (export containers) , then the exported data is read and processed into a Dataframe. 

  * **Default selection of the mode** : In case these options are not specified, the connector tries to approximate row count and the time the query would take. If the approximated row count exceeds the query limits specified above, the connector will automatically use Distributed mode.

  **Note**: Distributed mode will use an external storage hop and will add to COGS because of the storage that is used in between.

  **Storage security**
  In Distributed mode by default the internal Kusto storage accounts are queried and used for ease of the connector. This is not recommended in production scenarios. The storage to be used can be customized by passing [Transient storage parameters](#transient-storage-parameters) parameter specified below. This gives the caller of the read a more granular control on blob security policies as opposed to default blob security policies used by the connector and Kusto. This will also come in useful when Kusto administrators restrict security by applying callout policies on what hosts can traffic be let onto. If traffic is not permitted by administrators to the default internal storage, the read operation will fail for Distributed mode.

* **KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE**
When 'Distributed' read mode is used and this is set to 'true', the request query is exported only once and exported data is reused.

* **KUSTO_QUERY_FILTER_PUSH_DOWN**
If set to 'true', query executed on kusto cluster will include the filters.
  'false' by default if KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE=true, and
  'true' by default if KUSTO_DISTRIBUTED_READ_MODE_TRANSIENT_CACHE=false

* **KUSTO_EXPORT_OPTIONS_JSON**:
  'kustoExportOptionsJson' - JSON that provides the list of [export options](https://learn.microsoft.com/azure/data-explorer/kusto/management/data-export/export-data-to-storage) in case of distributed read (either because of query limits getting hit or user request for ForceDistributed mode). 
  The export options do not support the _OutputDataFormat_ which is defaulted to _parquet_, _namePrefix_ which is a new directory specifically for the current read,
   _compressionType_ is defaulted to snappy and the command also specifies _compressed_ (to create .snappy.gz files), to turn extra compression off - it can be set to _none_ (**not recommended**)
  i.e .option("kustoExportOptionsJson", "{\"distribution\":\"per_node\"}")
>Note: Connector versions >= 3.1.10 will automatically set useNativeParquetWriter=false if Spark version < 3.3.0 as Kusto service uses now vectorized parquet writer introduced in this version. Do not set to true for lower versions as it will fail. Users are advised to move to Spark 3.3.0 to leverage this new great performance enhancement on both Spark side and Kusto service side.
 
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

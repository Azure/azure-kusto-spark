# Kusto Source Connector

Kusto source connector allows reading data from a table in the specified Kusto cluster and database 
to a Spark DataFrame 

## Authentication

Kusto connector uses  **Azure Active Directory (AAD)** to authenticate the client application 
that is using it. Please verify the following before using Kusto connector:
 * Client application is registered in AAD
 * Client application has 'viewer' privileges or above on the target database, 
 or 'admin' privileges on the target table (either one is sufficient)
 
 For details on Kusto principal roles, please refer to [Role-based Authorization](https://docs.microsoft.com/en-us/azure/kusto/management/access-control/role-based-authorization) 
 section in [Kusto Documentation](https://docs.microsoft.com/en-us/azure/kusto/).
 
 For managing security roles, please refer to [Security Roles Management](https://docs.microsoft.com/en-us/azure/kusto/management/security-roles) 
 section in [Kusto Documentation](https://docs.microsoft.com/en-us/azure/kusto/).
 
 ## Source: 'read' command
 
 Kusto connector implements Spark 'Datasource V1' API. 
 Kusto data source identifier is "com.microsoft.kusto.spark.datasource". 
 Kusto table schema is translated into a spark as explained in [DataTypes](Spark-Kusto DataTypes mapping.md).
 
 ### Command Syntax
 
 There are two command flavors for reading from Kusto:
 
 **Simplified Command Syntax**: 
  ```
 <dataframe-name> = 
 spark.read.kusto(<cluster-name>, <database-name>, <kusto-query>, <parameters map>)
  ```
 where:
 * **Kusto-query** is any valid Kusto query. For details, please refer to [Query statements documentation](https://docs.microsoft.com/en-us/azure/kusto/query/statements). To read the whole table, just provide the table name as query.
 >Note:
 Kusto commands  cannot be executed via the connector.
 * **Parameters map** is a set of key-value pairs, where the key is the parameter name. See [Supported Options](#supported-options)
 section for details
 
  
**Elaborate Command Syntax**: 
```
<dataframe-name> = 
sqlContext.read
.format("com.microsoft.kusto.spark.datasource")
.option(KustoSourceOptions.KUSTO_CLUSTER, <cluster-name>)
.option(KustoSourceOptions.KUSTO_DATABASE, <database-name>)
.option(KustoSourceOptions.KUSTO_QUERY, <kusto-query>)
.load()
```
Where **parameters map** is identical for both syntax flavors.
      
 ### Supported Options
   
All the options that can be use in the Kusto source are under the object KustoSourceOptions.

 **Mandatory Parameters:** 
  
 * **KUSTO_CLUSTER**:
  Kusto cluster from which the data will be read.
  Use either cluster profile name for global clusters, or <profile-name.region> for regional clusters.
  For example: if the cluster URL is 'https://testcluster.eastus.kusto.windows.net', set this property 
  as either 'testcluster.eastus', or  'https://testcluster.eastus.kusto.windows.net'.
   
  * **KUSTO_DATABASE**: 
  Kusto database from which the data will be read. The client must have 'viewer' 
  privileges on this database, unless it has 'admin' privileges on the table.
  
  **Authentication Parameters** can be found [AAD Application Authentication](Authentication.md). 

  * **KUSTO_QUERY**: 
  A flexible Kusto query (can simply be a table name). The schema of the resulting dataframe will match the schema of the query result. 
 
 
 **Optional Parameters:** 
 
 * **KUSTO_TIMEOUT_LIMIT**:
 An integer number corresponding to the period in seconds after which the operation will timeout.
 This is an upper limit that may coexist with addition timeout limits as configured on Spark or Kusto clusters.
 
    **Default:** '5400' (90 minutes)    
    
* **KUSTO_CLIENT_REQUEST_PROPERTIES_JSON**:
  A json representation for [ClientRequestProperties](https://github.com/Azure/azure-kusto-java/blob/master/data/src/main/java/com/microsoft/azure/kusto/data/ClientRequestProperties.java)
   used in the call for reading from kusto (or the export command for 'scale' mode). Use toString to make the json.
    
    
#### Transient Storage Parameters
When reading data from Kusto in 'scale' mode, the data is exported from Kusto into a blob storage every time the corresponding RDD is materialized.
In order to allow working in 'scale' mode, storage parameters must be provided by the caller. 

>Note: maintenance of the blob storage is the caller responsibility. This includes provisioning the storage, rotating access keys, 
deleting transient artifacts etc. KustoBlobStorageUtils module contains helper functions for deleting blobs based on either account and container 
coordinates and account credentials, or a full SAS URL, once the corresponding RDD is no longer needed. Each transaction stores transient blob 
artifacts in a separate directory. This directory is captured as part of read-transaction information logs reported on Spark Driver node 

* **KUSTO_BLOB_STORAGE_ACCOUNT_NAME**
Transient storage account name. Either this, or a SAS url, must be provided in order to access the storage account

* **KUSTO_BLOB_STORAGE_ACCOUNT_KEY**
Storage account key. Either this, or a SAS url, must be provided in order to access the storage account

* **KUSTO_BLOB_STORAGE_SAS_URL**
SAS access url: a complete url of the SAS to the container. Either this, or a storage account name and key, must be provided
  in order to access the storage account
  
* **KUSTO_BLOB_CONTAINER**
Blob container name. This container will be used to store all transient artifacts created every time the corresponding RDD is materialized. 
Once the RDD is no longer required by the caller application, the container and/or all its contents can be deleted by the caller.

 ### Examples
 
 **Using simplified syntax**
 
 Create a DataFrame based on a query accessing 'MyKustoTable' table
 ```
 val conf: Map[String, String] = Map(
       KustoSourceOptions.KUSTO_AAD_CLIENT_ID -> appId,
       KustoSourceOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey
     )
     
 val df = spark.read.kusto(cluster, database, "MyKustoTable | where (ColB % 1000 == 0) | distinct ColA ", conf)
 // spark.read.kusto(cluster, database, table, conf, Some(clientRequestProperties))
 ``` 
 
 **Using elaborate syntax**
  
  Create a DataFrame by reading all of 'MyKustoTable' table
  ```
 val conf: Map[String, String] = Map(
       KustoSourceOptions.KUSTO_AAD_CLIENT_ID -> appId,
       KustoSourceOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
       KustoSourceOptions.KUSTO_QUERY -> "MyKustoTable"
     )
 
 val df = sqlContext.read
  .format("com.microsoft.kusto.spark.datasource")
  .option(KustoSourceOptions.KUSTO_CLUSTER, "MyCluster")
  .option(KustoSourceOptions.KUSTO_DATABASE, "MyDatabase")
  .options(conf)
  .load()
  ```
  
  For more reference code examples please see 
  [SimpleKustoDataSource](../samples/src/main/scala/SimpleKustoDataSource.scala) and 
  [KustoConnectorDemo](../samples/src/main/scala/KustoConnectorDemo.scala).
# Kusto Sink Connector

Kusto sink connector allows writing data from Spark to a table 
in the specified Kusto cluster and database

## Authentication

Kusto connector uses **Azure Active Directory (AAD)** to authenticate the client application 
that is using it. Please verify the following before using Kusto connector:
 * Client application is registered in AAD
 * Client application has 'user' privileges or above on the target database
 * In addition, client application has 'ingestor' privileges on the target database
 * When writing to an existing table, client application has 'admin' privileges on the target table
 
 For details on Kusto principal roles, please refer to [Role-based Authorization](https://docs.microsoft.com/en-us/azure/kusto/management/access-control/role-based-authorization) 
 section in [Kusto Documentation](https://docs.microsoft.com/en-us/azure/kusto/).
 
 For managing security roles, please refer to [Security Roles Management](https://docs.microsoft.com/en-us/azure/kusto/management/security-roles) 
 section in [Kusto Documentation](https://docs.microsoft.com/en-us/azure/kusto/).
 
 ## Batch Sink: 'write' command
 
 Kusto connector implements Spark 'Datasource V1' API. 
 Kusto data source identifier is "com.microsoft.kusto.spark.datasource". 
 Dataframe schema is translated into kusto schema as explained in [DataTypes](Spark-Kusto DataTypes mapping.md).
 
 ### Command Syntax
 ```
 <dataframe-object>
 .write
 .format("com.microsoft.kusto.spark.datasource")
 .option(KustoOptions.<option-name-1>, <option-value-1>
 ...
 .option(KustoOptions.<option-name-n>, <option-value-n>
 .save()
 ```
 
 ### Supported Options
 
**Mandatory Parameters:** 
 
* **KUSTO_CLUSTER**:
 Target Kusto cluster to which the data will be written.
 Use either cluster profile name for global clusters, or <profile-name.region> for regional clusters.
 For example: if the cluster URL is 'https://testcluster.eastus.kusto.windows.net', set this property 
 as 'testcluster.eastus' 
  
 * **KUSTO_DATABASE**: 
 Target Kusto database to which the data will be written. The client must have 'user' and 'ingestor' 
 privileges on this database.
 
 * **KUSTO_TABLE**: 
 Target Kusto table to which the data will be written. If _KUSTO_CREATE_TABLE_OPTIONS_ is 
 set to "FailIfNotExist" (default), the table must already exist, and the client must have 
 'admin' privileges on the table.
 
 **Authentication Parameters** can be found here - [AAD Application Authentication](AuthenticationMethods.md). 
 
 **Optional Parameters:** 
 * **KUSTO_TABLE_CREATE_OPTIONS**: 
 If set to 'FailIfNotExist' (default), the operation will fail if the table is not found 
 in the requested cluster and database.  
 If set to 'CreateIfNotExist' and the table is not found in the requested cluster and database,
 it will be created, with a schema matching the DataFrame that is being written.
 
 * **KUSTO_WRITE_ENABLE_ASYNC**:
  If set to 'false' (default), writing to Kusto is done synchronously. This means:
   * Once the operation completes successfully, it is guaranteed that that data was written to
 the requested table in Kusto
   * Exceptions will propagate to the client
 
   This is the recommended option for typical use cases. However, using it results in blocking
 Spark driver for as long as the operation is running, up to several minutes. 
 To avoid blocking Spark driver, it is possible to execute Kusto 'write' operation in an 
 opportunistic mode as an asynchronous operation. This results in the following behavior:
   * Spark driver is not blocked
   * In a success scenario, all data is written eventually
   * In a failure scenario, error messages are logged on Spark executor nodes, 
 but exceptions will not propagate to the client
 

 >**Note**:
 For both synchronous and asynchronous operation, 'write' is an atomic transaction, i.e. 
 either all data is written to Kusto, or no data is written.  
 
### Examples

Synchronous mode, table already exists:
```
df.write
  .format("com.microsoft.kusto.spark.datasource")
  .partitionBy("value")
  .option(KustoOptions.KUSTO_CLUSTER, "MyCluster")
  .option(KustoOptions.KUSTO_DATABASE, "MyDatabase")
  .option(KustoOptions.KUSTO_TABLE, "MyTable")
  .option(KustoOptions.KUSTO_AAD_CLIENT_ID, "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
  .option(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, "MyPassword") 
  .option(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "AAD Authority Id") // "microsoft.com"
  .save()
``` 

Asynchronous mode, table may not exist and will be created:
```
df.write
  .format("com.microsoft.kusto.spark.datasource")
  .partitionBy("value")
  .option(KustoOptions.KUSTO_CLUSTER, "MyCluster")
  .option(KustoOptions.KUSTO_DATABASE, "MyDatabase")
  .option(KustoOptions.KUSTO_TABLE, "MyTable")
  .option(KustoOptions.KUSTO_AAD_CLIENT_ID, "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
  .option(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, "MyPassword") 
  .option(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "AAD Authority Id") // "microsoft.com"
  .option(KustoOptions.KUSTO_WRITE_ENABLE_ASYNC, true)
  .option(KustoOptions.KUSTO_TABLE_CREATE_OPTIONS, "CreateIfNotExist")
  .save()
```

 ## Streaming Sink: 'writeStream' command
 
 Kusto sink connector was adapted to support writing from a streaming source to a Kusto table.
 
 ### Command Syntax
  ```
  <queue-name> = <streaming-source-name>
   .writeStream
   .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
   .options(Map(
      KustoOptions.<option-name-1>, <option-value-1>,
      ...,
      KustoOptions.<option-name-n>, <option-value-n>))
   .trigger(Trigger.Once)
  ```
 ### Example
 ```
 var customSchema = new StructType().add("colA", StringType, nullable = true).add("colB", IntegerType, nullable = true)
 
 // Read data to stream 
 val csvDf = spark
       .readStream      
       .schema(customSchema)
       .csv("/FileStore/tables")
 
 spark.conf.set("spark.sql.streaming.checkpointLocation", "/FileStore/temp/checkpoint")
 spark.conf.set("spark.sql.codegen.wholeStage", "false")
 
 val kustoQ = csvDf
       .writeStream
       .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
       .options(Map(
         KustoOptions.KUSTO_CLUSTER -> cluster,
         KustoOptions.KUSTO_TABLE -> table,
         KustoOptions.KUSTO_DATABASE -> database,
         KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
         KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
         KustoOptions.KUSTO_AAD_AUTHORITY_ID -> authorityId))
       .trigger(Trigger.Once)
 
 kustoQ.start().awaitTermination(TimeUnit.MINUTES.toMillis(8))      
 ```
 
  For more reference code examples please see 
   [SimpleKustoDataSink](../src/main/scala/com/microsoft/kusto/spark/Sample/SimpleKustoDataSink.scala) and 
   [KustoConnectorDemo](../src/main/scala/com/microsoft/kusto/spark/Sample/KustoConnectorDemo.scala).
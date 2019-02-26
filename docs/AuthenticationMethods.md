#Authentication Methods
Kusto Spark connector allows the user to authenticate with AAD to the Kusto cluster using either an AAD application,
 a user token or device authentication. Alternatively, authentication parameters can be stored in Azure Key Vault.
  In this case, the user must provide once application credentials in order to access the Key Vault resource.

## AAD Application Authentication
This authentication method is fairly straightforward, and it is used in most of the examples in this documentation.

 * **KUSTO_AAD_CLIENT_ID**: 
  AAD application (client) identifier.
  
 * **KUSTO_AAD_AUTHORITY_ID**: 
  AAD authentication authority. This is the AAD Directory (tenant) ID.
 
 * **KUSTO_AAD_CLIENT_PASSWORD**: 
 AAD application key for the client.
 
#### Example
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
## Key Vault

Kusto Spark connector allows authentication using Azure Key Vault. The  Key Vault must contain the 
mandatory read/write authentication parameters. If a parameter appears in both the Key Vault and passed directly as an option, the direct option will take precedence.
Although a combination of Key Vault and direct options is supported, for clarity, it is advised to use 
either key vault or direct options for passing authentication parameters but not both.
                                               
                                               
**The connector will look for the following secret names:**

### Kusto Cluster Authentication 
 * **kustoAppId**
 AAD application (client) identifier.
 
 * **kustoAppKey**
 AAD application key for the client.

 * **kustoAppAuthority**
  AAD authentication authority. This is the AAD Directory (tenant) ID.

### Read Parameters

>**Note**: this hese parameters are only required when working in "scale" reading mode. For details, refer to [Kusto Sink](KustoSource.md/#Transient-Storage-Parameters).
 * **blobStorageAccountName**
 Transient storage account name. Either this, or a SAS url, must be provided in order to access the storage account

 * **blobStorageAccountKey**
 Storage account key. Either this, or a SAS url, must be provided in order to access the storage account

* **blobStorageSasUrl**
 SAS access url: a complete url of the SAS to the container. Either this, or a storage account name and key, 
 must be provided in order to access the storage account
    
 * **blobContainer**
 Blob container name. This container will be used to store all transient artifacts created every time the corresponding RDD is materialized. 
 Once the RDD is no longer required by the caller application, the container and/or all its contents can be deleted by the caller.  

### Key Vault Authentication Example

```
val keyVaultClientID: String = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
val keyVaultClientPassword: String = "MyPassword"
val keyVaultUri: String = "keyVaultUri" 
 
df.write
  .format("com.microsoft.kusto.spark.datasource")
  .option(KustoOptions.KUSTO_CLUSTER, MyCluster)
  .option(KustoOptions.KUSTO_DATABASE, MyDatabase)
  .option(KustoOptions.KUSTO_TABLE, MyTable)
  .option(KustoOptions.KEY_VAULT_URI, keyVaultUri)
  .option(KustoOptions.KEY_VAULT_APP_ID, keyVaultClientID)
  .option(KustoOptions.KEY_VAULT_APP_KEY, keyVaultClientPassword)
  .save()

val conf: Map[String, String] = Map(
  KustoOptions.KEY_VAULT_URI -> keyVaultUri,
  KustoOptions.KEY_VAULT_APP_ID -> keyVaultClientID,
  KustoOptions.KEY_VAULT_APP_KEY -> keyVaultClientPassword
)

val dfResult = spark.read.kusto(cluster, database, table, conf)
 ```
## Access token
User can also use ADAL directly to acquire an AAD access token to access Kusto. 
The token must be valid throughout the duration of the read/write operation

 * **KUSTO_ACCESS_TOKEN**: 
    The AAD access token
```
df.write
  .format("com.microsoft.kusto.spark.datasource")
  .partitionBy("value")
  .option(KustoOptions.KUSTO_CLUSTER, "MyCluster")
  .option(KustoOptions.KUSTO_DATABASE, "MyDatabase")
  .option(KustoOptions.KUSTO_TABLE, "MyTable")
  .option(KustoOptions., "MyTable")
  .save()
```
## Device Authentication
If no authentication parameters were passed. The connector will request for user authentication by writing a token 
to the console, this token can be used to authenticate at https://login.microsoftonline.com/common/oauth2/deviceauth 
and will allow temporary access. The user needs appropriate privileges for the Kusto cluster as explained in [Kusto Sink](KustoSink.md/#Authentication). 

>**Note**: This method is not recommended for production!   

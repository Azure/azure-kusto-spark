#Authentication Methods
Kusto spark connector allows user to authenticate with AAD to the Kusto cluster using either an AAD application,
 a user token or device authentication. The application parameters can also be passed using azure keyVault.

## AAD application authentication
This is the easiest way to authenticate using this connector and is explained used in all the documentation examples.

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
## KeyVault
Kusto spark connector allows authentication through keyVault. The keyVault must contain the 
write/read mandatory parameters if not given in the options. If an option is passed in both,
the option passed in the command will be taken.
The connector will look for the following secret names:

###Write
 * **kustoAppId**
 AAD application (client) identifier.
 
 * **kustoAppKey**
 AAD application key for the client.

 * **kustoAppAuthority**
  AAD authentication authority. This is the AAD Directory (tenant) ID.

###Read
 * **blobStorageAccountName**
 Transient storage account name. Either this, or a SAS url, must be provided in order to access the storage account

 * **blobStorageAccountKey**
 Storage account key. Either this, or a SAS url, must be provided in order to access the storage account

* **blobStorageSasUrl**
  SAS access url: a complete url of the SAS to the container. Either this, or a storage account name and key, must be provided
    in order to access the storage account
    
 * **blobContainer**
 Blob container name. This container will be used to store all transient artifacts created every time the corresponding RDD is materialized. 
 Once the RDD is no longer required by the caller application, the container and/or all its contents can be deleted by the caller.  

##Eample

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
User can also use ADAL himself to acquire an AAD access token to access Kusto, in which case he will need to make sure
is valid for the duration of the reading/writing operation.

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
## Device authentication
If no authentication parameters were passed to the connector - the connector will write a token to the console, 
which can be used to authenticate at https://login.microsoftonline.com/common/oauth2/deviceauth and will allow
 temporary access. 

**Note:** This method is not recommended!   

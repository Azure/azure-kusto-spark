#Authentication Methods
Kusto Spark connector allows the user to authenticate with AAD using an AAD application,
 user token or device authentication. Alternatively, authentication parameters can be stored in Azure Key Vault.
  In this case, the user must provide once application credentials to access the Key Vault resource.

## AAD Application Authentication
This authentication method is fairly straightforward, and it is used in most of the examples in this documentation.

 * **KUSTO_AAD_APP_ID**: 
  'kustoAadAppId' - AAD application (client) identifier. 
  
 * **KUSTO_AAD_AUTHORITY_ID**: 
  'kustoAadAuthorityID' - AAD authentication authority. This is the AAD Directory (tenant) ID.
 
 * **KUSTO_AAD_APP_SECRET**: 
  'kustoAadAppSecret' - AAD application key for the client.
  
  * **KUSTO_AAD_APP_CERTIFICATE_PATH**: 
  'kutoAadAppCertPath' - AAD application pfx certificate path.
  
  * **KUSTO_AAD_APP_CERTIFICATE_PASSWORD**: 
  'kutoAadAppCertPassword' - AAD application certificate password.
  
  
 
 >**Note:** Older versions (less then 2.0.0) have the following naming: "kustoAADClientID", "kustoClientAADClientPassword", "kustoAADAuthorityID"
 
#### Example
```
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
## Key Vault

Kusto Spark Connector allows authentication using Azure Key Vault. The  Key Vault must contain the 
mandatory read/write authentication parameters. If a parameter appears in both the Key Vault and is passed directly as an option, the direct option will take precedence.
Although a combination of Key Vault and direct options is supported, for clarity, it is advised to 
pass authentication parameters using just one approach, and not both.

>**Note:** when working with a Databricks notebook, azure-keyvault package must be installed.
For details, refer to [Databricks documentation](https://docs.databricks.com/user-guide/libraries.html#maven-or-spark-package). 
                                                           
* **KEY_VAULT_URI**
 'keyVaultUri' - URI to the Key vault
 
 * **KEY_VAULT_APP_ID**
 'keyVaultAppId' - AAD application identifier that has access to 'get' and 'list' secrets from the vault.

 * **KEY_VAULT_APP_KEY**
 'keyVaultAppKey' - AAD application key for the application.
                                                                                             
**The connector will look for the following secret names:**
![Import libraries](KeyVaultSecrets.png)
### Kusto Cluster Authentication 
 * **kustoAppId**
 AAD application (client) identifier.
 
 * **kustoAppKey**
 AAD application key for the client.

 * **kustoAppAuthority**
  AAD authentication authority. This is the AAD Directory (tenant) ID.

### Transient Storage Parameters

>**Note:** these parameters are required when the connector is expected to read large amounts of data. 
This is a temporary requirement - future versions will be able to provision blob storage internally.

 * **blobStorageAccountName**
 Transient storage account name. Either this, or a SAS URL, must be provided to access the storage account

 * **blobStorageAccountKey**
 Storage account key. Either this, or a SAS URL, must be provided to access the storage account

* **blobStorageSasUrl**
 SAS URL: a complete SAS URL to access the container. Either this, or a storage account name and key, 
 must be provided to access the storage account
    
 * **blobContainer**
 Blob container name. This container will be used to store all transient artifacts created every time the corresponding RDD is materialized. 
 Once the RDD is no longer required by the caller application, the container and/or all its contents can be deleted by the caller.

### Key Vault Authentication Example

```
val keyVaultAppId: String = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
val keyVaultAppKey: String = "MyPassword"
val keyVaultUri: String = "keyVaultUri" 
 
df.write
  .format("com.microsoft.kusto.spark.datasource")
  .option(KustoSinkOptions.KUSTO_CLUSTER, MyCluster)
  .option(KustoSinkOptions.KUSTO_DATABASE, MyDatabase)
  .option(KustoSinkOptions.KUSTO_TABLE, MyTable)
  .option(KustoSinkOptions.KEY_VAULT_URI, keyVaultUri)
  .option(KustoSinkOptions.KEY_VAULT_APP_ID, keyVaultAppId)
  .option(KustoSinkOptions.KEY_VAULT_APP_KEY, keyVaultAppKey)
  .mode(SaveMode.Append)
  .save()

val conf: Map[String, String] = Map(
  KustoSourceOptions.KEY_VAULT_URI -> keyVaultUri,
  KustoSourceOptions.KEY_VAULT_APP_ID -> keyVaultAppId,
  KustoSourceOptions.KEY_VAULT_APP_KEY -> keyVaultAppKey
)

val query = table
val dfResult = spark.read.kusto(cluster, database, query, conf)
 ```
## Direct Authentication with Access Token
User can also use ADAL directly to acquire an AAD access token to access Kusto. 
The token must be valid throughout the duration of the read/write operation

 * **KUSTO_ACCESS_TOKEN**: 
    'accessToken' - The AAD access token
```
df.write
  .format("com.microsoft.kusto.spark.datasource")
  .option(KustoSinkOptions.KUSTO_CLUSTER, "MyCluster")
  .option(KustoSinkOptions.KUSTO_DATABASE, "MyDatabase")
  .option(KustoSinkOptions.KUSTO_TABLE, "MyTable")
  .option(KustoSinkOptions.KUSTO_ACCESS_TOKEN, "MyAadToken")
  .option(KustoOptions., "MyTable")
  .mode(SaveMode.Append)
  .save()
```

## Pfx certificate based AAD App Authentication with Access Token
User can also use AAD App authenticate with pfx certificate 

 * **KUSTO_AAD_APP_ID**: 
   'kustoAadAppId' - AAD application (client) identifier. 
    
 * **KUSTO_AAD_APP_CERTIFICATE_PATH**: 
    'kutoAadAppCertPath' - AAD application pfx certificate path.
    
 * **KUSTO_AAD_APP_CERTIFICATE_PASSWORD**: 
    'kutoAadAppCertPassword' - AAD application certificate password.
   
```
df.write
  .format("com.microsoft.kusto.spark.datasource")
  .option(KustoSinkOptions.KUSTO_CLUSTER, "MyCluster")
  .option(KustoSinkOptions.KUSTO_DATABASE, "MyDatabase")
  .option(KustoSinkOptions.KUSTO_AAD_APP_ID, "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
  .option(KustoSinkOptions.KUSTO_AAD_APP_CERTIFICATE_PATH, "absolute path to pfx cert") 
  .option(KustoSinkOptions.KUSTO_AAD_APP_CERTIFICATE_PASSWORD, "cert password if it is protected")
  .option(KustoOptions., "MyTable")
  .mode(SaveMode.Append)
  .save()
```

## Device Authentication
If no authentication parameters were passed, the connector will request for user authentication by writing a token 
to the console. This token can be used to authenticate at https://login.microsoftonline.com/common/oauth2/deviceauth 
and will allow temporary access (about one hour). For Python, use this [sample](../samples/src/main/python/pyKusto.py).
The user needs appropriate privileges for the Kusto cluster as explained in [Kusto Sink authentication section](KustoSink.md#authentication).

### Device Authentication for PySpark
Please refer to the [Python samples](../samples/src/main/python/pyKusto.py).

>**Note:** Device authentication is not recommended for production

## Authentication via token provider callback

User can provide a class to be used as a callback for providing an AAD authentication token. 
The class should have a constructor that accepts one argument of type CaseInsensitiveMap[String] (which will contain
the options provided to the Connector). The class should extend Callable<String> with Serializeable and is expected
to return an AAD token upon invoking the call method.
The provider will be called for every request to the kusto service

* **KUSTO_TOKEN_PROVIDER_CALLBACK_CLASSPATH**: 
    'tokenProviderCallbackClasspath' - The classpath to the class
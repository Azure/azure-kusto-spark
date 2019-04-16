/***************************************************************************************/
/*   This code is intended for reference only.                                         */
/*   It was tested on an Azure DataBricks cluster using Databricks Runtime Version 5.0 */
/*   (Apache Spark 2.4.0, Scala 2.11)                                                  */
/***************************************************************************************/

import java.util.concurrent.atomic.AtomicInteger

import com.microsoft.kusto.spark.datasource.KustoOptions
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}

object KustoConnectorDemo {
  def main(args: Array[String]): Unit = {

    // Note! This command is not required if you run in a Databricks notebook
    val spark: SparkSession = SparkSession.builder()
      .appName("KustoSink")
      .master(f"local[4]")
      .getOrCreate()

    // BASIC SETUP  ----------
    // Set logging level. Logs are available via databricks visualization    
    KDSU.setLoggingLevel("all")

    // Get the application Id and Key from the secret store. Scope must be pre-configured in Databricks. For details, refer to https://docs.azuredatabricks.net/user-guide/secrets/index.html#secrets-user-guide
    // The application must be registered in Azure Active Directory (AAD)
    // It must also have proper privileges on the target database and table
    val KustoSparkTestAppId = "Your AAD Application Id" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "KustoSparkTestAppId")
    val KustoSparkTestAppKey = "Your AAD Application Key" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "KustoSparkTestAppKey")

    // SETUP SINK CONNECTION PARAMETERS ----------
    // Set up sink connection parameters
    val appId= KustoSparkTestAppId
    val appKey=KustoSparkTestAppKey
    val authorityId = "Your AAD authority id. A.K.A. directory/tenant id when registering an AAD client" // For microsoft applications, typically "72f988bf-86f1-41af-91ab-2d7cd011db47"
    val cluster = "Your Cluster Name"
    val database = "Your Database Name"
    val table = "Your Kusto Table Name"

    // GENERATE DATA TO WRITE ----------
    val rowId = new AtomicInteger(1)
    def newRow(): String = s"row-${rowId.getAndIncrement()}"

    // Create 8KB rows, each containing a String and an Integer (change this size per intended test scenario)
    val rows = (1 to 8 * 1024).map(v => (newRow(), v))

    import spark.implicits._
    val df = rows.toDF("name", "value")

    // BATCH SINK (WRITE)
    // Write the data to a Kusto cluster, synchronously
    // To see how the number of partitions effect the command performance, use 'repartition': e.g. 'df.repartition(16).write...'
    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoOptions.KUSTO_CLUSTER, cluster)
      .option(KustoOptions.KUSTO_DATABASE, database)
      .option(KustoOptions.KUSTO_TABLE, table)
      .option(KustoOptions.KUSTO_AAD_CLIENT_ID, appId)
      .option(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, appKey)
      .option(KustoOptions.KUSTO_AAD_AUTHORITY_ID, authorityId)
      .save()

      // To write the data to a Kusto cluster, ASYNCronously, add:
      // " .option(KustoOptions.KUSTO_WRITE_ENABLE_ASYNC, true) "
      // The driver will return quickly, and will complete the operation asynchronously once the workers end ingestion to Kusto.
      // However exceptions are not captured in the driver, and tracking command success/failure status is not straightforward as in synchronous mode
    

    // PREPARE A DATA FRAME FOR STREAMING SINK (WRITE)
    import org.apache.spark.sql.types.DataTypes.IntegerType
    val customSchema = new StructType().add("colA", StringType, nullable = true).add("colB", IntegerType, nullable = true)

    // Read data from file to a stream 
    val csvDf = spark
          .readStream      
          .schema(customSchema)
          .csv("/FileStore/tables")

    // PERFORM STREAMING WRITE
    import java.util.concurrent.TimeUnit

    import org.apache.spark.sql.streaming.Trigger

    // Set up a checkpoint and disable codeGen
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/FileStore/temp/checkpoint")
    spark.conf.set("spark.sql.codegen.wholeStage","false")

    // Write to a Kusto table fro streaming source
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

      val conf: Map[String, String] = Map(
      KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
      KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
      KustoOptions.KUSTO_QUERY -> s"$table | where (ColB % 1000 == 0) | distinct ColA"
    )

    // Simplified syntax flavour
    import com.microsoft.kusto.spark.sql.extension.SparkExtension._
    val df2 = spark.read.kusto(cluster, database, "", conf)

    // ELABORATE READ SYNTAX
    // Here we read the whole table.
    val conf2: Map[String, String] = Map(
          KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
          KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
          KustoOptions.KUSTO_QUERY -> "StringAndIntTable"
        )

        val df3 = spark.sqlContext.read
          .format("com.microsoft.kusto.spark.datasource")
          .option(KustoOptions.KUSTO_CLUSTER, cluster)
          .option(KustoOptions.KUSTO_DATABASE, database)
          .options(conf2)
          .load()

    // GET STORAGE PARAMETERS FOR READING A LARGE DATA SET
    // NOTE: this is a temporary requirement - future connector versions will provision storage internally
    // Use either container/account-key/account name, or container SaS
    val container = "Your container name" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "blobContainer")
    val storageAccountKey = "Your storage account key" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "blobStorageAccountKey")
    val storageAccountName = "Your storage account name" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "blobStorageAccountName")
    // Note: alternatively, provide just the container SAS.
    // val storageSas = "Your container SAS" //Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "blobStorageSasUrl")


    // SET UP AZURE FS CONFIGURATION FOR BLOB ACCESS
    // Note: alternatively, ask the connector to setup FS configuration on every read access by setting:
    // KustoOptions.KUSTO_BLOB_SET_FS_CONFIG -> "true"

    // When using storage account key
    spark.conf.set(s"fs.azure.account.key.$storageAccountName.blob.core.windows.net", s"$storageAccountKey")
    // when using SAS
    // spark.conf.set(s"fs.azure.sas.$container.$storageAccountName.blob.core.windows.net", s"$storageSas")

    // READING LARGE DATA SET: SET UP CONFIGURATION
    val conf3: Map[String, String] = Map(
      KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
      KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
      KustoOptions.KUSTO_BLOB_CONTAINER -> container,
      //KustoOptions.KUSTO_BLOB_STORAGE_SAS_URL -> storageSas,
      KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_NAME -> storageAccountName,
      KustoOptions.KUSTO_BLOB_STORAGE_ACCOUNT_KEY -> storageAccountKey
    )

    // READING LARGE DATA SET(FULL TABLE, UNFILTERED)
    val query = "ReallyBigTable"
    val df4 = spark.read.kusto(cluster, database, query, conf3)

    // READING LARGE DATA SET (WITH PRUNING AND FILTERING)
    val df5 = spark.read.kusto(cluster, database, query, conf3)

    val dfFiltered = df5
      .where(df5.col("ColA").startsWith("row-2"))
      .filter("ColB > 12")
      .filter("ColB <= 21")
      .select("ColA")

    // READING USING KEY VAULT ACCESS
    // There are two different approaches to use parameters stored in Azure Key Vault for Kusto connector:
    // 1. One can use DataBricks KV-assisted store under a DataBricks secrete scope (see https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html#akv-ss). This is how keyVaultClientPassword is stored.
    // 2. Some parameters, including all secretes required for Kusto connector operations, can be accessed as described in https://github.com/Azure/azure-kusto-spark/blob/dev/docs/Authentication.md
    // This requires accessing the KV with the three parameters below
    val keyVaultClientPassword = "Password of the AAD client used to identify to your key vault"//Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "keyVaultClientPassword")
    val keyVaultClientID = "The client id of the AAD client used to identify to your key vault"
    val keyVaultUri = "https://<Your key vault name>.vault.azure.net"

    /************************************************************************************************************/
    /* The following parameters are taken from Key Vault using connector Key Vault access                       */
    /* blobContainer, blobStorageAccountKey, blobStorageAccountName, kustoAppAuthority, kustoAppId, kustoAppKey */
    /************************************************************************************************************/
    val conf4: Map[String, String] = Map(
      KustoOptions.KEY_VAULT_URI -> keyVaultUri,
      KustoOptions.KEY_VAULT_APP_ID -> keyVaultClientID,
      KustoOptions.KEY_VAULT_APP_KEY -> keyVaultClientPassword,
      KustoOptions.KUSTO_QUERY -> "StringAndIntExpTable"
    )

    val df6 =
    spark.sqlContext.read
    .format("com.microsoft.kusto.spark.datasource")
    .option(KustoOptions.KUSTO_CLUSTER, cluster)
    .option(KustoOptions.KUSTO_DATABASE, "ExperimentDb")
    .options(conf4)
    .load()
  }
}

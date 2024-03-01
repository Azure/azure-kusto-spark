//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

/**
 * ************************************************************************************
 */
/*   This code is intended for reference only.                                         */
/*   It was tested on an Azure DataBricks cluster using Databricks Runtime Version 5.0 */
/*   (Apache Spark 2.4.0, Scala 2.11)                                                  */
/**
 * ************************************************************************************
 */

import java.util.concurrent.atomic.AtomicInteger

import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.datasource.{
  KustoSourceOptions,
  TransientStorageCredentials,
  TransientStorageParameters
}
import com.microsoft.kusto.spark.sql.extension.SparkExtension.DataFrameReaderExtension
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

object KustoConnectorDemo {
  def main(args: Array[String]): Unit = {

    // Note! This command is not required if you run in a Databricks notebook
    val spark: SparkSession = SparkSession
      .builder()
      .appName("KustoSink")
      .master(f"local[4]")
      .getOrCreate()

    // BASIC SETUP  ----------
    // Set logging level. Logs are available via databricks visualization
    KDSU.setLoggingLevel("all")

    // Get the application Id and Key from the secret store. Scope must be pre-configured in Databricks. For details, refer to https://docs.azuredatabricks.net/user-guide/secrets/index.html#secrets-user-guide
    // The application must be registered in Azure Active Directory (AAD)
    // It must also have proper privileges on the target database and table
    val KustoSparkTestAppId =
      "Your AAD Application Id" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "KustoSparkTestAppId")
    val KustoSparkTestAppKey =
      "Your AAD Application Key" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "KustoSparkTestAppKey")

    // SETUP SINK CONNECTION PARAMETERS ----------
    // Set up sink connection parameters
    val appId = KustoSparkTestAppId
    val appKey = KustoSparkTestAppKey
    val authorityId =
      "Your AAD authority id. A.K.A. directory/tenant id when registering an AAD client" // For microsoft applications, typically "72f988bf-86f1-41af-91ab-2d7cd011db47"
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
      .option(KustoSinkOptions.KUSTO_CLUSTER, cluster)
      .option(KustoSinkOptions.KUSTO_DATABASE, database)
      .option(KustoSinkOptions.KUSTO_TABLE, table)
      .option(KustoSinkOptions.KUSTO_AAD_APP_ID, appId)
      .option(KustoSinkOptions.KUSTO_AAD_APP_SECRET, appKey)
      .option(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, authorityId)
      .mode(SaveMode.Append)
      .save()

    // To write the data to a Kusto cluster, asynchronously, add:
    // " .option(KustoOptions.KUSTO_WRITE_ENABLE_ASYNC, true) "
    // The driver will return quickly, and will complete the operation asynchronously once the workers end ingestion to Kusto.
    // However exceptions are not captured in the driver, and tracking command success/failure status is not straightforward as in synchronous mode

    // PREPARE A DATA FRAME FOR STREAMING SINK (WRITE)
    import org.apache.spark.sql.types.DataTypes.IntegerType
    val customSchema = new StructType()
      .add("colA", StringType, nullable = true)
      .add("colB", IntegerType, nullable = true)

    // Read data from file to a stream
    val csvDf = spark.readStream
      .schema(customSchema)
      .csv("Samples/FileStore/tables")

    // PERFORM STREAMING WRITE
    import java.util.concurrent.TimeUnit

    import org.apache.spark.sql.streaming.Trigger

    // Set up a checkpoint
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/FileStore/temp/checkpoint")

    // Write to a Kusto table from streaming source
    val kustoQ = csvDf.writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .options(Map(
        KustoSinkOptions.KUSTO_CLUSTER -> cluster,
        KustoSinkOptions.KUSTO_TABLE -> table,
        KustoSinkOptions.KUSTO_DATABASE -> database,
        KustoSinkOptions.KUSTO_AAD_APP_ID -> appId,
        KustoSinkOptions.KUSTO_AAD_APP_SECRET -> appKey,
        KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID -> authorityId,
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS -> "CreateIfNotExist"))
      .trigger(Trigger.Once)

    // On databricks - this connection will hold without awaitTermination call
    kustoQ.start().awaitTermination(TimeUnit.MINUTES.toMillis(8))

    val conf: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
      KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID -> authorityId,
      KustoSourceOptions.KUSTO_QUERY -> s"$table | where colB % 50 == 0 | distinct colA")

    // Simplified syntax flavour
    import com.microsoft.kusto.spark.sql.extension.SparkExtension._
    val df2 = spark.read.kusto(cluster, database, query = "", conf)

    // ELABORATE READ SYNTAX
    // Here we read the whole table.
    val conf2: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
      KustoSourceOptions.KUSTO_QUERY -> "StringAndIntTable")

    val df3 = spark.sqlContext.read
      .format("com.microsoft.kusto.spark.datasource")
      .option(KustoSourceOptions.KUSTO_CLUSTER, cluster)
      .option(KustoSourceOptions.KUSTO_DATABASE, database)
      .options(conf2)
      .load()

    // OPTIONAL:
    // PROVIDE STORAGE PARAMETERS YOURSELF FOR READING A LARGE DATA SET
    // NOTE: this is not required as the connector will alternatively get temporary storage itself
    // Use either container/account-key/account name, or container SaS
    val container =
      "Your container name" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "blobContainer")
    val storageAccountKey =
      "Your storage account key" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "blobStorageAccountKey")
    val storageAccountName =
      "Your storage account name" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "blobStorageAccountName")
    // Note: alternatively, provide just the container SAS.
    // val storageSas = "Your container SAS" //Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "blobStorageSasUrl")

    // When using storage account key
    spark.conf.set(
      s"fs.azure.account.key.$storageAccountName.blob.core.windows.net",
      s"$storageAccountKey")
    // when using SAS
    // spark.conf.set(s"fs.azure.sas.$container.$storageAccountName.blob.core.windows.net", s"$storageSas")

    val st = new TransientStorageParameters(
      Array(new TransientStorageCredentials(storageAccountName, storageAccountKey, container)))
    // SET UP CONFIGURATION FOR PROVIDING STORAGE YOURSELF
    val conf3: Map[String, String] = Map(
      KustoSourceOptions.KUSTO_AAD_APP_ID -> appId,
      KustoSourceOptions.KUSTO_AAD_APP_SECRET -> appKey,
      KustoSourceOptions.KUSTO_TRANSIENT_STORAGE -> st.toString)

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
    // 1. One can use DataBricks KV-assisted store under a DataBricks secret scope (see https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html#akv-ss). This is how keyVaultAppKey is stored.
    // 2. Some parameters, including all secrets required for Kusto connector operations, can be accessed as described in https://github.com/Azure/azure-kusto-spark/blob/dev/docs/Authentication.md
    // This requires accessing the KV with the three parameters below
    val keyVaultAppKey =
      "Password of the AAD client used to identify to your key vault" // Databricks example: dbutils.secrets.get(scope = "KustoDemos", key = "keyVaultAppKey")
    val keyVaultAppId = "The client id of the AAD client used to identify to your key vault"
    val keyVaultUri = "https://<Your key vault name>.vault.azure.net"

    /**
     * *********************************************************************************************************
     */
    /* The following parameters are taken from Key Vault using connector Key Vault access                       */
    /* blobContainer, blobStorageAccountKey, blobStorageAccountName, kustoAppAuthority, kustoAppId, kustoAppKey */
    /**
     * *********************************************************************************************************
     */
    val conf4: Map[String, String] = Map(
      KustoSourceOptions.KEY_VAULT_URI -> keyVaultUri,
      KustoSourceOptions.KEY_VAULT_APP_ID -> keyVaultAppId,
      KustoSourceOptions.KEY_VAULT_APP_KEY -> keyVaultAppKey,
      KustoSourceOptions.KUSTO_QUERY -> "StringAndIntExpTable")

    val df6 =
      spark.sqlContext.read
        .format("com.microsoft.kusto.spark.datasource")
        .option(KustoSourceOptions.KUSTO_CLUSTER, cluster)
        .option(KustoSourceOptions.KUSTO_DATABASE, "ExperimentDb")
        .options(conf4)
        .load()
  }
}

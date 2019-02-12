package com.microsoft.kusto.spark

import java.util.UUID

import com.microsoft.azure.kusto.data.{ClientFactory, ConnectionStringBuilder}
import com.microsoft.kusto.spark.datasource.KustoOptions
import com.microsoft.kusto.spark.datasource.KustoOptions.SinkTableCreationMode
import com.microsoft.kusto.spark.utils.KustoQueryUtils
import org.apache.spark.sql.SparkSession

import scala.collection.immutable
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class keyVaultAuthenticationTest extends FlatSpec{
  private val spark: SparkSession = SparkSession.builder()
    .appName("KustoSink")
    .master(f"local[2]")
    .getOrCreate()

  val cluster: String = System.getProperty(KustoOptions.KUSTO_CLUSTER)
  val database: String = System.getProperty(KustoOptions.KUSTO_DATABASE)

  val appId: String = System.getProperty(KustoOptions.KUSTO_AAD_CLIENT_ID)
  val appKey: String = System.getProperty(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD)
  val authority: String = System.getProperty(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com")

  val keyVaultClientID: String = System.getProperty(KustoOptions.KEY_VAULT_APP_ID)
  val keyVaultClientPassword: String = System.getProperty(KustoOptions.KEY_VAULT_APP_KEY)
  val keyVaultUri: String = System.getProperty(KustoOptions.KEY_VAULT_URI)

  "keyVaultAuthentication " should "export data to a blob and read it to a dataframe" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 1000
    val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes

    val rows: immutable.IndexedSeq[(String, Int)] = (1 to expectedNumberOfRows).map(v=>(s"row-$v", v))
    val prefix = "keyVaultAuthentication"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    val df = rows.toDF("name", "value")

    df.write
      .format("com.microsoft.kusto.spark.datasource")
      .partitionBy("value")
      .option(KustoOptions.KUSTO_CLUSTER, cluster)
      .option(KustoOptions.KUSTO_DATABASE, database)
      .option(KustoOptions.KUSTO_TABLE, table)
      .option(KustoOptions.KEY_VAULT_URI, keyVaultUri)
      .option(KustoOptions.KEY_VAULT_APP_ID, keyVaultClientID)
      .option(KustoOptions.KEY_VAULT_APP_KEY, keyVaultClientPassword)
      .option(KustoOptions.KUSTO_TABLE_CREATE_OPTIONS, SinkTableCreationMode.CreateIfNotExist.toString)
      .save()

    KustoTestUtils.validateResultsAndCleanup(kustoAdminClient, table, database, expectedNumberOfRows, timeoutMs, tableCleanupPrefix = prefix)

  }
}

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

package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.datasink.{KustoSinkOptions, SinkTableCreationMode}
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import com.microsoft.kusto.spark.utils.KustoQueryUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import scala.collection.immutable

class KustoAuthenticationTestE2E extends AnyFlatSpec {
  private val spark: SparkSession = SparkSession
    .builder()
    .appName("KustoSink")
    .master(f"local[2]")
    .getOrCreate()

  val cluster: String = System.getProperty(KustoSinkOptions.KUSTO_CLUSTER)
  val database: String = System.getProperty(KustoSinkOptions.KUSTO_DATABASE)

  val appId: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_ID)
  val appKey: String = System.getProperty(KustoSinkOptions.KUSTO_AAD_APP_SECRET)
  val authority: String =
    System.getProperty(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com")

  val keyVaultAppId: String = System.getProperty(KustoSinkOptions.KEY_VAULT_APP_ID)
  val keyVaultAppKey: String = System.getProperty(KustoSinkOptions.KEY_VAULT_APP_KEY)
  val keyVaultUri: String = System.getProperty(KustoSinkOptions.KEY_VAULT_URI)

  "keyVaultAuthentication" should "use key vault for authentication and retracting kusto app auth params" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 1000
    val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes

    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (s"row-$v", v))
    val prefix = "keyVaultAuthentication"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://$cluster.kusto.windows.net",
      appId,
      appKey,
      authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    val df = rows.toDF("name", "value")
    val conf: Map[String, String] = Map(
      KustoSinkOptions.KEY_VAULT_URI -> keyVaultUri,
      KustoSinkOptions.KEY_VAULT_APP_ID -> (if (keyVaultAppId == null) appId else keyVaultAppId),
      KustoSinkOptions.KEY_VAULT_APP_KEY -> (if (keyVaultAppKey == null) appKey
                                             else keyVaultAppKey),
      KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS -> SinkTableCreationMode.CreateIfNotExist.toString)

    df.write.kusto(cluster, database, table, conf)

    val dfResult = spark.read.kusto(cluster, database, table, conf)
    val result = dfResult.select("name", "value").rdd.collect().sortBy(x => x.getInt(1))
    val orig = df.select("name", "value").rdd.collect().sortBy(x => x.getInt(1))

    assert(result.diff(orig).isEmpty)
  }

  "managedIdentityAuthentication" should "use managed resource for authentication" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 1000

    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (s"row-$v", v))
    val prefix = "managedIdentityAuth"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")
    val engineKcsb =
      ConnectionStringBuilder.createWithAadManagedIdentity(s"https://$cluster.kusto.windows.net")
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    val df = rows.toDF("name", "value")
    val conf: Map[String, String] =
      Map(KustoSinkOptions.KUSTO_MANAGED_IDENTITY_AUTH -> true.toString)

    df.write.kusto(cluster, database, table, conf)

    val dfResult = spark.read.kusto(cluster, database, table, conf)
    val result = dfResult.select("name", "value").rdd.collect().sortBy(x => x.getInt(1))
    val orig = df.select("name", "value").rdd.collect().sortBy(x => x.getInt(1))

    assert(result.diff(orig).isEmpty)
  }

  "deviceAuthentication" should "use aad device authentication" taggedAs KustoE2E in {
    import spark.implicits._
    val expectedNumberOfRows = 1000
    val timeoutMs: Int = 8 * 60 * 1000 // 8 minutes

    val rows: immutable.IndexedSeq[(String, Int)] =
      (1 to expectedNumberOfRows).map(v => (s"row-$v", v))
    val prefix = "deviceAuthentication"
    val table = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")

    val deviceAuth = new com.microsoft.kusto.spark.authentication.DeviceAuthentication(
      s"https://${cluster}.kusto.windows.net",
      authority)

    val token = deviceAuth.acquireToken()

    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      s"https://$cluster.kusto.windows.net",
      token)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)

    val df = rows.toDF("name", "value")
    val conf: Map[String, String] = Map(
      KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS -> SinkTableCreationMode.CreateIfNotExist.toString)

    df.write.kusto(cluster, database, table, conf)

    KustoTestUtils.validateResultsAndCleanup(
      kustoAdminClient,
      table,
      database,
      expectedNumberOfRows,
      timeoutMs,
      tableCleanupPrefix = prefix)
  }
}

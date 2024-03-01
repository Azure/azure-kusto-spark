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
import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.datasource.{
  KustoResponseDeserializer,
  KustoSourceOptions,
  TransientStorageCredentials,
  TransientStorageParameters
}
import com.microsoft.kusto.spark.sql.extension.SparkExtension._
import com.microsoft.kusto.spark.utils.{
  CslCommandsGenerator,
  KustoBlobStorageUtils,
  KustoQueryUtils,
  KustoDataSourceUtils => KDSU
}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.security.InvalidParameterException
import java.util.UUID
import scala.collection.JavaConverters._

class KustoBlobAccessE2E extends AnyFlatSpec with BeforeAndAfterAll {
  private val myName = this.getClass.getSimpleName

  private val nofExecutors = 4
  private val spark: SparkSession = SparkSession
    .builder()
    .appName("KustoSink")
    .master(f"local[$nofExecutors]")
    .getOrCreate()

  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    sc = spark.sparkContext
    sqlContext = spark.sqlContext
  }

  override def afterAll(): Unit = {
    super.afterAll()

    sc.stop()
  }

  val appId: String = System.getProperty(KustoSourceOptions.KUSTO_AAD_APP_ID)
  val appKey: String = System.getProperty(KustoSourceOptions.KUSTO_AAD_APP_SECRET)
  val authority: String = System.getProperty(KustoSourceOptions.KUSTO_AAD_AUTHORITY_ID)
  val cluster: String = System.getProperty(KustoSourceOptions.KUSTO_CLUSTER)
  val database: String = System.getProperty(KustoSourceOptions.KUSTO_DATABASE)
  val table: String = System.getProperty(KustoSinkOptions.KUSTO_TABLE, "")
  val storageAccount: String = System.getProperty("storageAccount", "sparkblobforkustomichael")
  val container: String = System.getProperty("container", "CONTAINER")
  val blobKey: String = System.getProperty("blobKey", "KEY")
  val blobSas: String = System.getProperty("blobSas")

  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  def updateKustoTable(tableName: String): String = {
    var updatedTable = table

    if (updatedTable.isEmpty) {
      val prefix = "KustoBatchSinkE2EIngestAsync"
      updatedTable = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")

      import java.util.concurrent.atomic.AtomicInteger
      val rowId = new AtomicInteger(1)
      def newRow(): String = s"row-${rowId.getAndIncrement()}"
      // Create 1000 rows, each containing a String and an Integer (change this size per intended test scenario)
      val rows = (1 to 1000).map(v => (newRow(), v))

      import spark.implicits._
      val df = rows.toDF("name", "value")

      val conf = Map(
        KustoSinkOptions.KUSTO_AAD_APP_ID -> appId,
        KustoSinkOptions.KUSTO_AAD_APP_SECRET -> appKey,
        KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID -> authority,
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS -> "CreateIfNotExist")

      df.write.kusto(cluster, database, updatedTable, conf)
    }

    updatedTable
  }

  "KustoCreateBlobFile" should "export data to a blob and read it to a dataframe" taggedAs KustoE2E in {
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
      s"https://$cluster.kusto.windows.net",
      appId,
      appKey,
      authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    val myTable = updateKustoTable(table)
    val schema = KustoResponseDeserializer(
      kustoAdminClient
        .execute(database, KustoQueryUtils.getQuerySchemaQuery(myTable))
        .getPrimaryResults).getSchema

    val firstColumn =
      if (schema.sparkSchema.nonEmpty) {
        schema.sparkSchema.head.name
      } else {
        throw new RuntimeException(
          s"Failed to read schema for myTable $myTable in cluster $cluster, database $database")
      }

    var secret = ""

    if (blobSas != null) {
      val storageParams = new datasource.TransientStorageCredentials(blobSas)
      secret = storageParams.sasKey
    } else {
      secret = if (blobSas != null) blobSas else blobKey
    }

    val numberOfPartitions = 10
    val partitionId = 0
    val partitionPredicate = s" hash($firstColumn, $numberOfPartitions) == $partitionId"
    val useKeyNotSas = blobSas == null

    val (_, directory: String, _) =
      getBlobCoordinates(storageAccount, container, secret, useKeyNotSas)

    val exportCommand = CslCommandsGenerator.generateExportDataCommand(
      myTable,
      directory,
      partitionId,
      new TransientStorageParameters(
        Array(new TransientStorageCredentials(storageAccount, secret, container))),
      Some(partitionPredicate))

    val blobs = kustoAdminClient
      .execute(database, exportCommand)
      .getPrimaryResults
      .getData
      .asScala
      .map(row => row.get(0))

    blobs.foreach(blob => KDSU.logInfo(myName, s"Exported to blob: $blob"))
    if (useKeyNotSas) {
      spark.conf.set(s"fs.azure.account.key.$storageAccount.blob.core.windows.net", s"$secret")
    } else {
      if (blobSas.isEmpty) {
        throw new InvalidParameterException(
          "Please provide a complete query string of your SaS as a container when accessing blob storage with SaS key")
      }
      spark.conf.set(s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net", s"$secret")
    }
    spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")

    val df =
      spark.read.parquet(s"wasbs://$container@$storageAccount.blob.core.windows.net/$directory")

    var rowAsString: String = ""
    var rowsProcessed = 0
    val pattern = """\[row-([0-9]+),([0-9]+)]""".r

    df.take(10)
      .foreach(row => {
        rowAsString = row.toString(); rowsProcessed += 1;
        KDSU.logInfo(myName, s"row: $rowAsString")
        val pattern(rowInt, int) = rowAsString
        assert(rowInt == int)
      })

    KustoBlobStorageUtils.deleteFromBlob(
      storageAccount,
      directory,
      container,
      secret,
      !useKeyNotSas)
  }

  private def getBlobCoordinates(
      storageAccountName: String,
      container: String,
      secret: String,
      useKeyNotSas: Boolean): (String, String, String) = {
    val secretString =
      if (useKeyNotSas) s""";" h@"$secret""""
      else if (secret(0) == '?') s"""" h@"$secret""""
      else s"""?" h@"$secret""""
    val subDir = "dir" + UUID.randomUUID()
    val directory = KustoQueryUtils.simplifyName(s"$appId/$subDir/")
    val blobContainerUri = s"https://$storageAccountName.blob.core.windows.net/" + container

    (blobContainerUri, directory, secretString)
  }
}

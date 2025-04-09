// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.kusto.spark.KustoTestUtils.getSystemTestOptions
import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.datasource.{
  KustoResponseDeserializer,
  KustoSourceOptions,
  TransientStorageCredentials,
  TransientStorageParameters
}
import com.microsoft.kusto.spark.sql.extension.SparkExtension._

import java.util.concurrent.atomic.AtomicInteger
import com.microsoft.kusto.spark.utils.KustoQueryUtils.getQuerySchemaQuery
import com.microsoft.kusto.spark.utils.{
  CslCommandsGenerator,
  KustoBlobStorageUtils,
  KustoQueryUtils,
  KustoDataSourceUtils => KDSU
}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, ParallelTestExecution}
import org.scalatest.flatspec.AnyFlatSpec

import java.security.InvalidParameterException
import java.util.UUID
import scala.collection.JavaConverters._

class KustoBlobAccessE2E extends AnyFlatSpec with BeforeAndAfterAll with ParallelTestExecution {
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
    // sc.stop()
  }

  private lazy val kustoTestConnectionOptions = getSystemTestOptions
  private val table: String = System.getProperty(KustoSinkOptions.KUSTO_TABLE, "")
  private val storageAccount: String =
    System.getProperty("storageAccount", "sparkblobforkustomichael")
  private val container: String = System.getProperty("container", "CONTAINER")
  private val blobKey: String = System.getProperty("blobKey", "KEY")
  private val blobSas: String = System.getProperty("blobSas")

  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  def updateKustoTable(): String = {
    var updatedTable = table

    if (updatedTable.isEmpty) {
      val prefix = "KustoBatchSinkE2EIngestAsync"
      updatedTable = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")

      val rowId = new AtomicInteger(1)
      def newRow(): String = s"row-${rowId.getAndIncrement()}"
      // Create 1000 rows, each containing a String and an Integer (change this size per intended test scenario)
      val rows = (1 to 1000).map(v => (newRow(), v))

      import spark.implicits._
      val df = rows.toDF("name", "value")

      val conf = Map(
        KustoSinkOptions.KUSTO_ACCESS_TOKEN -> kustoTestConnectionOptions.accessToken,
        KustoSinkOptions.KUSTO_TABLE_CREATE_OPTIONS -> "CreateIfNotExist")

      df.write.kusto(
        kustoTestConnectionOptions.cluster,
        kustoTestConnectionOptions.database,
        updatedTable,
        conf)
    }
    updatedTable
  }

  "KustoCreateBlobFile" should "export data to a blob and read it to a dataframe" taggedAs KustoE2E in {
    val engineKcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
      kustoTestConnectionOptions.cluster,
      kustoTestConnectionOptions.accessToken)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    val myTable = updateKustoTable()
    val schema = KustoResponseDeserializer(
      kustoAdminClient
        .execute(kustoTestConnectionOptions.database, getQuerySchemaQuery(myTable))
        .getPrimaryResults).getSchema

    val firstColumn =
      if (schema.sparkSchema.nonEmpty) {
        schema.sparkSchema.head.name
      } else {
        throw new RuntimeException(
          s"Failed to read schema for myTable $myTable in cluster " +
            s"${kustoTestConnectionOptions.cluster}, " +
            s"database ${kustoTestConnectionOptions.database}")
      }

    val secret = if (blobSas != null) {
      val storageParams = new datasource.TransientStorageCredentials(blobSas)
      storageParams.sasKey
    } else {
      if (blobSas != null) blobSas else blobKey
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
      .execute(kustoTestConnectionOptions.database, exportCommand)
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
        rowAsString = row.toString(); rowsProcessed += 1
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
      if (useKeyNotSas) {
        s""";" h@"$secret""""
      } else if (secret(0) == '?') {
        s"""" h@"$secret""""
      } else {
        s"""?" h@"$secret""""
      }
    val subDir = "dir" + UUID.randomUUID()
    val directory = KustoQueryUtils.simplifyName(s"${UUID.randomUUID()}/$subDir/")
    val blobContainerUri = s"https://$storageAccountName.blob.core.windows.net/" + container

    (blobContainerUri, directory, secretString)
  }
}

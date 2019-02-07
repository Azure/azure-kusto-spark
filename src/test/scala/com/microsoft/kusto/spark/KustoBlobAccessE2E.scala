package com.microsoft.kusto.spark

import java.security.InvalidParameterException
import java.util.UUID

import com.microsoft.azure.kusto.data.{ClientFactory, ConnectionStringBuilder}
import com.microsoft.kusto.spark.datasource.{KustoOptions, KustoResponseDeserializer}
import com.microsoft.kusto.spark.utils.{KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.collection.JavaConverters._


@RunWith(classOf[JUnitRunner])
class KustoBlobAccessE2E extends FlatSpec with BeforeAndAfterAll {
  private val myName = this.getClass.getSimpleName

  private val nofExecutors = 4
  private val spark: SparkSession = SparkSession.builder()
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


  val appId: String = System.getProperty(KustoOptions.KUSTO_AAD_CLIENT_ID)
  val appKey: String = System.getProperty(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD)
  val authority: String = System.getProperty(KustoOptions.KUSTO_AAD_AUTHORITY_ID)
  val cluster: String = System.getProperty(KustoOptions.KUSTO_CLUSTER)
  val database: String = System.getProperty(KustoOptions.KUSTO_DATABASE)
  val table: String = System.getProperty(KustoOptions.KUSTO_TABLE, "")
  val storageAccount: String = System.getProperty("storageAccount", "sparkblobforkustomichael")
  val container: String = System.getProperty("container", "CONTAINER")
  val blobKey: String = System.getProperty("blobKey", "KEY")
  val blobSas: String = System.getProperty("blobSas")
  val blobSasConnectionString: String = System.getProperty("blobSasQuery")

  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  def updateKustoTable(tableName: String): String = {
    var updatedTable = table

    if(updatedTable.isEmpty) {
      val prefix = "KustoBatchSinkE2EIngestAsync"
      updatedTable = KustoQueryUtils.simplifyName(s"${prefix}_${UUID.randomUUID()}")

      import java.util.concurrent.atomic.AtomicInteger
      val rowId = new AtomicInteger(1)
      def newRow(): String = s"row-${rowId.getAndIncrement()}"
      // Create 1000 rows, each containing a String and an Integer (change this size per intended test scenario)
      val rows = (1 to 1000).map(v => (newRow(), v))

      import spark.implicits._
      val df = rows.toDF("name", "value")

      df.write
        .format("com.microsoft.kusto.spark.datasource")
        .partitionBy("value")
        .option(KustoOptions.KUSTO_CLUSTER, cluster)
        .option(KustoOptions.KUSTO_DATABASE, database)
        .option(KustoOptions.KUSTO_TABLE, updatedTable)
        .option(KustoOptions.KUSTO_AAD_CLIENT_ID, appId)
        .option(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, appKey)
        .option(KustoOptions.KUSTO_AAD_AUTHORITY_ID, authority)
        .option(KustoOptions.KUSTO_TABLE_CREATE_OPTIONS, "CreateIfNotExist")
        .save()
    }

    updatedTable
  }

  "KustoCreateBlobFile" should "export data to a blob and read it to a dataframe" taggedAs KustoE2E in {
    val engineKcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authority)
    val kustoAdminClient = ClientFactory.createClient(engineKcsb)
    val myTable = updateKustoTable(table)
    val schema = KustoResponseDeserializer(kustoAdminClient.execute(database, KustoQueryUtils.getQuerySchemaQuery(myTable))).getSchema

    val firstColumn =
    if (schema.nonEmpty)
    {
      schema.head.name
    }
    else
    {
      throw new RuntimeException(s"Failed to read schema for myTable $myTable in cluster $cluster, database $database")
    }

    val secret = if (blobSas != null) blobSas else blobKey

    val numberOfPartitions = 10
    val partitionId = 0
    val partitionPredicate = s" hash($firstColumn, $numberOfPartitions) == $partitionId"
    val useKeyNotSas = blobSas == null

    val (exportCommand, directory) = generateExportDataCommand(
      myTable,
      storageAccount,
      container,
      secret,
      useKeyNotSas,
      partitionId,
      partitionPredicate)

    val blobs = kustoAdminClient.execute(database, exportCommand)
      .getValues.asScala
      .map(row => row.get(0))

    blobs.foreach(blob => KDSU.logInfo(myName, s"Exported to blob: $blob"))
    if (useKeyNotSas) {
      spark.conf.set(s"fs.azure.account.key.$storageAccount.blob.core.windows.net", s"$secret")
    }
    else {
      if (blobSasConnectionString.isEmpty) {
        throw new InvalidParameterException("Please provide a complete query string of your SaS as a container when accessing blob storage with SaS key")
      }
      spark.conf.set(s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net", s"$blobSasConnectionString")
    }
    spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")

    val df = spark.read.parquet(s"wasbs://$container@$storageAccount.blob.core.windows.net/$directory")

    var rowAsString: String = ""
    var rowsProcessed = 0
    val pattern = """\[row-([0-9]+),([0-9]+)]""".r

    df.take(10).foreach(row => {
      rowAsString = row.toString(); rowsProcessed += 1; KDSU.logInfo(myName, s"row: $rowAsString")
      val pattern(rowInt, int) = rowAsString
      assert(rowInt == int)
    })
  }

  def generateExportDataCommand(
                                 tableName: String,
                                 storageAccountName: String,
                                 container: String,
                                 secret: String,
                                 useKeyNotSas: Boolean = true,
                                 partitionId: Int,
                                 partitionPredicate: String = ""): (String, String) = {

    val secretString = if (useKeyNotSas) s""";" h@"$secret"""" else s"""?" h@"$secret""""
    val subDir = "dir" + UUID.randomUUID()
    val directory = KustoQueryUtils.simplifyName(s"$appId/$subDir/")
    val blobUri = s"https://$storageAccountName.blob.core.windows.net"

    var command = s""".export to parquet ("$blobUri/$container$secretString)""" +
    s""" with (namePrefix="${directory}part$partitionId", fileExtension=parquet) <| $tableName"""

    if (partitionPredicate.nonEmpty)
    {
      command += s" | where $partitionPredicate"
    }

    (command, directory)
  }

}
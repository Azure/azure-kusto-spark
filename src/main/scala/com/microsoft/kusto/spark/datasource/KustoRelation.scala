package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.Locale

import com.microsoft.azure.kusto.data.{ClientFactory, ConnectionStringBuilder}
import com.microsoft.kusto.spark.utils.KustoQueryUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

case class KustoRelation(cluster: String,
                         database: String,
                         appId: String,
                         appKey: String,
                         authorityId: String,
                         query: String,
                         isLeanMode: Boolean,
                         numPartitions: Int,
                         partitioningColumn: Option[String],
                         partitioningMode: Option[String],
                         customSchema: Option[String] = None,
                         storageAccount: Option[String] = None,
                         storageContainer: Option[String] = None,
                         storageAccountSecrete: Option[String] = None,
                         isStorageSecreteKeyNotSas: Boolean = true)
                        (@transient val sparkSession: SparkSession) extends BaseRelation with TableScan with Serializable {

  private val normalizedQuery = KustoQueryUtils.normalizeQuery(query)

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = {
    if (customSchema.isDefined) {
      StructType.fromDDL(customSchema.get)
    }
    else getSchema
  }

  override def buildScan(): RDD[Row] = {
    if (isLeanMode) {
      KustoReader.leanBuildScan(
        KustoReadRequest(sparkSession, schema, cluster, database, query, appId, appKey, authorityId)
      )
    } else {
      KustoReader.scaleBuildScan(
        KustoReadRequest(sparkSession, schema, cluster, database, query, appId, appKey, authorityId),
        getTransientStorageParameters(storageAccount, storageContainer, storageAccountSecrete, isStorageSecreteKeyNotSas),
        KustoPartitionInfo(numPartitions, getPartitioningColumn(partitioningColumn, isLeanMode), getPartitioningMode(partitioningMode))
      )
    }
  }

  private def getTransientStorageParameters(
                                             storageAccount: Option[String],
                                             storageContainer: Option[String],
                                             storageAccountSecrete: Option[String],
                                             isKeyNotSas: Boolean): KustoStorageParameters = {
    if (storageAccount.isEmpty) {
      throw new InvalidParameterException("Storage account name is empty. Reading in 'Scale' mode requires a transient blob storage")
    }

    if (storageContainer.isEmpty) {
      throw new InvalidParameterException("Storage container name is empty.")
    }

    if (storageAccountSecrete.isEmpty) {
      throw new InvalidParameterException("Storage account secrete is empty. Please provide a storage account key or a SAS key")
    }

    KustoStorageParameters(storageAccount.get, storageAccountSecrete.get, storageContainer.get, isKeyNotSas)
  }

  private def getSchema: StructType = {
    if (query.isEmpty) {
      throw new InvalidParameterException("Query is empty")
    }

    val getSchemaQuery = if (KustoQueryUtils.isQuery(query)) KustoQueryUtils.getQuerySchemaQuery(normalizedQuery) else ""
    if (getSchemaQuery.isEmpty) {
      throw new RuntimeException("Spark connector cannot run Kusto commands. Please provide a valid query")
    }

    val kustoConnectionString = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authorityId)
    KustoResponseDeserializer(ClientFactory.createClient(kustoConnectionString).execute(database, getSchemaQuery)).getSchema
  }

  private def getPartitioningColumn(partitioningColumn: Option[String], isLean: Boolean): String = {
    if (isLean) return ""

    if (partitioningColumn.isDefined) {
      val requestedColumn = partitioningColumn.get.toLowerCase(Locale.ROOT)
      if (!schema.contains(requestedColumn)) {
        throw new InvalidParameterException(
          s"Cannot partition by column '$requestedColumn' since it is not art of the query schema: ${sys.props("line.separator")}${schema.mkString(", ")}")
      }
      requestedColumn
    } else schema.head.name
  }

  private def getPartitioningMode(partitioningMode: Option[String]): String = {
    if (partitioningMode.isDefined) {
      val mode = partitioningMode.get.toLowerCase(Locale.ROOT)
      if (!KustoOptions.supportedPartitioningModes.contains(mode)) {
        throw new InvalidParameterException(
          s"Specified partitioning mode '$mode' : ${sys.props("line.separator")}${KustoOptions.supportedPartitioningModes.mkString(", ")}")
      }
      mode
    } else "hash"
  }
}
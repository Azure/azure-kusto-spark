package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.Locale

import com.microsoft.kusto.spark.utils.{KustoClient, KustoQueryUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

private[kusto] case class KustoRelation(kustoCoordinates: KustoCoordinates,
   authentication: KustoAuthentication,
   query: String,
   isLeanMode: Boolean,
   numPartitions: Int,
   partitioningColumn: Option[String],
   partitioningMode: Option[String],
   customSchema: Option[String] = None,
   storageParameters: Option[KustoStorageParameters])
  (@transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan with PrunedFilteredScan with Serializable {


  private val normalizedQuery = KustoQueryUtils.normalizeQuery(query)
  var cachedSchema: StructType = _
  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = {
    if(cachedSchema == null){
      cachedSchema =  if (customSchema.isDefined) {
        StructType.fromDDL(customSchema.get)
      }
      else getSchema
    }
    cachedSchema
  }

  override def buildScan(): RDD[Row] = {
    if (isLeanMode) {
      KustoReader.leanBuildScan(
        KustoReadRequest(sparkSession, schema, kustoCoordinates, query, authentication)
      )
    } else {
      KustoReader.scaleBuildScan(
        KustoReadRequest(sparkSession, schema, kustoCoordinates, query, authentication),
        storageParameters.get,
        KustoPartitionParameters(numPartitions, getPartitioningColumn, getPartitioningMode)
      )
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    if (isLeanMode) {
      KustoReader.leanBuildScan(
        KustoReadRequest(sparkSession, schema, kustoCoordinates, query, authentication),
        KustoFiltering(requiredColumns, filters)
      )
    } else {
      KustoReader.scaleBuildScan(
        KustoReadRequest(sparkSession, schema, kustoCoordinates, query, authentication),
        storageParameters.get,
        KustoPartitionParameters(numPartitions, getPartitioningColumn, getPartitioningMode),
        KustoFiltering(requiredColumns, filters)
      )
    }

  private def getTransientStorageParameters(storageAccount: Option[String],
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
    KustoResponseDeserializer(KustoClient.getAdmin(authentication, kustoCoordinates.cluster).execute(kustoCoordinates.database, getSchemaQuery)).getSchema
  }

  private def getPartitioningColumn: String = {
    if (partitioningColumn.isDefined) {
      val requestedColumn = partitioningColumn.get.toLowerCase(Locale.ROOT)
      if (!schema.contains(requestedColumn)) {
        throw new InvalidParameterException(
          s"Cannot partition by column '$requestedColumn' since it is not part of the query schema: ${sys.props("line.separator")}${schema.mkString(", ")}")
      }
      requestedColumn
    } else schema.head.name
  }

  private def getPartitioningMode: String = {
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
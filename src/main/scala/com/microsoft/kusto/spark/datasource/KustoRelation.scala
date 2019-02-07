package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException

import com.microsoft.azure.kusto.data.{ClientFactory, ConnectionStringBuilder}
import com.microsoft.kusto.spark.utils.KustoQueryUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

case class KustoRelation(cluster: String,
                         database: String,
                         appId: String,
                         appKey: String,
                         authorityId: String,
                         query: String,
                         isLeanMode: Boolean,
                         customSchema: Option[String] = None)
                   (@transient val sparkContext: SQLContext) extends BaseRelation with TableScan with Serializable {

  private val normalizedQuery = KustoQueryUtils.normalizeQuery(query)

  override def sqlContext: SQLContext = sparkContext

  override def schema: StructType = {
    if (customSchema.isDefined)
    {
      StructType.fromDDL(customSchema.get)
    }
    else getSchema
  }

  override def buildScan(): RDD[Row] = {
    if (isLeanMode) leanBuildScan() else scaleBuildScan()
  }

  def leanBuildScan() : RDD[Row] = {
    val kustoConnectionString = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authorityId)
    val kustoResult = ClientFactory.createClient(kustoConnectionString).execute(database, normalizedQuery)
    var serializer = KustoResponseDeserializer(kustoResult)
    sparkContext.createDataFrame(serializer.toRows, serializer.getSchema).rdd
  }

  def scaleBuildScan() : RDD[Row] = {
    throw new NotImplementedException
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
}
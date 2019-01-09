package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException

import com.microsoft.azure.kusto.data.{ClientFactory, ConnectionStringBuilder}
import com.microsoft.kusto.spark.utils.KustoQueryUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

case class KustoRelation(cluster: String,
                         database: String,
                         table: String,
                         appId: String,
                         appKey: String,
                         authorityId: String,
                         query: String,
                         customSchema: Option[String] = None)
                   (@transient val sparkContext: SQLContext) extends BaseRelation with TableScan with Serializable {

  private val primaryResultTableIndex = 0
  override def sqlContext: SQLContext = sparkContext

  override def schema: StructType = {
    if (customSchema.isDefined)
    {
      StructType.fromDDL(customSchema.get)
    }
    else getSchema
  }

  override def buildScan(): RDD[Row] = {
    val kustoConnectionString = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authorityId)
    val queryToPost = if (!table.isEmpty) table else KustoQueryUtils.nomralizeQuery(query)
    val kustoResult = ClientFactory.createClient(kustoConnectionString).execute(database, queryToPost)
    var serializer = KustoResponseDeserializer(kustoResult)
    sparkContext.createDataFrame(serializer.toRows, serializer.getSchema).rdd
  }

  private def getSchema: StructType = {
    if (query.isEmpty && table.isEmpty) {
      throw new InvalidParameterException("Query and table name are both empty")
    }

    val getSchemaQuery = if (!table.isEmpty) KustoQueryUtils.getQuerySchemaQuery(table) else if (KustoQueryUtils.isQuery(query)) KustoQueryUtils.getQuerySchemaQuery(query) else ""
    if (getSchemaQuery.isEmpty) {
      throw new RuntimeException("Cannot get schema. Please provide a valid kusto table name or a valid query.")
    }
    val kustoConnectionString = ConnectionStringBuilder.createWithAadApplicationCredentials(s"https://$cluster.kusto.windows.net", appId, appKey, authorityId)
    KustoResponseDeserializer(ClientFactory.createClient(kustoConnectionString).execute(database, getSchemaQuery)).getSchema
  }
}

package com.microsoft.kusto.spark.datasink

import java.util

import com.microsoft.azure.kusto.data.{Client, ConnectionStringBuilder}
import com.microsoft.kusto.spark.datasource.{KustoAuthentication, KustoCoordinates}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoClient, KustoDataSourceUtils}
import org.joda.time.{DateTime, DateTimeZone, Period}

import scala.collection.JavaConverters._

object TempStorageCache{

  var roundRubinIdx = 0
  var cluster = ""
  var storages = new Array[String](0)
  var dmClient: Client = _
  var lastRefresh: DateTime = new DateTime(DateTimeZone.UTC)

  val storageExpiryMinutes = 120

  def getNewTempBlobReference(clusterAlias: String, kcsb: ConnectionStringBuilder): String = {
    getNextUri(clusterAlias, kcsb)
  }

  private def getNextUri(clusterAlias: String, kcsb: ConnectionStringBuilder): String = {
    // Refresh if 120 minutes have passed since last refresh for this cluster as SAS should be valid for at least 120 minutes
    if(storages.length == 0 ||  new Period(new DateTime(DateTimeZone.UTC), lastRefresh).getMinutes > storageExpiryMinutes){
      if(cluster != clusterAlias)
      {
        KustoDataSourceUtils.logWarn("TempStorageCache", "Code should not get here")
      }
      dmClient = KustoClient.getAdmin(kcsb)

      cluster = clusterAlias

      lastRefresh = new DateTime(DateTimeZone.UTC)

      val res = dmClient.execute(generateCreateTmpStorageCommand())
      storages = res.getValues.asScala.map(row => row.get(0)).toArray
    }

    roundRubinIdx = (roundRubinIdx + 1) % storages.length
    storages(roundRubinIdx)
  }
}
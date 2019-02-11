package com.microsoft.kusto.spark.datasink

import java.util

import com.microsoft.azure.kusto.data.{Client, ClientFactory, ConnectionStringBuilder}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import org.joda.time.{DateTime, DateTimeZone, Period}

object TempStorageCache{

  var roundRubinIdx = 0
  var cluster = ""
  var storages = new util.ArrayList[String]
  var dmClient: Client = _
  var lastRefresh: DateTime = new DateTime(DateTimeZone.UTC)

  val storageExpiryMinutes = 120

  def getNewTempBlobReference(ingestKcsb: ConnectionStringBuilder, clusterName: String): String = {
    getNextUri(ingestKcsb, clusterName)
  }

  private def getNextUri(ingestKcsb: ConnectionStringBuilder, clusterName: String): String = {
    // Refresh if 120 minutes have passed since last refresh for this cluster as SAS should be valid for at least 120 minutes
    if(storages.size() == 0 || cluster != clusterName || new Period(new DateTime(DateTimeZone.UTC), lastRefresh).getMinutes > storageExpiryMinutes){
      dmClient = ClientFactory.createClient(ingestKcsb)
      cluster = clusterName

      lastRefresh = new DateTime(DateTimeZone.UTC)

      val res = dmClient.execute(generateCreateTmpStorageCommand())
      storages = res.getValues.get(0)
    }

    roundRubinIdx = (roundRubinIdx + 1) % storages.size
    storages.get(roundRubinIdx)
  }

}
package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ConnectionStringBuilder
import com.microsoft.kusto.spark.utils.CslCommandsGenerator._
import com.microsoft.kusto.spark.utils.{KustoClient, KustoConstants}
import org.joda.time.{DateTime, DateTimeZone, Period}

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

object KustoTempIngestStorageCache {

  var roundRobinIdx = 0
  // Maps each cluster (by cluster-alias including region) to a set of access UIRs to intermediate storage accounts
  var storagesMap = new HashMap[String, Array[String]]
  var lastRefresh: DateTime = new DateTime(DateTimeZone.UTC)

  def getNewTempBlobReference(clusterAlias: String, kcsb: ConnectionStringBuilder): String = {
    getNextUri(clusterAlias, kcsb, new DateTime(DateTimeZone.UTC))
  }

  private def getNextUri(clusterAlias: String, kcsb: ConnectionStringBuilder, now: DateTime): String = {
    var storageCached = storagesMap.get(clusterAlias)
    // Refresh if storageExpiryMinutes have passed since last refresh for this cluster as SAS should be valid for at least 120 minutes
    if (storageCached.isEmpty ||
      storageCached.get.length == 0 ||
      new Period(now, lastRefresh).getMinutes > KustoConstants.storageExpiryMinutes) {
      val dmClient = KustoClient.getAdmin(kcsb, clusterAlias, isIngestCluster = true)

      lastRefresh = now

      val res = dmClient.execute(generateCreateTmpStorageCommand())
      storageCached = Some(res.getValues.asScala.map(row => row.get(0)).toArray)
      storagesMap = storagesMap + (clusterAlias -> storageCached.get)
    }

    roundRobinIdx = (roundRobinIdx + 1) % storageCached.get.length
    storageCached.get(roundRobinIdx)
  }
}
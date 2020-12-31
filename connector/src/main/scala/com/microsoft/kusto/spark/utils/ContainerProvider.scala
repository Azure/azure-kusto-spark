package com.microsoft.kusto.spark.utils

import com.microsoft.azure.kusto.data.Client
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.joda.time.{DateTime, DateTimeZone, Period}

import scala.collection.JavaConverters._

class ContainerProvider[A](val dmClient: Client, val clusterAlias: String, val command: String, cacheEntryCreator: ContainerAndSas => A) {
  private var roundRobinIdx = 0
  private var storageUris: Seq[A] = Seq.empty
  private var lastRefresh: DateTime = new DateTime(DateTimeZone.UTC)
  private val myName = this.getClass.getSimpleName

  def getContainer: A = {
    // Refresh if storageExpiryMinutes have passed since last refresh for this cluster as SAS should be valid for at least 120 minutes
    if (storageUris.isEmpty ||
      new Period(lastRefresh, new DateTime(DateTimeZone.UTC)).toStandardMinutes.getMinutes > KustoConstants.storageExpiryMinutes) {
      refresh
    } else {
      roundRobinIdx = (roundRobinIdx + 1) % storageUris.size
      storageUris(roundRobinIdx)
    }
  }

  def getAllContainers: Seq[A] = {
    if (storageUris.isEmpty ||
      new Period(lastRefresh, new DateTime(DateTimeZone.UTC)).toStandardMinutes.getMinutes > KustoConstants.storageExpiryMinutes){
      refresh
    }
    storageUris
  }

  private def refresh = {
      val res = dmClient.execute(command)
      val storage = res.getPrimaryResults.getData.asScala.map(row => {
        val parts = row.get(0).toString.split('?')
        cacheEntryCreator(ContainerAndSas(parts(0), '?' + parts(1)))
      })

      if (storage.isEmpty) {
        KDSU.reportExceptionAndThrow(myName, new RuntimeException("Failed to allocate temporary storage"), "writing to Kusto", clusterAlias)
      }

      KDSU.logInfo(myName, s"Got ${storage.length} storage SAS with command :'$command'. from service 'ingest-$clusterAlias'")
      lastRefresh = new DateTime(DateTimeZone.UTC)
      storageUris = scala.util.Random.shuffle(storage)
      roundRobinIdx = 0
      storage(roundRobinIdx)
  }
}

package com.microsoft.kusto.spark.utils
import org.joda.time.{DateTime, DateTimeZone, Period}
import scala.collection.mutable.Map

private[kusto] object KustoAzureFsSetupCache {
  private var storageAccountKeyMap: Map[String, String] = Map.empty[String,String]
  private var storageSasMap: Map[String, String] = Map.empty[String,String]
  private var nativeAzureFsSet = false
  private var lastRefresh: DateTime = new DateTime(DateTimeZone.UTC)

  // Return 'true' iff the entry exists in the cache. If it doesn't, or differs - update the cache
  // now is typically 'new DateTime(DateTimeZone.UTC)'
  def updateAndGetPrevStorageAccountAccess(account: String, secret: String, now: DateTime): Boolean = {
    var secretCached = storageAccountKeyMap(account)
    if (!secretCached.isEmpty && (secretCached != secret)) {
      // Entry exists but with a different secret - remove it and update
      storageAccountKeyMap.remove(account)
      secretCached = ""
    }

    if (secretCached.isEmpty || checkIfRefreshNeeded(now)) {
      storageAccountKeyMap += (account -> secret)
      lastRefresh = now
      false
    } else true
  }

  def updateAndGetPrevSas(container: String, account: String, secret: String, now: DateTime): Boolean = {
    val key = container + "." + account
    var secretCached = storageSasMap(key)
    if (!secretCached.isEmpty && (secretCached != secret)) {
      // Entry exists but with a different secret - remove it and update
      storageSasMap.remove(key)
      secretCached = ""
    }

    if (secretCached.isEmpty || checkIfRefreshNeeded(now)) {
      storageSasMap += (key -> secret)
      lastRefresh = now
      false
    } else true
  }

  def updateAndGetPrevNativeAzureFs(now: DateTime): Boolean = {
    if (nativeAzureFsSet || checkIfRefreshNeeded(now)) true else {
      nativeAzureFsSet = true
      false
    }
  }

  private[kusto] def checkIfRefreshNeeded(utcNow: DateTime) = {
    new Period(utcNow, lastRefresh).getMinutes > KustoConstants.sparkSettingsRefreshMinutes
  }
}

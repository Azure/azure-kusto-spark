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
  def updateAndGetPrevStorageAccountAccess(account: String, secret: String, utcNow: DateTime): Boolean = {
    var secreteCached = storageAccountKeyMap(account)
    if (!secreteCached.isEmpty && (secreteCached != secret)) {
      // Entry exists but with a different secret - remove it and update
      storageAccountKeyMap.remove(account)
      secreteCached = ""
    }

    if (secreteCached.isEmpty || checkIfRefreshNeeded(utcNow)) {
      storageAccountKeyMap += (account -> secret)
      lastRefresh = utcNow

      return false
    }
    true
  }

  def updateAndGetPrevSas(container: String, account: String, secret: String, utcNow: DateTime): Boolean = {
    val key = container + "." + account
    var secreteCached = storageSasMap(key)
    if (!secreteCached.isEmpty && (secreteCached != secret)) {
      // Entry exists but with a different secret - remove it and update
      storageSasMap.remove(key)
      secreteCached = ""
    }

    if (secreteCached.isEmpty || checkIfRefreshNeeded(utcNow)) {
      storageSasMap += (key -> secret)
      lastRefresh = utcNow

      return false
    }
    true
  }

  def updateAndGetPrevNativeAzureFs(utcNow: DateTime): Boolean = {
    if (nativeAzureFsSet || checkIfRefreshNeeded(utcNow)) true else {
      nativeAzureFsSet = true
      false
    }
  }

  private[kusto] def checkIfRefreshNeeded(utcNow: DateTime) = {
    new Period(utcNow, lastRefresh).getMinutes > KustoConstants.sparkSettingsRefreshMinutes
  }
}

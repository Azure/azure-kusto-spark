// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import java.time.{Clock, Duration, Instant}
import scala.collection.mutable

private[kusto] object KustoAzureFsSetupCache {
  private val storageAccountKeyMap: mutable.Map[String, String] =
    mutable.Map.empty[String, String]
  private var nativeAzureFsSet = false
  private var lastRefresh: Instant = Instant.now(Clock.systemUTC())

  // Return 'true' iff the entry exists in the cache. If it doesn't, or differs - update the cache
  // now is typically 'new DateTime(DateTimeZone.UTC)'
  def updateAndGetPrevStorageAccountAccess(
      account: String,
      secret: String,
      now: Instant): Boolean = {
    val maybeSecretCache = storageAccountKeyMap.get(account)
    val shouldRefreshCache = maybeSecretCache match {
      // There exists a secret or the secret is stale
      case Some(secretCached) => !secret.equals(secretCached) || checkIfRefreshNeeded(now)
      // There is no secret
      case None => true
    }
    if (shouldRefreshCache) {
      storageAccountKeyMap.put(account, secret)
      lastRefresh = now
      false
    } else {
      true
    }
  }

  def updateAndGetPrevSas(
      container: String,
      account: String,
      secret: String,
      now: Instant): Boolean = {
    updateAndGetPrevStorageAccountAccess(s"$container.$account", secret, now)
  }

  def updateAndGetPrevNativeAzureFs(now: Instant): Boolean = {
    if (nativeAzureFsSet || checkIfRefreshNeeded(now)) {
      true
    }
    else {
      nativeAzureFsSet = true
      false
    }
  }

  private[kusto] def checkIfRefreshNeeded(utcNow: Instant) = {
    Duration
      .between(utcNow, lastRefresh)
      .abs()
      .toMinutes
      .intValue() > KustoConstants.SparkSettingsRefreshMinutes
  }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.prop.TableDrivenPropertyChecks._

import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant}
class KustoAzureFsSetupCacheTest extends AnyFunSuite {

  private val Account1 = "account1"
  private val Secret1 = "secret1"
  private val Secret2 = "secret2"
  private val Container1 = "container1"
  private val Now = "now"
  private val ExpectedResult = "expectedResult"

  test("testUpdateAndGetPrevStorageAccountAccess") {
    val dataToTest = Table(
      ("account", "secret", Now, ExpectedResult),
      // a non existing key
      (Account1, Secret1, Instant.now(Clock.systemUTC()), false),
      // A key that is same but is expired
      (
        Account1,
        Secret1,
        Instant
          .now(Clock.systemUTC())
          .minus(3 * KustoConstants.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES),
        false),
      // A new secret value
      (Account1, Secret2, Instant.now(Clock.systemUTC()), false),
      // Same secret
      (
        Account1,
        Secret2,
        Instant
          .now(Clock.systemUTC())
          .minus(KustoConstants.SparkSettingsRefreshMinutes / 2, ChronoUnit.MINUTES),
        true))

    forAll(dataToTest) {
      (account: String, secret: String, now: Instant, expectedResult: Boolean) =>
        val actualResult =
          KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(account, secret, now)
        actualResult shouldEqual expectedResult
    }
  }

  test("testUpdateAndGetPrevNativeAzureFs") {
    val now = Instant.now(Clock.systemUTC())
    val dataToTest = Table(
      ("now", "checkIfRefreshNeeded", "Scenario"),
      // Initial set is false for the flag, but refresh
      (now, true, "Initial set is false, refresh is needed"),
      // The cache is expired, so it will be re-set.The checkIfRefreshNeeded will return false, but the state is already true.
      (
        now.minus(3 * KustoConstants.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES),
        true,
        "The cache is expired, so it will be re-set.The checkIfRefreshNeeded will return false, but the state is already true."),
      // This will be within the cache interval and also the flag is set to true
      (
        now.minus(KustoConstants.SparkSettingsRefreshMinutes / 2, ChronoUnit.MINUTES),
        true,
        "This will be within the cache interval and also the flag is set to true"))

    forAll(dataToTest) { (now: Instant, checkIfRefreshNeeded: Boolean, scenario: String) =>
      val actualResult = KustoAzureFsSetupCache.updateAndGetPrevNativeAzureFs(now)
      assert(actualResult == checkIfRefreshNeeded, scenario)
      actualResult shouldEqual checkIfRefreshNeeded
    }
  }

  test("testCheckIfRefreshNeeded") {
    val dataToTest = Table(
      (Now, ExpectedResult),
      // The cache is expired, so it will be re-set.The checkIfRefreshNeeded will return false, but the state is already true.
      (
        Instant
          .now(Clock.systemUTC())
          .minus(3 * KustoConstants.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES),
        true),
      // This will be within the cache interval and also the flag is set to true
      (
        Instant
          .now(Clock.systemUTC())
          .minus(KustoConstants.SparkSettingsRefreshMinutes / 2, ChronoUnit.MINUTES),
        false))

    forAll(dataToTest) { (now: Instant, expectedResult: Boolean) =>
      val actualResult = KustoAzureFsSetupCache.checkIfRefreshNeeded(now)
      actualResult shouldEqual expectedResult
    }
  }

  test("testUpdateAndGetPrevSas") {
    val dataToTest = Table(
      ("container", "account", "secret", Now, ExpectedResult),
      // a non existing key
      (Container1, Account1, Secret1, Instant.now(Clock.systemUTC()), false),
      // A key that is same but is expired
      (
        Container1,
        Account1,
        Secret1,
        Instant
          .now(Clock.systemUTC())
          .minus(3 * KustoConstants.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES),
        false),
      // A new secret value
      (Container1, Account1, Secret2, Instant.now(Clock.systemUTC()), false),
      // Same secret
      (
        Container1,
        Account1,
        Secret2,
        Instant
          .now(Clock.systemUTC())
          .minus(KustoConstants.SparkSettingsRefreshMinutes / 2, ChronoUnit.MINUTES),
        true),
      // Container name changes. This should get set
      ("container2", Account1, Secret2, Instant.now(Clock.systemUTC()), false),
      // Since the key exists, this should return true
      ("container2", Account1, Secret2, Instant.now(), true))

    forAll(dataToTest) {
      (
          container: String,
          account: String,
          secret: String,
          now: Instant,
          expectedResult: Boolean) =>
        val actualResult =
          KustoAzureFsSetupCache.updateAndGetPrevSas(container, account, secret, now)
        actualResult shouldEqual expectedResult
    }
  }
}

package com.microsoft.kusto.spark.utils

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.prop.TableDrivenPropertyChecks._

import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant}
class KustoAzureFsSetupCacheTest extends AnyFunSuite {

  test("testUpdateAndGetPrevStorageAccountAccess") {
    val dataToTest = Table(
      ("account", "secret", "now", "expectedResult"),
        // a non existing key
      ("account1","secret1",Instant.now(Clock.systemUTC()),false),
      // A key that is same but is expired
      ("account1","secret1",
      Instant.now(Clock.systemUTC()).minus(3 * KustoConstants.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES),false),
      // A new secret value
      ("account1","secret2",Instant.now(Clock.systemUTC()),false),
      // Same secret
      ("account1", "secret2", Instant.now(Clock.systemUTC()).minus(KustoConstants.SparkSettingsRefreshMinutes / 2, ChronoUnit.MINUTES), true)
    )

    forAll(dataToTest) { (account: String, secret: String, now: Instant, expectedResult:Boolean) =>
      val actualResult = KustoAzureFsSetupCache.updateAndGetPrevStorageAccountAccess(account, secret, now)
      actualResult shouldEqual expectedResult
    }
  }

  test("testUpdateAndGetPrevNativeAzureFs") {
    val dataToTest = Table(
      ("now", "expectedResult"),
      // Initial set is false for the flag, but refresh
      (Instant.now(Clock.systemUTC()), true),
      // The cache is expired, so it will be re-set.The checkIfRefreshNeeded will return false, but the state is already true.
      (Instant.now(Clock.systemUTC()).minus(3 * KustoConstants.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES), true),
      // This will be within the cache interval and also the flag is set to true
      (Instant.now(Clock.systemUTC()).minus(KustoConstants.SparkSettingsRefreshMinutes / 2, ChronoUnit.MINUTES) , true),
    )

    forAll(dataToTest) { (now: Instant, expectedResult: Boolean) =>
      val actualResult = KustoAzureFsSetupCache.updateAndGetPrevNativeAzureFs(now)
      actualResult shouldEqual expectedResult
    }
  }

  test("testCheckIfRefreshNeeded") {
    val dataToTest = Table(
      ("now", "expectedResult"),
      // The cache is expired, so it will be re-set.The checkIfRefreshNeeded will return false, but the state is already true.
      (Instant.now(Clock.systemUTC()).minus(3 * KustoConstants.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES), true),
      // This will be within the cache interval and also the flag is set to true
      (Instant.now(Clock.systemUTC()).minus(KustoConstants.SparkSettingsRefreshMinutes / 2, ChronoUnit.MINUTES), false),
    )

    forAll(dataToTest) { (now: Instant, expectedResult: Boolean) =>
      val actualResult = KustoAzureFsSetupCache.checkIfRefreshNeeded(now)
      actualResult shouldEqual expectedResult
    }
  }

  test("testUpdateAndGetPrevSas") {
    val dataToTest = Table(
      ("container","account", "secret", "now", "expectedResult"),
      // a non existing key
      ("container1","account1", "secret1", Instant.now(Clock.systemUTC()), false),
      // A key that is same but is expired
      ("container1","account1", "secret1",
        Instant.now(Clock.systemUTC()).minus(3 * KustoConstants.SparkSettingsRefreshMinutes, ChronoUnit.MINUTES), false),
      // A new secret value
      ("container1","account1", "secret2", Instant.now(Clock.systemUTC()), false),
      // Same secret
      ("container1","account1", "secret2", Instant.now(Clock.systemUTC()).minus(KustoConstants.SparkSettingsRefreshMinutes / 2, ChronoUnit.MINUTES), true),
      // Container name changes. This should get set
      ("container2","account1", "secret2", Instant.now(Clock.systemUTC()), false),
      // Since the key exists, this should return true
      ("container2","account1", "secret2", Instant.now(), true),
    )

    forAll(dataToTest) { (container:String, account: String, secret: String, now: Instant, expectedResult: Boolean) =>
      val actualResult = KustoAzureFsSetupCache.updateAndGetPrevSas(container,account, secret, now)
      actualResult shouldEqual expectedResult
    }
  }
}

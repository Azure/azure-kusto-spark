package com.microsoft.kusto.spark.authentication

import java.util.concurrent.CompletableFuture
import java.util.function.Consumer

import com.microsoft.aad.msal4j.{DeviceCode, DeviceCodeFlowParameters, IAuthenticationResult}
import com.microsoft.azure.kusto.data.auth

import scala.concurrent.TimeoutException

class DeviceAuthentication (val cluster: String, val authority:String) extends auth.DeviceAuthTokenProvider(cluster, authority) {
  var deviceCode: Option[DeviceCode] = None
  var expiresAt: Option[Long] = None
  var awaitAuthentication: Option[CompletableFuture[IAuthenticationResult]] = None
  val NewDeviceCodeFetchTimeout = 5000
  val Interval = 500

  override def acquireNewAccessToken(): IAuthenticationResult = {
    awaitAuthentication = Some(acquireNewAccessTokenAsync())
    awaitAuthentication.get.join()
  }

  def acquireNewAccessTokenAsync(): CompletableFuture[IAuthenticationResult] = {
    val deviceCodeConsumer: Consumer[DeviceCode] = new Consumer[DeviceCode] {
      override def accept(code: DeviceCode): Unit = {
        deviceCode = Some(code)
        expiresAt = Some(System.currentTimeMillis + (code.expiresIn() * 1000))
        println(code.message())
      }
    }

    val deviceCodeFlowParams: DeviceCodeFlowParameters = DeviceCodeFlowParameters.builder(scopes, deviceCodeConsumer).build
    clientApplication.acquireToken(deviceCodeFlowParams)
  }

  def refreshIfNeeded(): Unit = {
    if (deviceCode.isEmpty || expiresAt.get <= System.currentTimeMillis) {
      val oldDeviceCode = this.deviceCode
      awaitAuthentication = Some(acquireNewAccessTokenAsync())
      var awaitTime = NewDeviceCodeFetchTimeout
      while (this.deviceCode == oldDeviceCode){
        if (awaitTime <= 0) {
          throw new TimeoutException("Timed out waiting for a new device code")
        }
        Thread.sleep(Interval)
        awaitTime = awaitTime - Interval
      }
    }
  }

  def getDeviceCodeMessage: String  = {
    refreshIfNeeded()
    deviceCode.get.message()
  }

  def getDeviceCode: DeviceCode = {
    refreshIfNeeded()
    deviceCode.get
  }

  def acquireToken(): String = {
    refreshIfNeeded()
    awaitAuthentication.get.join().accessToken()
  }
}


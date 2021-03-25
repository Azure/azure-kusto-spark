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
  val NEW_DEVICE_CODE_FETCH_TIMEOUT = 5000
  override def acquireNewAccessToken(): IAuthenticationResult = {
    awaitAuthentication = Some(acquireNewAccessTokenAsync())
    awaitAuthentication.get.join()
  }

  def acquireNewAccessTokenAsync(): CompletableFuture[IAuthenticationResult] = {
    val deviceCodeConsumer: Consumer[DeviceCode] = (deviceCode: DeviceCode) => {
      this.deviceCode = Some(deviceCode)
      expiresAt = Some(System.currentTimeMillis + deviceCode.expiresIn() * 1000)
      System.out.println(deviceCode.message())
    }

    val deviceCodeFlowParams: DeviceCodeFlowParameters = DeviceCodeFlowParameters.builder(scopes, deviceCodeConsumer).build
    clientApplication.acquireToken(deviceCodeFlowParams)
  }

  def refreshIfNeeded(): Unit = {
    if (deviceCode.isEmpty || expiresAt.get <= System.currentTimeMillis) {
      val oldDeviceCode = this.deviceCode
      awaitAuthentication = Some(acquireNewAccessTokenAsync())
      var awaitTime = NEW_DEVICE_CODE_FETCH_TIMEOUT
      val interval = 500
      while (this.deviceCode == oldDeviceCode){
        if (awaitTime <= 0) {
          throw new TimeoutException("Timed out waiting for a new device code")
        }
        Thread.sleep(interval)
        awaitTime = awaitTime - interval
      }
    }
  }

  def getDeviceCodeMessage(): String  = {
    refreshIfNeeded()
    deviceCode.get.message()
  }

  def getDeviceCode(): DeviceCode = {
    refreshIfNeeded()
    deviceCode.get
  }

  def acquireToken(): String = {
    refreshIfNeeded()
    awaitAuthentication.get.join().accessToken()
  }
}


package com.microsoft.kusto.spark.authentication

import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.Consumer

import com.microsoft.aad.msal4j.{DeviceCode, DeviceCodeFlowParameters, IAuthenticationResult}
import com.microsoft.azure.kusto.data.auth

class DeviceAuthentication (val cluster: String, val authority:String) extends auth.DeviceAuthTokenProvider(cluster, authority, null) {
  var currentDeviceCode: Option[DeviceCode] = None
  var expiresAt: Option[Long] = None
  val NewDeviceCodeFetchTimeout = 60L * 1000L
  var currentToken: Option[String] = None

  override def acquireNewAccessToken(): IAuthenticationResult = {
    acquireNewAccessTokenAsync().get(NewDeviceCodeFetchTimeout, TimeUnit.MILLISECONDS)
  }

  def acquireNewAccessTokenAsync(): CompletableFuture[IAuthenticationResult] = {
    val deviceCodeConsumer: Consumer[DeviceCode] = (deviceCode: DeviceCode) => {
      currentDeviceCode = Some(deviceCode)
      expiresAt = Some(System.currentTimeMillis + (deviceCode.expiresIn() * 1000))
      println(deviceCode.message())
    }

    val deviceCodeFlowParams: DeviceCodeFlowParameters = DeviceCodeFlowParameters.builder(scopes, deviceCodeConsumer).build
    clientApplication.acquireToken(deviceCodeFlowParams)
  }

  def refreshIfNeeded(): Unit = {
    if (currentDeviceCode.isEmpty || expiresAt.get <= System.currentTimeMillis) {
      currentToken = Some(acquireAccessToken())
    }
  }

  def getDeviceCodeMessage: String  = {
    refreshIfNeeded()
    currentDeviceCode.get.message()
  }

  def getDeviceCode: DeviceCode = {
    refreshIfNeeded()
    currentDeviceCode.get
  }

  def acquireToken(): String = {
    refreshIfNeeded()
    currentToken.get
  }
}


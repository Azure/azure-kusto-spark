package com.microsoft.kusto.spark.authentication

import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.Consumer
import com.microsoft.aad.msal4j.{DeviceCode, DeviceCodeFlowParameters, IAuthenticationResult}
import com.microsoft.azure.kusto.data.auth
import scala.concurrent.TimeoutException

class DeviceAuthentication (val cluster: String, val authority:String) extends auth.DeviceAuthTokenProvider(cluster, authority, null) {
  var currentDeviceCode: Option[DeviceCode] = None
  var expiresAt: Option[Long] = None
  val NewDeviceCodeFetchTimeout = 60L * 1000L
  var currentToken: Option[String] = None

  override def acquireNewAccessToken(): IAuthenticationResult = {
    acquireNewAccessTokenAsync().get(NewDeviceCodeFetchTimeout, TimeUnit.MILLISECONDS)
  }

  def acquireNewAccessTokenAsync(): CompletableFuture[IAuthenticationResult] = {
    val deviceCodeConsumer: Consumer[DeviceCode] = toJavaConsumer((deviceCode:DeviceCode) => {
      this.currentDeviceCode = Some(deviceCode)
      this.expiresAt = Some(System.currentTimeMillis + (deviceCode.expiresIn() * 1000))
      println(deviceCode.message())
      return null
    })

    val deviceCodeFlowParams: DeviceCodeFlowParameters = DeviceCodeFlowParameters.builder(scopes, deviceCodeConsumer).build
    clientApplication.acquireToken(deviceCodeFlowParams)
  }

  implicit def toJavaConsumer[T](f:Function1[T, Void]): Consumer[T] = new Consumer[T] {
    override def accept(t: T) = f(t)
  }

  def refreshIfNeeded(): Unit = {
    if (currentDeviceCode.isEmpty || expiresAt.get <= System.currentTimeMillis) {
      currentToken = Some(acquireAccessToken())
    }
  }

  def getDeviceCodeMessage: String = {
    refreshIfNeeded()
    this.currentDeviceCode.get.message()
  }

  def getDeviceCode: DeviceCode = {
    refreshIfNeeded()
    this.currentDeviceCode.get
  }

  def acquireToken(): String = {
    refreshIfNeeded()
    currentToken.get
  }
}

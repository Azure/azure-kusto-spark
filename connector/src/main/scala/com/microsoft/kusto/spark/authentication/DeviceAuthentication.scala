// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.authentication

import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.Consumer
import com.microsoft.aad.msal4j.{DeviceCode, DeviceCodeFlowParameters, IAuthenticationResult}
import com.microsoft.azure.kusto.data.auth
import org.apache.http.impl.client.HttpClients

import scala.concurrent.TimeoutException

class DeviceAuthentication(val cluster: String, val authority: String)
    extends auth.DeviceAuthTokenProvider(cluster, authority, HttpClients.createDefault()) {
  private var currentDeviceCode: Option[DeviceCode] = None
  private var expiresAt: Option[Long] = None
  private val NewDeviceCodeFetchTimeout = 60L * 1000L
  private var currentToken: Option[String] = None

  override def acquireNewAccessToken(): IAuthenticationResult = {
    acquireNewAccessTokenAsync().get(NewDeviceCodeFetchTimeout, TimeUnit.MILLISECONDS)
  }

  private def acquireNewAccessTokenAsync(): CompletableFuture[IAuthenticationResult] = {
    val deviceCodeConsumer: Consumer[DeviceCode] = toJavaConsumer((deviceCode: DeviceCode) => {
      this.currentDeviceCode = Some(deviceCode)
      this.expiresAt = Some(System.currentTimeMillis + (deviceCode.expiresIn() * 1000))
      println(deviceCode.message())
    })

    val deviceCodeFlowParams: DeviceCodeFlowParameters =
      DeviceCodeFlowParameters.builder(scopes, deviceCodeConsumer).build
    clientApplication.acquireToken(deviceCodeFlowParams)
  }

  private def toJavaConsumer[T](f: T => Unit): Consumer[T] = (t: T) => f(t)

  private def refreshIfNeeded(): Unit = {
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

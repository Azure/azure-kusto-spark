//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark.authentication

import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.Consumer
import com.microsoft.aad.msal4j.{DeviceCode, DeviceCodeFlowParameters, IAuthenticationResult}
import com.microsoft.azure.kusto.data.auth
import scala.concurrent.TimeoutException

class DeviceAuthentication(val cluster: String, val authority: String)
    extends auth.DeviceAuthTokenProvider(cluster, authority, null) {
  var currentDeviceCode: Option[DeviceCode] = None
  var expiresAt: Option[Long] = None
  val NewDeviceCodeFetchTimeout = 60L * 1000L
  var currentToken: Option[String] = None

  override def acquireNewAccessToken(): IAuthenticationResult = {
    acquireNewAccessTokenAsync().get(NewDeviceCodeFetchTimeout, TimeUnit.MILLISECONDS)
  }

  def acquireNewAccessTokenAsync(): CompletableFuture[IAuthenticationResult] = {
    val deviceCodeConsumer: Consumer[DeviceCode] = toJavaConsumer((deviceCode: DeviceCode) => {
      this.currentDeviceCode = Some(deviceCode)
      this.expiresAt = Some(System.currentTimeMillis + (deviceCode.expiresIn() * 1000))
      println(deviceCode.message())
    })

    val deviceCodeFlowParams: DeviceCodeFlowParameters =
      DeviceCodeFlowParameters.builder(scopes, deviceCodeConsumer).build
    clientApplication.acquireToken(deviceCodeFlowParams)
  }

  implicit def toJavaConsumer[T](f: Function1[T, Unit]): Consumer[T] = new Consumer[T] {
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

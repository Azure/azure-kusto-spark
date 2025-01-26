//// Copyright (c) Microsoft Corporation. All rights reserved.
//// Licensed under the MIT License.
//
//package com.microsoft.kusto.spark.authentication
//
//import com.azure.core.credential.TokenRequestContext
//import com.azure.identity.DeviceCodeCredentialBuilder
//import com.microsoft.azure.kusto.data.auth
//
//import java.time.Duration
//import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
//
//class DeviceAuthentication(val cluster: String, val authority: String)
//    extends auth.DeviceAuthTokenProvider(cluster, authority, null) {
//  private val newDeviceCodeFetchTimeout = 60L * 1000L
//  private var expiresAt: Option[Long] = Some(0L)
//  private var currentToken: Option[String] = None
//
//  def acquireToken(): String = {
//    refreshIfNeeded()
//    currentToken.get
//  }
//
//  private def refreshIfNeeded(): Unit = {
//    if (isRefreshNeeded) {
//      val tokenCredential =
//        acquireNewAccessToken.getToken(new TokenRequestContext().addScopes(scopes.toSeq: _*))
//      val tokenCredentialValue =
//        tokenCredential.blockOptional(Duration.ofMillis(newDeviceCodeFetchTimeout))
//      tokenCredentialValue.ifPresent(token => {
//        currentToken = Some(token.getToken)
//        expiresAt = Some(token.getExpiresAt.toEpochSecond * 1000)
//      })
//    }
//  }
//
//  private def acquireNewAccessToken = {
//    val deviceCodeFlowParams = new DeviceCodeCredentialBuilder()
//    super.createTokenCredential(deviceCodeFlowParams)
//  }
//
//  private def isRefreshNeeded: Boolean = {
//    expiresAt.isEmpty || expiresAt.get < System.currentTimeMillis()
//  }
//}

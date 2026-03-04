// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.authentication

import org.apache.hadoop.fs.azurebfs.oauth2.{AccessTokenProvider, AzureADToken}

class AzureTokenTokenProvider extends AccessTokenProvider {

  override def refreshToken(): AzureADToken = {
    AzureTokenTokenProvider.azureADToken.orNull
  }
}

object AzureTokenTokenProvider {
  var azureADToken: Option[AzureADToken] = None
}

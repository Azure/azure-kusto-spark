// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.exceptions

import com.microsoft.azure.kusto.data.KustoResultSetTable

class FailedOperationException(msg: String, result: Option[KustoResultSetTable])
    extends scala.Exception(msg) {
  def getResult: Option[KustoResultSetTable] = result
}

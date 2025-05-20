// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

final case class CustomTransform(
    targetColumnName: String,
    cslDataType: String,
    transformationMethod: String,
    sourceColumnName: String)

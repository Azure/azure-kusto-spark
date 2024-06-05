// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.exceptions

case class SchemaMatchException(msg: String) extends scala.Exception(msg) {}

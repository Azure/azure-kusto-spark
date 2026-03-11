// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.source

import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasource.KustoFiltering
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

/** Single-mode partition: reader will query Kusto directly. */
final case class KustoSingleModeInputPartition(
    coordinates: KustoCoordinates,
    authentication: KustoAuthentication,
    query: String,
    readSchema: StructType,
    filtering: KustoFiltering,
    requestId: String)
    extends InputPartition

/**
 * Distributed-mode partition: reader will read parquet files from blob storage. Each partition
 * corresponds to one storage credential / export path.
 */
final case class KustoDistributedInputPartition(
    blobPath: String,
    storageAccountName: String,
    storageAccountKey: String,
    sasKey: String,
    blobContainer: String,
    endpointSuffix: String,
    storageProtocol: String,
    readSchema: StructType,
    requestId: String)
    extends InputPartition

/** Empty partition for when no data was exported. */
final case class KustoEmptyInputPartition(readSchema: StructType) extends InputPartition

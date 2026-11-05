// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.kusto.spark.utils.KustoConstants.{
  DefaultExtentsCountForSplitMergePerNode,
  DefaultMaxRetriesOnMoveExtents
}

final case class KustoCustomDebugWriteOptions(
    minimalExtentsCountForSplitMergePerNode: Int = DefaultExtentsCountForSplitMergePerNode,
    maxRetriesOnMoveExtents: Int = DefaultMaxRetriesOnMoveExtents,
    disableFlushImmediately: Boolean = false,
    ensureNoDuplicatedBlobs: Boolean = false,
    addSourceLocationTransform: Boolean = false,
    keyVaultPemFilePath: Option[String] = None,
    keyVaultPemFileKey: Option[String] = None)

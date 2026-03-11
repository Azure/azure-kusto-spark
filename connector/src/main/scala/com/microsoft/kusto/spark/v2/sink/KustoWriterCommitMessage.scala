// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.sink

import org.apache.spark.sql.connector.write.WriterCommitMessage

// Only carries serializable primitives — IngestionResult is NOT serializable
// and cannot cross the executor→driver boundary.
// For transactional mode, ingestion polling happens on the executor in commit().
final case class KustoWriterCommitMessage(
    partitionId: Int,
    ingestionCount: Int)
    extends WriterCommitMessage

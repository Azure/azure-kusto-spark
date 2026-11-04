// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasink.v2

/**
 * Thrown by the ingest-v2 write path when the write can safely be retried on the existing (V1)
 * path: nothing that could still land in the target table has been sent to the service.
 * KustoWriter catches it and runs the V1 write.
 *
 * @param stagingTableUsed
 *   true if the ingest-v2 attempt already created or used the staging table, so the V1 retry must
 *   use a fresh one
 */
class IngestV2FallbackException(
    message: String,
    cause: Throwable = null,
    val stagingTableUsed: Boolean = false)
    extends RuntimeException(message, cause)

/** One submitted ingest-v2 operation. Serializable so executors can report it to the driver. */
final case class IngestV2SubmittedOperation(
    operationId: String,
    database: String,
    table: String,
    ingestKind: String,
    blobCount: Int)

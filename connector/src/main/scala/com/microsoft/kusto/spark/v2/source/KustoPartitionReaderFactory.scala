// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.source

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory
}
import org.apache.spark.sql.types.StructType

/**
 * Factory that creates the appropriate [[PartitionReader]] based on the partition type
 * (single-mode Kusto query vs distributed parquet read).
 */
final class KustoPartitionReaderFactory(readSchema: StructType)
    extends PartitionReaderFactory
    with Serializable {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case p: KustoSingleModeInputPartition =>
        new KustoSingleModePartitionReader(p)
      case p: KustoDistributedInputPartition =>
        new KustoDistributedPartitionReader(p)
      case _: KustoEmptyInputPartition =>
        new KustoEmptyPartitionReader()
      case other =>
        throw new IllegalArgumentException(s"Unexpected partition type: ${other.getClass}")
    }
  }
}

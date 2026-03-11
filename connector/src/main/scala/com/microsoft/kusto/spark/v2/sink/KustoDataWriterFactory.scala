// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.v2.sink

import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.datasink.{WriteFormat, WriteOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

final case class KustoDataWriterFactory(
    tableCoordinates: KustoCoordinates,
    authentication: KustoAuthentication,
    writeOptions: WriteOptions,
    schema: StructType,
    tmpTableName: String,
    batchIdIfExists: String)
    extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    writeOptions.writeFormat match {
      case WriteFormat.CSV =>
        KustoCsvDataWriter(
          partitionId = partitionId,
          taskId = taskId,
          tableCoordinates = tableCoordinates,
          authentication = authentication,
          writeOptions = writeOptions,
          schema = schema,
          tmpTableName = tmpTableName,
          batchIdIfExists = batchIdIfExists)
      case WriteFormat.Parquet =>
        KustoParquetDataWriter(
          partitionId = partitionId,
          taskId = taskId,
          tableCoordinates = tableCoordinates,
          authentication = authentication,
          writeOptions = writeOptions,
          schema = schema,
          tmpTableName = tmpTableName,
          batchIdIfExists = batchIdIfExists)
    }
  }
}

// A no-op factory for when ingestion is skipped
object KustoNoOpDataWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new DataWriter[InternalRow] {
      override def write(record: InternalRow): Unit = ()
      override def commit(): WriterCommitMessage =
        KustoWriterCommitMessage(partitionId, 0)
      override def abort(): Unit = ()
      override def close(): Unit = ()
    }
  }
}

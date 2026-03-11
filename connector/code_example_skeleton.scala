/**
 * SPARK 4.0 DATASOURCEV2 IMPLEMENTATION - CODE SKELETON
 * For Azure Kusto Connector
 */

// ============================================================================
// 1. TABLE PROVIDER - Entry Point (replaces DefaultSource)
// ============================================================================

package com.microsoft.kusto.spark.datasource.v2

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.expressions.Transform
import java.util

/**
 * Entry point for Spark 4.0 DataSourceV2 API.
 * Replaces DefaultSource which used CreatableRelationProvider + RelationProvider.
 */
class KustoTableProvider extends TableProvider with DataSourceRegister {

  override def shortName(): String = "kusto"

  /**
   * Infer the schema by connecting to Kusto and analyzing the table/query result.
   * This is called BEFORE getTable() so you can cache the schema if needed.
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // Parse options (kusto.cluster, kusto.database, kusto.table, kusto.query, etc.)
    val kustoCoordinates = KustoCoordinates.fromOptions(options)
    val authentication = KustoAuthentication.fromOptions(options)
    
    // Connect to Kusto and infer schema
    val kustoConnector = new KustoConnector(kustoCoordinates, authentication)
    val schema = kustoConnector.inferSchema(
      options.getOrDefault("kusto.query", ""),
      options.getOrDefault("kusto.table", "")
    )
    kustoConnector.close()
    
    schema
  }

  /**
   * Infer partitioning if table supports it.
   * For Kusto, we might partition by a key column if available.
   */
  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    // For now, no partitioning - override if needed
    Array.empty[Transform]
  }

  /**
   * Create a Table instance with the given schema and properties.
   * This Table will support both read and write operations.
   */
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    
    val kustoCoordinates = KustoCoordinates.fromMap(properties)
    val authentication = KustoAuthentication.fromMap(properties)
    
    new KustoTable(
      schema = schema,
      kustoCoordinates = kustoCoordinates,
      authentication = authentication,
      options = new CaseInsensitiveStringMap(properties))
  }

  override def supportsExternalMetadata(): Boolean = false
}


// ============================================================================
// 2. TABLE - Logical representation of data
// ============================================================================

/**
 * Represents a Kusto table/query as a readable and writable data source.
 * Mix-ins: SupportsRead + SupportsWrite
 */
class KustoTable(
    val schema: StructType,
    val kustoCoordinates: KustoCoordinates,
    val authentication: KustoAuthentication,
    val options: CaseInsensitiveStringMap) 
  extends Table 
  with SupportsRead 
  with SupportsWrite {

  override def name(): String = {
    val db = kustoCoordinates.database
    val table = options.getOrDefault("kusto.table", "unknown")
    s"$db.$table"
  }

  // IMPORTANT: schema() must return the same schema as inferSchema()
  override def schema(): StructType = this.schema

  /**
   * Declare what operations this table supports.
   * BATCH_READ: Can be read in batch mode
   * BATCH_WRITE: Can be written to in batch mode
   */
  override def capabilities(): util.Set[TableCapability] = {
    val caps = util.EnumSet.of(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE
    )
    caps
  }

  /**
   * SupportsRead implementation: Return builder for reads
   */
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new KustoScanBuilder(
      schema = this.schema,
      kustoCoordinates = this.kustoCoordinates,
      authentication = this.authentication,
      options = options)
  }

  /**
   * SupportsWrite implementation: Return builder for writes
   */
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new KustoWriteBuilder(
      schema = info.schema(),
      kustoCoordinates = this.kustoCoordinates,
      authentication = this.authentication,
      options = info.options())
  }
}


// ============================================================================
// 3. READ PIPELINE - ScanBuilder → Scan → Batch → PartitionReader
// ============================================================================

class KustoScanBuilder(
    val schema: StructType,
    val kustoCoordinates: KustoCoordinates,
    val authentication: KustoAuthentication,
    val options: CaseInsensitiveStringMap) 
  extends ScanBuilder {

  override def build(): Scan = {
    new KustoScan(
      schema = schema,
      kustoCoordinates = kustoCoordinates,
      authentication = authentication,
      options = options)
  }
}

class KustoScan(
    val schema: StructType,
    val kustoCoordinates: KustoCoordinates,
    val authentication: KustoAuthentication,
    val options: CaseInsensitiveStringMap) 
  extends Scan {

  override def readSchema(): StructType = schema

  override def description(): String = 
    s"KustoScan(${kustoCoordinates.database}.${options.getOrDefault("table", "?")})"

  override def toBatch(): Batch = {
    new KustoBatch(
      schema = schema,
      kustoCoordinates = kustoCoordinates,
      authentication = authentication,
      options = options)
  }
}

class KustoBatch(
    val schema: StructType,
    val kustoCoordinates: KustoCoordinates,
    val authentication: KustoAuthentication,
    val options: CaseInsensitiveStringMap) 
  extends Batch {

  override def description(): String = "KustoBatch"

  override def createReaderFactory(): PartitionReaderFactory = {
    new KustoPartitionReaderFactory(
      schema = schema,
      kustoCoordinates = kustoCoordinates,
      authentication = authentication,
      options = options)
  }
}

class KustoPartitionReaderFactory(
    val schema: StructType,
    val kustoCoordinates: KustoCoordinates,
    val authentication: KustoAuthentication,
    val options: CaseInsensitiveStringMap) 
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new KustoPartitionReader(
      schema = schema,
      kustoCoordinates = kustoCoordinates,
      authentication = authentication,
      options = options)
  }

  override def supportColumnar(partition: InputPartition): Boolean = false
}

class KustoPartitionReader(
    val schema: StructType,
    val kustoCoordinates: KustoCoordinates,
    val authentication: KustoAuthentication,
    val options: CaseInsensitiveStringMap) 
  extends PartitionReader[InternalRow] {

  private var kustoConnector: Option[KustoConnector] = None
  private var currentIterator: Option[Iterator[InternalRow]] = None

  override def next(): Boolean = {
    // Lazy init
    if (kustoConnector.isEmpty) {
      kustoConnector = Some(new KustoConnector(kustoCoordinates, authentication))
      val query = options.getOrDefault("kusto.query", "")
      currentIterator = Some(kustoConnector.get.query(query, schema))
    }
    
    currentIterator.get.hasNext
  }

  override def get(): InternalRow = {
    currentIterator.get.next()
  }

  override def close(): Unit = {
    kustoConnector.foreach(_.close())
  }
}


// ============================================================================
// 4. WRITE PIPELINE - WriteBuilder → Write → BatchWrite → DataWriter
// ============================================================================

class KustoWriteBuilder(
    val schema: StructType,
    val kustoCoordinates: KustoCoordinates,
    val authentication: KustoAuthentication,
    val options: CaseInsensitiveStringMap) 
  extends WriteBuilder {

  override def build(): Write = {
    new KustoWrite(
      schema = schema,
      kustoCoordinates = kustoCoordinates,
      authentication = authentication,
      options = options)
  }
}

class KustoWrite(
    val schema: StructType,
    val kustoCoordinates: KustoCoordinates,
    val authentication: KustoAuthentication,
    val options: CaseInsensitiveStringMap) 
  extends Write {

  override def description(): String = "KustoWrite"

  override def toBatch(): BatchWrite = {
    new KustoBatchWrite(
      schema = schema,
      kustoCoordinates = kustoCoordinates,
      authentication = authentication,
      options = options)
  }
}

/**
 * Physical batch write implementation.
 * Manages the transaction: factory creation → writer creation → commit/abort
 */
class KustoBatchWrite(
    val schema: StructType,
    val kustoCoordinates: KustoCoordinates,
    val authentication: KustoAuthentication,
    val options: CaseInsensitiveStringMap) 
  extends BatchWrite {

  /**
   * Called once on driver to create factory.
   * Factory will be serialized and sent to executors.
   */
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new KustoDataWriterFactory(
      schema = schema,
      kustoCoordinates = kustoCoordinates,
      authentication = authentication,
      options = options,
      numPartitions = info.numPartitions())
  }

  override def useCommitCoordinator(): Boolean = true

  /**
   * Called on driver when a single writer successfully commits.
   * Useful for progress tracking.
   */
  override def onDataWriterCommit(message: WriterCommitMessage): Unit = {
    message match {
      case m: KustoWriterCommitMessage =>
        println(s"Partition wrote ${m.recordCount} records to ${m.manifestPath}")
      case _ =>
    }
  }

  /**
   * CRITICAL: Called on driver when ALL writers complete successfully.
   * This is where final coordination happens (e.g., moving temp files, finalizing ingest).
   */
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val kustoConnector = new KustoConnector(kustoCoordinates, authentication)
    
    try {
      // Collect all commit messages
      val commitMessages = messages.collect { 
        case m: KustoWriterCommitMessage => m 
      }

      // For Kusto: all temporary ingests are complete, finalize the operation
      val totalRecords = commitMessages.map(_.recordCount).sum
      println(s"Final commit: $totalRecords records from ${commitMessages.length} partitions")
      
      // Kusto-specific: manifests are already in storage, data is ingested
      // This is where you might verify all ingests completed successfully
      
    } finally {
      kustoConnector.close()
    }
  }

  /**
   * CRITICAL: Called on driver if ANY writer fails or job is aborted.
   * Clean up temporary files/partial ingests.
   */
  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    val kustoConnector = new KustoConnector(kustoCoordinates, authentication)
    
    try {
      val commitMessages = messages.collect { 
        case m: KustoWriterCommitMessage => m 
      }
      
      println(s"Aborting write: cleaning up ${commitMessages.length} partitions")
      
      // For each partition that started writing, clean up temp data
      commitMessages.foreach { msg =>
        kustoConnector.deleteTemporaryData(msg.manifestPath)
      }
      
    } finally {
      kustoConnector.close()
    }
  }
}

/**
 * Factory for creating DataWriters - serialized and sent to executors.
 * Must be Serializable!
 */
class KustoDataWriterFactory(
    val schema: StructType,
    val kustoCoordinates: KustoCoordinates,
    val authentication: KustoAuthentication,
    val options: CaseInsensitiveStringMap,
    val numPartitions: Int) 
  extends DataWriterFactory 
  with Serializable {

  private val serialVersionUID = 1L

  /**
   * Called on executor to create a writer for a specific partition.
   * partitionId: RDD partition ID (0 to numPartitions-1)
   * taskId: Task attempt ID from TaskContext.taskAttemptId()
   */
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new KustoDataWriter(
      partitionId = partitionId,
      taskId = taskId,
      schema = schema,
      kustoCoordinates = kustoCoordinates,
      authentication = authentication,
      options = options)
  }
}

/**
 * Per-partition writer running on executor.
 * Writes records and returns commit message.
 */
class KustoDataWriter(
    val partitionId: Int,
    val taskId: Long,
    val schema: StructType,
    val kustoCoordinates: KustoCoordinates,
    val authentication: KustoAuthentication,
    val options: CaseInsensitiveStringMap) 
  extends DataWriter[InternalRow] {

  private var recordCount = 0L
  private val batchBuffer = scala.collection.mutable.ArrayBuffer[InternalRow]()
  private val batchSize = 10000
  private var tempDirectoryPath: String = _

  /**
   * Called for each record in the partition.
   */
  override def write(record: InternalRow): Unit = {
    batchBuffer += record
    recordCount += 1
    
    // Batch write to temp location every N records
    if (batchBuffer.size >= batchSize) {
      flushBatch()
    }
  }

  /**
   * Called when all records are written successfully.
   * Returns a message that will be sent back to driver and passed to
   * BatchWrite.commit() or BatchWrite.abort().
   */
  override def commit(): WriterCommitMessage = {
    // Flush any remaining records
    flushBatch()
    
    // For Kusto: ingest the temp files
    val kustoConnector = new KustoConnector(kustoCoordinates, authentication)
    try {
      kustoConnector.ingestToKusto(tempDirectoryPath, schema)
    } finally {
      kustoConnector.close()
    }
    
    // Return commit message with metadata for driver-side finalization
    new KustoWriterCommitMessage(
      manifestPath = tempDirectoryPath,
      recordCount = recordCount)
  }

  /**
   * Called if write fails or is aborted.
   * Clean up any partial data written to temp location.
   */
  override def abort(): Unit = {
    // Delete temp directory
    if (tempDirectoryPath != null) {
      val fs = FileSystem.get(new Configuration())
      fs.delete(new Path(tempDirectoryPath), true)
    }
  }

  override def close(): Unit = {
    // Called after commit() or abort()
    // Any final cleanup
  }

  private def flushBatch(): Unit = {
    if (batchBuffer.isEmpty) return
    
    // Write batch to temporary location
    // For example, write CSV/Parquet to temp directory
    // tempDirectoryPath = s"/temp/kusto_write/${partitionId}_${taskId}"
    // WriteBatchToStorage(batchBuffer, tempDirectoryPath, schema)
    
    batchBuffer.clear()
  }
}

/**
 * Message sent back from executor to driver.
 * Must be Serializable!
 */
class KustoWriterCommitMessage(
    val manifestPath: String,
    val recordCount: Long) 
  extends WriterCommitMessage {
  
  private val serialVersionUID = 1L
}


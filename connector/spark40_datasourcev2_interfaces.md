# Spark 4.0 DataSourceV2 Interface Signatures

## Overview
These are the exact interface signatures for implementing a Spark 4.0 DataSourceV2 data source.
All interfaces are located in `org.apache.spark.sql.connector.*` packages.

## Core Interfaces

### 1. TableProvider (Entry Point)
```java
package org.apache.spark.sql.connector.catalog;

public interface TableProvider {
  // Required: Infer the schema from options
  StructType inferSchema(CaseInsensitiveStringMap options);
  
  // Optional: Infer partitioning (default returns empty)
  default Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return new Transform[0];
  }
  
  // Required: Return a Table instance
  Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties);
  
  // Optional: Can this source accept external table metadata?
  default boolean supportsExternalMetadata() {
    return false;
  }
}
```

**Key Implementation Notes:**
- Implementations must have a public 0-arg constructor
- Only supports data operations (read, append, delete, overwrite), NOT metadata operations (create/drop tables)
- `getTable()` must return a Table configured with the provided schema, partitioning, and properties

---

### 2. Table (Logical Data Representation)
```java
package org.apache.spark.sql.connector.catalog;

public interface Table {
  // Required: A meaningful name to identify this table
  String name();
  
  // Required: Get schema (deprecated - use columns() instead)
  @Deprecated(since = "3.4.0")
  StructType schema();
  
  // Optional: Get columns (default derives from schema)
  default Column[] columns() {
    return CatalogV2Util.structTypeToV2Columns(schema());
  }
  
  // Optional: Get physical partitioning (default: empty)
  default Transform[] partitioning() {
    return new Transform[0];
  }
  
  // Optional: Get table properties (default: empty map)
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
  
  // Required: Declare what this table can do
  Set<TableCapability> capabilities();
}
```

**Mix-in Interfaces:**
- Implement `SupportsRead` to enable reading
- Implement `SupportsWrite` to enable writing

---

### 3. SupportsRead (Mix-in for Readable Tables)
```java
package org.apache.spark.sql.connector.catalog;

public interface SupportsRead extends Table {
  // Required: Build a scan for reading
  ScanBuilder newScanBuilder(CaseInsensitiveStringMap options);
}
```

---

### 4. SupportsWrite (Mix-in for Writable Tables)
```java
package org.apache.spark.sql.connector.catalog;

public interface SupportsWrite extends Table {
  // Required: Build a write for writing
  WriteBuilder newWriteBuilder(LogicalWriteInfo info);
}
```

---

## Read Pipeline Interfaces

### 5. ScanBuilder
```java
package org.apache.spark.sql.connector.read;

public interface ScanBuilder {
  // Required: Build and return the Scan
  Scan build();
}
```

**Can Mix-in:**
- `SupportsPushDownFilters` - Push down WHERE filters
- `SupportsPushDownRequiredColumns` - Column pruning
- `SupportsPushDownLimit` - Push down LIMIT
- `SupportsPushDownOffset` - Push down OFFSET
- `SupportsPushDownAggregates` - Push down aggregations
- `SupportsPushDownTopN` - Push down TOP-N (SORT + LIMIT)
- `SupportsPushDownTableSample` - Push down SAMPLE
- `SupportsPushDownV2Filters` - V2 filter push down
- `SupportsReportPartitioning` - Report partitioning info
- `SupportsReportOrdering` - Report ordering
- `SupportsReportStatistics` - Report statistics

---

### 6. Scan (Logical Read Representation)
```java
package org.apache.spark.sql.connector.read;

public interface Scan {
  // Required: Return the schema that will be read
  StructType readSchema();
  
  // Optional: Describe what filters/optimizations are applied
  default String description() {
    return this.getClass().toString();
  }
  
  // Required if BATCH_READ capability: Convert to physical batch read
  default Batch toBatch() {
    throw new UnsupportedOperationException();
  }
  
  // Optional if MICRO_BATCH_READ capability: For streaming micro-batches
  default MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    throw new UnsupportedOperationException();
  }
  
  // Optional if CONTINUOUS_READ capability: For continuous streaming
  default ContinuousStream toContinuousStream(String checkpointLocation) {
    throw new UnsupportedOperationException();
  }
  
  // Optional: Custom metrics support
  default CustomMetric[] supportedCustomMetrics() {
    return new CustomMetric[]{};
  }
  
  default CustomTaskMetric[] reportDriverMetrics() {
    return new CustomTaskMetric[]{};
  }
}
```

---

### 7. Batch (Physical Batch Read)
```java
package org.apache.spark.sql.connector.read;

public interface Batch {
  // Required: Describe the read
  String description();
  
  // Required: Build the actual readers for each partition
  PartitionReaderFactory createReaderFactory();
}
```

---

## Write Pipeline Interfaces

### 8. WriteBuilder
```java
package org.apache.spark.sql.connector.write;

public interface WriteBuilder {
  // Required: Build the Write object
  default Write build() {
    return new Write() {
      @Override
      public BatchWrite toBatch() {
        return buildForBatch();
      }
      
      @Override
      public StreamingWrite toStreaming() {
        return buildForStreaming();
      }
    };
  }
  
  // Deprecated: Use build() instead
  @Deprecated(since = "3.2.0")
  default BatchWrite buildForBatch() {
    throw new UnsupportedOperationException();
  }
  
  // Deprecated: Use build() instead
  @Deprecated(since = "3.2.0")
  default StreamingWrite buildForStreaming() {
    throw new UnsupportedOperationException();
  }
}
```

**Can Mix-in:**
- `RequiresDistributionAndOrdering` - Specify data distribution/ordering requirements
- `SupportsOverwriteV2` - Support overwrite by filter
- `SupportsDynamicOverwrite` - Support dynamic partition overwrite
- `SupportsTruncate` - Support table truncate
- `SupportsDelta` - Support delta writes

---

### 9. Write (Logical Write Representation)
```java
package org.apache.spark.sql.connector.write;

public interface Write {
  // Optional: Describe the write
  default String description() {
    return this.getClass().toString();
  }
  
  // Required if BATCH_WRITE capability: Convert to physical batch write
  default BatchWrite toBatch() {
    throw new UnsupportedOperationException();
  }
  
  // Optional if STREAMING_WRITE capability: For streaming
  default StreamingWrite toStreaming() {
    throw new UnsupportedOperationException();
  }
  
  // Optional: Custom metrics
  default CustomMetric[] supportedCustomMetrics() {
    return new CustomMetric[]{};
  }
  
  default CustomTaskMetric[] reportDriverMetrics() {
    return new CustomTaskMetric[]{};
  }
}
```

---

### 10. BatchWrite (Physical Batch Write)
```java
package org.apache.spark.sql.connector.write;

public interface BatchWrite {
  // Required: Create writer factory for each executor
  DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info);
  
  // Optional: Should Spark use commit coordinator? (default: true)
  default boolean useCommitCoordinator() {
    return true;
  }
  
  // Optional: Handle individual writer commits
  default void onDataWriterCommit(WriterCommitMessage message) {}
  
  // Required: Final commit with all commit messages
  void commit(WriterCommitMessage[] messages);
  
  // Required: Cleanup on failure
  void abort(WriterCommitMessage[] messages);
}
```

**Writing Flow:**
1. `createBatchWriterFactory(PhysicalWriteInfo)` - Called once on driver
2. Factory serialized to executors
3. `DataWriterFactory.createWriter(partitionId, taskId)` - Called per executor partition
4. `DataWriter.write(record)` - Called for each record
5. `DataWriter.commit()` - Called when partition is done
6. `BatchWrite.commit(messages)` or `BatchWrite.abort(messages)` - Final coordination

---

### 11. DataWriterFactory (Serializable Factory)
```java
package org.apache.spark.sql.connector.write;

public interface DataWriterFactory extends Serializable {
  // Required: Create a writer for a partition
  // partitionId: Unique RDD partition identifier
  // taskId: Task attempt ID from TaskContext.taskAttemptId()
  DataWriter<InternalRow> createWriter(int partitionId, long taskId);
}
```

---

### 12. DataWriter (Per-Partition Writer)
```java
package org.apache.spark.sql.connector.write;

public interface DataWriter<T> extends Closeable {
  // New in 4.0: Write record with metadata
  default void write(T metadata, T record) throws IOException {
    write(record);
  }
  
  // Required: Write a single record
  void write(T record) throws IOException;
  
  // New in 4.0: Write all records from iterator
  default void writeAll(Iterator<T> records) throws IOException {
    while (records.hasNext()) {
      write(records.next());
    }
  }
  
  // Required: Finalize and return commit message
  WriterCommitMessage commit() throws IOException;
  
  // Required: Cleanup on failure
  void abort() throws IOException;
  
  // Optional: Report custom task metrics
  default CustomTaskMetric[] currentMetricsValues() {
    return new CustomTaskMetric[]{};
  }
  
  // From Closeable:
  void close() throws IOException;
}
```

---

### 13. WriterCommitMessage (Serializable Message)
```java
package org.apache.spark.sql.connector.write;

// Empty interface - implementations define their own message structure
public interface WriterCommitMessage extends Serializable {}
```

**Example Implementation:**
```java
public class KustoWriterCommitMessage implements WriterCommitMessage {
  private static final long serialVersionUID = 1L;
  public String manifestPath;  // Or whatever data you need
  public int recordCount;
}
```

---

## Info Interfaces

### 14. LogicalWriteInfo (Write Configuration)
```java
package org.apache.spark.sql.connector.write;

public interface LogicalWriteInfo {
  // Required: User-specified write options
  CaseInsensitiveStringMap options();
  
  // Required: Unique query identifier
  String queryId();
  
  // Required: Input DataFrame schema
  StructType schema();
  
  // Optional: ID columns for row-level operations (4.0+)
  default Optional<StructType> rowIdSchema() {
    throw new UnsupportedOperationException();
  }
  
  // Optional: Metadata columns (4.0+)
  default Optional<StructType> metadataSchema() {
    throw new UnsupportedOperationException();
  }
}
```

---

### 15. PhysicalWriteInfo (Executor-Side Write Info)
```java
package org.apache.spark.sql.connector.write;

public interface PhysicalWriteInfo {
  // Required: Number of partitions in input data
  int numPartitions();
}
```

---

## Enum: TableCapability

```java
package org.apache.spark.sql.connector.catalog;

public enum TableCapability {
  BATCH_READ,              // Table supports batch reads
  MICRO_BATCH_READ,        // Table supports micro-batch streaming
  CONTINUOUS_READ,         // Table supports continuous streaming
  BATCH_WRITE,             // Table supports batch append writes
  STREAMING_WRITE,         // Table supports streaming append writes
  TRUNCATE,                // Table supports truncate
  OVERWRITE_BY_FILTER,     // Table supports filter-based overwrite
  OVERWRITE_DYNAMIC,       // Table supports dynamic partition overwrite
  ACCEPT_ANY_SCHEMA,       // Table accepts any input schema
  V1_BATCH_WRITE          // Table supports V1 InsertableRelation
}
```

---

## Typical Implementation Flow

### Minimal Read Implementation
1. Extend `TableProvider`
2. `inferSchema()` - analyze data and return schema
3. `getTable()` - return Table instance implementing `SupportsRead`
4. Table's `capabilities()` - return `{BATCH_READ}`
5. Table's `newScanBuilder()` - return `ScanBuilder`
6. `ScanBuilder.build()` - return `Scan` 
7. `Scan.toBatch()` - return `Batch`
8. `Batch.createReaderFactory()` - return `PartitionReaderFactory`

### Minimal Write Implementation
1. Extend `TableProvider`
2. `inferSchema()` - return input schema
3. `getTable()` - return Table implementing `SupportsWrite`
4. Table's `capabilities()` - return `{BATCH_WRITE}`
5. Table's `newWriteBuilder()` - return `WriteBuilder`
6. `WriteBuilder.build().toBatch()` - return `BatchWrite`
7. `BatchWrite.createBatchWriterFactory()` - return `DataWriterFactory`
8. `DataWriterFactory.createWriter()` - return `DataWriter`
9. `DataWriter.write()` - write records
10. `DataWriter.commit()` - return `WriterCommitMessage`
11. `BatchWrite.commit(messages)` - finalize on driver

---

## Key Traits from azure-kusto-spark

Current implementation uses V1 API:
- `CreatableRelationProvider` - for writes
- `RelationProvider` - for reads  
- `DataSourceRegister` - for registration

Should migrate to V2:
- `TableProvider` - new entry point
- `Table + SupportsRead/SupportsWrite` - new table abstraction
- Write pipeline: `WriteBuilder` → `Write` → `BatchWrite` → `DataWriterFactory` → `DataWriter`


# Spark 4.0 DataSourceV2 Implementation Summary

## Documents Generated

This directory now contains comprehensive documentation for implementing Spark 4.0 DataSourceV2 API for the Azure Kusto Connector:

1. **SPARK40_API_REFERENCE.txt** - Complete API reference with all interface signatures
2. **spark40_datasourcev2_interfaces.md** - Detailed interface documentation with flow diagrams
3. **migration_guide.md** - Side-by-side comparison of V1 vs V2 API with breaking changes
4. **code_example_skeleton.scala** - Full Scala code skeleton showing complete implementation pattern

## Quick Start

### Current State
The connector uses the **Spark V1 API**:
```
DefaultSource
├── CreatableRelationProvider
├── RelationProvider  
└── DataSourceRegister
```

### Target State
The connector should use the **Spark V2 API**:
```
TableProvider (+ DataSourceRegister)
├── inferSchema(options)
├── getTable(schema, partitioning, properties)
└── returns: Table + SupportsRead + SupportsWrite
   ├── SupportsRead → ScanBuilder → Scan → Batch → PartitionReaderFactory
   └── SupportsWrite → WriteBuilder → Write → BatchWrite → DataWriterFactory → DataWriter
```

## Key Interface Signatures (Exact from Spark 4.0)

### Entry Point
```scala
public interface TableProvider {
  StructType inferSchema(CaseInsensitiveStringMap options);
  Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties);
}
```

### Read Pipeline
```scala
ScanBuilder → Scan (readSchema) → Batch (createReaderFactory) → PartitionReader
```

### Write Pipeline
```scala
WriteBuilder → Write (toBatch) → BatchWrite (createBatchWriterFactory) 
  → DataWriterFactory → DataWriter (write, commit, abort)
```

## Critical Implementation Points

### 1. Schema Handling
- **inferSchema()** - Must connect to Kusto and analyze data to return StructType
- **getTable()** - Receives schema as parameter, must honor it
- **Table.schema()** - Must return same schema as inferSchema()

### 2. Capabilities Declaration
```scala
override def capabilities(): util.Set[TableCapability] = {
  util.EnumSet.of(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  )
}
```

### 3. Write Transaction Flow (CRITICAL)
```
Driver Side:
1. WriteBuilder.build() → Write
2. Write.toBatch() → BatchWrite
3. BatchWrite.createBatchWriterFactory(PhysicalWriteInfo) → DataWriterFactory
4. DataWriterFactory is serialized

Executor Side:
5. DataWriterFactory.createWriter(partitionId, taskId) → DataWriter
6. DataWriter.write(record) × N records
7. DataWriter.commit() → WriterCommitMessage
8. Messages serialized back to driver

Driver Side:
9. BatchWrite.commit(messages[]) OR BatchWrite.abort(messages[])
10. Final coordination (move temp files, verify ingests, etc.)
```

### 4. Read Data Flow
```
Driver:
1. ScanBuilder.build() → Scan
2. Scan.toBatch() → Batch
3. Batch.createReaderFactory() → PartitionReaderFactory

Executor (per partition):
4. PartitionReaderFactory.createReader(partition) → PartitionReader
5. Loop: PartitionReader.next() && PartitionReader.get() → InternalRow
```

## Package Structure

### Current Files to Refactor
```
datasource/
├── DefaultSource.scala          → Replace with KustoTableProvider
├── KustoRelation.scala          → Replace with KustoTable
├── KustoReader.scala            → Replace with Scan/Batch/PartitionReader
└── KustoFilter.scala

datasink/
├── KustoSinkProvider.scala      → Update for V2
└── KustoWriter.scala            → Replace with BatchWrite/DataWriter
```

### New V2 Structure to Create
```
datasource/v2/
├── catalog/
│   └── KustoTable.scala
├── read/
│   ├── KustoScanBuilder.scala
│   ├── KustoScan.scala
│   ├── KustoBatch.scala
│   └── KustoPartitionReader.scala
└── write/
    ├── KustoWriteBuilder.scala
    ├── KustoWrite.scala
    ├── KustoBatchWrite.scala
    ├── KustoDataWriter.scala
    └── KustoWriterCommitMessage.scala

KustoTableProvider.scala  (replaces DefaultSource)
```

## Imports Needed

```scala
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.DataSourceRegister

import java.io.IOException
import java.io.Serializable
import java.util
```

## Implementation Checklist

Phase 1: Basic Structure
- [ ] Create KustoTableProvider (replaces DefaultSource)
- [ ] Create KustoTable with SupportsRead + SupportsWrite
- [ ] Implement inferSchema() - connect to Kusto, return schema
- [ ] Implement getTable() - return KustoTable instance
- [ ] Set up capabilities() to return BATCH_READ + BATCH_WRITE

Phase 2: Read Path
- [ ] Create KustoScanBuilder
- [ ] Create KustoScan (implements Scan)
- [ ] Create KustoBatch (implements Batch)
- [ ] Create KustoPartitionReaderFactory
- [ ] Create KustoPartitionReader (actual data reading)
- [ ] Test reading from Kusto

Phase 3: Write Path
- [ ] Create KustoWriteBuilder
- [ ] Create KustoWrite
- [ ] Create KustoBatchWrite (transaction management)
- [ ] Create KustoDataWriterFactory
- [ ] Create KustoDataWriter (record writing)
- [ ] Create KustoWriterCommitMessage
- [ ] Implement commit() - finalize ingests
- [ ] Implement abort() - cleanup on failure
- [ ] Test writing to Kusto

Phase 4: Optimizations
- [ ] Add SupportsPushDownFilters for WHERE clause optimization
- [ ] Add SupportsPushDownRequiredColumns for column pruning
- [ ] Add SupportsPushDownLimit for LIMIT push down
- [ ] Add RequiresDistributionAndOrdering for write optimization

Phase 5: Cleanup & Testing
- [ ] Mark V1 classes as @Deprecated
- [ ] Update all tests to use V2 API
- [ ] Add integration tests
- [ ] Performance benchmarking
- [ ] Update user documentation

## Key Classes from Current Code

### Reuse/Adapt These
- `KustoCoordinates` - Already has kustoCluster, database info
- `KustoAuthentication` - Handle auth
- `KustoConnector` / `KustoClient` - Existing Kusto connectivity
- `KustoSourceOptions` - Parse read options
- `KustoSinkOptions` - Parse write options
- `KustoDataSourceUtils` - Helper methods

### Replace These
- `DefaultSource` → `KustoTableProvider`
- `KustoRelation` → `KustoTable`
- `KustoReader.buildScan()` → `PartitionReader`
- `KustoWriter.write()` → `DataWriter`

## Important Notes

1. **Serialization**: DataWriterFactory and WriterCommitMessage must be Serializable
2. **InternalRow**: All data types are `InternalRow`, not Row
3. **CaseInsensitiveStringMap**: All options are case-insensitive for keys
4. **No SQLContext**: V2 doesn't pass SQLContext, must get from SparkSession where needed
5. **Schema Consistency**: inferSchema() and getTable() schema must match exactly
6. **Executor Isolation**: DataWriters run on executors with no shared state
7. **Partitioning**: Each partition gets its own DataWriter, don't assume partition 0 is special

## Testing Strategy

1. Unit tests for each component
2. Integration tests with actual Kusto cluster
3. Test error scenarios:
   - Network failures during write
   - Partial writes that need abort
   - Large batch processing
   - Multiple concurrent writes
4. Performance regression tests
5. Backward compatibility tests (V1 API still works)

## Migration Path for Users

1. Create V2 implementation alongside V1
2. Keep both APIs working simultaneously
3. V2 takes precedence if available
4. Deprecate V1 in next major version
5. Remove V1 in version after that

Users can test with new connector without breaking existing code:
```scala
// Both work during transition period
spark.read.format("kusto").options(...).load()  // Routes to V2
spark.write.format("kusto").options(...).save() // Routes to V2
```

## Additional Resources

- Spark Documentation: https://spark.apache.org/docs/latest/sql-data-sources-generic.html
- Official DataSourceV2 Guide: https://issues.apache.org/jira/browse/SPARK-27961
- CSV DataSource Example: Look at `org.apache.spark.sql.execution.datasources.v2.csv` in Spark source

## Questions During Implementation

1. **How to handle authentication credentials?**
   - Pass through options/LogicalWriteInfo to each writer
   - Or store in thread-local context

2. **How to manage temporary storage?**
   - Use options to specify temp location
   - Create per-writer temp directories

3. **How to handle retries?**
   - DataWriter can fail, Spark will retry with new taskId
   - Must handle idempotency

4. **How to report progress?**
   - Use onDataWriterCommit() for progress updates
   - Return CustomTaskMetric[] for metrics

5. **What about streaming?**
   - Not in this iteration, focus on batch
   - V2 supports STREAMING_WRITE but it's optional

## Files in This Directory

```
connector/
├── SPARK40_API_REFERENCE.txt           ← Start here for signatures
├── spark40_datasourcev2_interfaces.md  ← Detailed interface docs
├── migration_guide.md                  ← V1 vs V2 comparison
├── code_example_skeleton.scala         ← Implementation template
├── IMPLEMENTATION_SUMMARY.md           ← This file
│
└── src/main/scala/com/microsoft/kusto/spark/datasource/
    ├── DefaultSource.scala             ← Current (V1)
    │
    └── v2/                             ← NEW - create this
        ├── KustoTableProvider.scala
        ├── catalog/KustoTable.scala
        ├── read/...
        └── write/...
```

---

**Document Generated:** March 10, 2025  
**Spark Version:** 4.0.0  
**Scala Version:** 2.13.16

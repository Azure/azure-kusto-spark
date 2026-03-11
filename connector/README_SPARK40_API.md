# Spark 4.0 DataSourceV2 API - Complete Implementation Guide

## Overview

This directory contains **complete, exact interface signatures and implementation guidelines** for migrating the Azure Kusto Connector from Spark 3.x V1 API to Spark 4.0 V2 API.

All interfaces are extracted directly from **Apache Spark 4.0.0** source code and are ready for implementation.

## 📚 Documentation Files

### 1. **START HERE** → `IMPLEMENTATION_SUMMARY.md`
- **Purpose**: High-level overview and quick start
- **Contains**: Current state vs target state, critical points, file structure, checklist
- **Read time**: 10-15 minutes
- **Best for**: Understanding the big picture before diving into code

### 2. `SPARK40_API_REFERENCE.txt`
- **Purpose**: Complete API signatures and flow diagrams
- **Contains**: All 16 core interfaces with exact method signatures
- **Format**: Quick reference with minimal explanation
- **Best for**: Looking up exact method signatures while coding

### 3. `spark40_datasourcev2_interfaces.md`
- **Purpose**: Detailed documentation of each interface
- **Contains**: Full interface documentation with implementation notes
- **Length**: ~1,900 lines with examples
- **Best for**: Understanding interface purposes and requirements

### 4. `migration_guide.md`
- **Purpose**: Side-by-side V1 vs V2 comparison
- **Contains**: Breaking changes, before/after code, benefits
- **Format**: Markdown tables and code examples
- **Best for**: Understanding what needs to change from current code

### 5. `code_example_skeleton.scala`
- **Purpose**: Complete implementation template
- **Contains**: Full Scala code for all classes (ready to fill in logic)
- **Format**: Runnable code with inline comments
- **Best for**: Copy-paste starting point for implementation

## 🔑 Key Information At A Glance

### Core Pattern
```
V1 (Current)                          V2 (New)
───────────────────────────────────────────────────
DefaultSource                         TableProvider
├─ createRelation()                 ├─ inferSchema()
└─ returns BaseRelation             └─ getTable()
                                       ↓
                                    Table
                                    ├─ SupportsRead
                                    │  └─ Scan → Batch → Reader
                                    └─ SupportsWrite
                                       └─ Write → BatchWrite → Writer
```

### Critical Flows

**WRITE (Transaction):**
```
Driver:   WriteBuilder → Write → BatchWrite → createFactory (serialized)
Executor: DataWriterFactory → DataWriter → write() × N → commit()
Driver:   BatchWrite.commit(messages) ← (all executors done)
```

**READ (Scanning):**
```
Driver:   ScanBuilder → Scan → Batch → createFactory (serialized)
Executor: PartitionReaderFactory → PartitionReader → next() × N
```

### TableCapability Enum
```java
BATCH_READ           // Batch reads
MICRO_BATCH_READ     // Streaming micro-batches
CONTINUOUS_READ      // Streaming continuous
BATCH_WRITE          // Batch appends
STREAMING_WRITE      // Streaming appends
TRUNCATE             // Truncate operation
OVERWRITE_BY_FILTER  // Overwrite where condition
OVERWRITE_DYNAMIC    // Dynamic partition overwrite
ACCEPT_ANY_SCHEMA    // Any schema OK
V1_BATCH_WRITE       // V1 compatibility
```

For Kusto: Return `{BATCH_READ, BATCH_WRITE}`

## 📋 Implementation Checklist

### Phase 1: Basic Structure
- [ ] Create `KustoTableProvider` (replaces DefaultSource)
- [ ] Create `KustoTable` with `SupportsRead + SupportsWrite`
- [ ] Implement `inferSchema()` 
- [ ] Implement `getTable()`
- [ ] Set up `capabilities()`

### Phase 2: Read Path
- [ ] `KustoScanBuilder` → `KustoScan` → `KustoBatch` → `KustoPartitionReaderFactory` → `KustoPartitionReader`
- [ ] Test reading

### Phase 3: Write Path
- [ ] `KustoWriteBuilder` → `KustoWrite` → `KustoBatchWrite` → `KustoDataWriterFactory` → `KustoDataWriter`
- [ ] Implement `commit()` and `abort()` logic
- [ ] Test writing

### Phase 4: Optimizations
- [ ] `SupportsPushDownFilters`
- [ ] `SupportsPushDownRequiredColumns`
- [ ] `SupportsPushDownLimit`

### Phase 5: Testing & Cleanup
- [ ] Unit tests
- [ ] Integration tests
- [ ] Mark V1 classes as `@Deprecated`
- [ ] Update docs

## 🎯 Quick Implementation Tips

### Imports Needed
```scala
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.InternalRow
```

### Key Constraints
1. **Serialization**: DataWriterFactory and WriterCommitMessage must be Serializable
2. **Schema Consistency**: `inferSchema()` and `getTable()` schema must match exactly
3. **InternalRow**: All data is `InternalRow`, not `Row`
4. **Idempotency**: DataWriters may be retried with same data
5. **No SQLContext**: V2 doesn't provide it; use SparkSession if needed

### Common Mistakes
- ❌ Using non-Serializable objects in DataWriterFactory
- ❌ InternalRow schema mismatch between inferSchema and getTable
- ❌ Forgetting to implement commit() or abort() in BatchWrite
- ❌ Assuming partition 0 is special or using global state
- ❌ Not handling empty partitions in DataWriter

## 📖 Which File To Read First

**For implementation leaders/architects:**
1. Read `IMPLEMENTATION_SUMMARY.md` (overview)
2. Skim `SPARK40_API_REFERENCE.txt` (signatures)
3. Review `migration_guide.md` (breaking changes)

**For developers doing the coding:**
1. Read `IMPLEMENTATION_SUMMARY.md` (understand structure)
2. Open `code_example_skeleton.scala` (copy template)
3. Reference `SPARK40_API_REFERENCE.txt` (check exact signatures)
4. Consult `spark40_datasourcev2_interfaces.md` (deep dive on interfaces)

**For code reviewers:**
1. Read `migration_guide.md` (understand changes)
2. Check `code_example_skeleton.scala` (verify pattern)
3. Cross-reference `SPARK40_API_REFERENCE.txt` (validate signatures)

## 🔗 Cross-References

### By Interface

| Interface | Signature | Example | Notes |
|-----------|-----------|---------|-------|
| TableProvider | `inferSchema()`, `getTable()` | Section 1 in skeleton | Entry point |
| Table | `name()`, `schema()`, `capabilities()` | Section 2 | Implemented by KustoTable |
| SupportsRead | `newScanBuilder()` | Section 3 | Mix-in interface |
| SupportsWrite | `newWriteBuilder()` | Section 4 | Mix-in interface |
| ScanBuilder | `build()` | Section 3 | Builds Scan |
| Scan | `readSchema()`, `toBatch()` | Section 3 | Logical read |
| Batch | `createReaderFactory()` | Section 3 | Physical read |
| PartitionReader | `next()`, `get()` | Section 3 | Actual reading |
| WriteBuilder | `build()` | Section 4 | Builds Write |
| Write | `toBatch()` | Section 4 | Logical write |
| BatchWrite | `commit()`, `abort()` | Section 4 | Physical write |
| DataWriter | `write()`, `commit()` | Section 4 | Executor-side |
| WriterCommitMessage | *empty interface* | Section 4 | Custom message |

### Current Code Classes to Reuse

| Class | Location | Use in V2 |
|-------|----------|-----------|
| `KustoCoordinates` | `common/` | Pass to all classes |
| `KustoAuthentication` | `authentication/` | Pass to all classes |
| `KustoConnector` | `datasource/` | Use in schema inference & reading |
| `KustoSourceOptions` | `datasource/` | Parse read options |
| `KustoSinkOptions` | `datasink/` | Parse write options |
| `KustoFilter` | `datasource/` | Adapt for filter push-down |
| `KustoReader` | `datasource/` | Refactor into PartitionReader |
| `KustoWriter` | `datasink/` | Refactor into DataWriter |

## 📝 Implementation Examples

### Minimal TableProvider
```scala
class KustoTableProvider extends TableProvider with DataSourceRegister {
  override def shortName(): String = "kusto"
  
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // Connect to Kusto, analyze, return schema
  }
  
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    new KustoTable(schema, ...)
  }
}
```

### Minimal Table
```scala
class KustoTable(...) extends Table with SupportsRead with SupportsWrite {
  override def name(): String = "db.table"
  override def schema(): StructType = schema
  override def capabilities(): util.Set[TableCapability] = 
    util.EnumSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE)
  override def newScanBuilder(opts) = new KustoScanBuilder(...)
  override def newWriteBuilder(info) = new KustoWriteBuilder(...)
}
```

### Minimal DataWriter
```scala
class KustoDataWriter(...) extends DataWriter[InternalRow] {
  override def write(record: InternalRow): Unit = {
    // Add record to batch
  }
  
  override def commit(): WriterCommitMessage = {
    // Ingest batch, return message
    new KustoWriterCommitMessage(path, count)
  }
  
  override def abort(): Unit = {
    // Cleanup temp files
  }
  
  override def close(): Unit = {}
}
```

## 🔍 Debugging Tips

**If schema inference fails:**
- Check that `inferSchema()` actually connects to Kusto
- Verify returned StructType is not empty
- Ensure same schema in `getTable()`

**If read doesn't work:**
- Check `PartitionReaderFactory.createReader()` is called
- Verify `PartitionReader.next()` and `get()` logic
- Test with small dataset first

**If write fails:**
- Check `DataWriterFactory` is Serializable
- Verify `DataWriter.write()` is called for each record
- Check `commit()` is called and returns message
- Verify `BatchWrite.commit()` receives messages

**If executor crashes:**
- Check `WriterCommitMessage` is Serializable
- Verify auth credentials are available on executors
- Check for unSerializable objects in factory

## 📚 Additional Resources

- **Spark Docs**: https://spark.apache.org/docs/latest/sql-data-sources-generic.html
- **DataSourceV2 Design**: https://issues.apache.org/jira/browse/SPARK-27961
- **Spark Source**: `org/apache/spark/sql/connector/` in Spark repo
- **CSV Example**: `org/apache/spark/sql/execution/datasources/v2/csv/` in Spark source
- **Parquet Example**: `org/apache/spark/sql/execution/datasources/v2/parquet/` in Spark source

## ✅ Validation Checklist

Before submitting for review:
- [ ] All 5 documents present in connector directory
- [ ] Code follows interfaces exactly from SPARK40_API_REFERENCE.txt
- [ ] No compilation errors
- [ ] Unit tests pass
- [ ] Integration tests pass with Kusto
- [ ] Error handling implemented (abort, cleanup)
- [ ] Serialization works (DataWriterFactory, WriterCommitMessage)
- [ ] Read and write both work
- [ ] No deprecated V1 code in V2 implementations

---

**Version**: 1.0  
**Spark**: 4.0.0  
**Scala**: 2.13.16  
**Generated**: March 10, 2025

For questions or clarifications, refer to the specific document sections above.

# Migration Guide: DefaultSource (V1) → TableProvider (V2)

## Current V1 Implementation (DefaultSource.scala)

The current `DefaultSource` implements:
- `CreatableRelationProvider` - `createRelation(sqlContext, mode, parameters, data)`
- `RelationProvider` - `createRelation(sqlContext, parameters)`
- `DataSourceRegister` - `shortName()`

## What Needs to Change

### 1. Main Entry Point Class

**BEFORE (V1):**
```scala
class DefaultSource extends CreatableRelationProvider with RelationProvider with DataSourceRegister
```

**AFTER (V2):**
```scala
class DefaultSource extends TableProvider with DataSourceRegister
```

Key differences:
- Remove `CreatableRelationProvider` and `RelationProvider`
- Add `TableProvider` 
- Keep `DataSourceRegister` (still needed for registration)

### 2. Read Operation

**BEFORE (V1):**
```scala
override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
  val readOptions = KDSU.getReadParameters(parameters, sqlContext)
  // ... parse auth, storage params ...
  KustoRelation(
    kustoCoordinates,
    kustoAuthentication.get,
    parameters.getOrElse(KustoSourceOptions.KUSTO_QUERY, ""),
    readOptions,
    timeout,
    parameters.get(KustoSourceOptions.KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES),
    mergedStorageParameters,
    clientRequestProperties,
    requestId.get)(sqlContext.sparkSession)
}
```

**AFTER (V2):**
```scala
override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
  // Parse options and connect to Kusto to infer schema
  // Return StructType of the table/query result
}

override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]): Table = {
  new KustoTable(
    schema,
    kustoCoordinates,
    kustoAuthentication,
    queryId,
    options,
    sparkSession)
}
```

### 3. Write Operation

**BEFORE (V1):**
```scala
override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {
  val sinkParameters = KDSU.parseSinkParameters(parameters, mode)
  
  KustoWriter.write(
    None,
    data,
    kustoCoordinates,
    authenticationParameters.get,
    sinkParameters.writeOptions,
    clientRequestProperties.get)
  
  // Return a relation for reading back the written data
}
```

**AFTER (V2):**
```scala
// In KustoTable that implements SupportsWrite:
override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
  new WriteBuilder {
    override def build(): Write = {
      new Write {
        override def toBatch(): BatchWrite = new KustoBatchWrite(
          kustoCoordinates,
          kustoAuthentication,
          info.schema(),
          info.options())
      }
    }
  }
}
```

### 4. Table Implementation

**NEW (V2):**
```scala
class KustoTable(
    val tableSchema: StructType,
    kustoCoordinates: KustoCoordinates,
    kustoAuthentication: KustoAuthentication,
    queryId: String,
    options: CaseInsensitiveStringMap,
    sparkSession: SparkSession) 
  extends Table 
  with SupportsRead 
  with SupportsWrite {
  
  override def name(): String = 
    s"${kustoCoordinates.database}.${options.get("kusto.table")}"
  
  override def schema(): StructType = tableSchema
  
  override def capabilities(): util.Set[TableCapability] = 
    util.EnumSet.of(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE)
  
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = 
    new KustoScanBuilder(tableSchema, kustoCoordinates, kustoAuthentication, options)
  
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = 
    new KustoWriteBuilder(kustoCoordinates, kustoAuthentication, info)
}
```

### 5. Scan Pipeline (for reads)

**NEW (V2):**
```scala
class KustoScanBuilder(schema: StructType, ...) extends ScanBuilder {
  override def build(): Scan = 
    new KustoScan(schema, ...)
}

class KustoScan(schema: StructType, ...) extends Scan {
  override def readSchema(): StructType = schema
  
  override def description(): String = 
    s"Kusto($database.$table) query=$query"
  
  override def toBatch(): Batch = 
    new KustoBatch(...)
}

class KustoBatch extends Batch {
  override def createReaderFactory(): PartitionReaderFactory = 
    new KustoPartitionReaderFactory(...)
}
```

### 6. Write Pipeline (for writes)

**NEW (V2):**
```scala
class KustoWriteBuilder(...) extends WriteBuilder {
  override def build(): Write = new KustoWrite(...)
}

class KustoWrite(...) extends Write {
  override def toBatch(): BatchWrite = new KustoBatchWrite(...)
}

class KustoBatchWrite(...) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = 
    new KustoDataWriterFactory(...)
  
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // All partitions written, finalize on driver
    val commitMessages = messages.collect { case m: KustoWriterCommitMessage => m }
    // Finalize Kusto ingest
  }
  
  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // Cleanup on failure
  }
}

class KustoDataWriterFactory(...) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = 
    new KustoDataWriter(...)
}

class KustoDataWriter(...) extends DataWriter[InternalRow] {
  override def write(record: InternalRow): Unit = {
    // Convert InternalRow to Kusto format and add to batch
  }
  
  override def commit(): WriterCommitMessage = {
    // Ingest current batch to temporary location
    // Return manifest path or other metadata
    new KustoWriterCommitMessage(manifestPath, recordCount)
  }
  
  override def abort(): Unit = {
    // Cleanup temporary files
  }
  
  override def close(): Unit = {
    // Close resources
  }
}

class KustoWriterCommitMessage(
    val manifestPath: String,
    val recordCount: Int) 
  extends WriterCommitMessage
```

## Breaking Changes

| Aspect | V1 | V2 |
|--------|----|----|
| **Entry Point** | `CreatableRelationProvider`/`RelationProvider` | `TableProvider` |
| **Read Return** | `BaseRelation` | `Table` + `SupportsRead` |
| **Write Return** | `BaseRelation` | `Table` + `SupportsWrite` |
| **Reader Type** | RDD-based with `BaseRelation.buildScan()` | Partition-based with `PartitionReaderFactory` |
| **Write Pattern** | Single `KustoWriter.write()` call | Multi-stage: Writer Factory → Writer → Commit |
| **Schema** | Inferred/provided to relation | Inferred separately in `inferSchema()` |
| **Transaction Support** | File-based | Explicit commit/abort messages |

## Benefits of V2

1. **Better Performance**: Partition-level readers instead of RDD-based
2. **More Control**: Explicit commit/abort gives transaction control
3. **Better Streaming**: Support for micro-batch and continuous streams
4. **Predicate Pushdown**: Built-in support for filtering at source
5. **Column Pruning**: Native support for selecting only needed columns
6. **Metadata Support**: Proper table metadata handling

## Files That Need Changes

```
Current Structure:
├── DefaultSource.scala          → Keep but refactor to delegate to TableProvider
├── KustoRelation.scala          → Can remove (replaced by KustoTable)
├── KustoReader.scala            → Refactor into KustoScanBuilder/KustoScan
├── KustoSinkProvider.scala      → Implement TableProvider for streaming
└── datasink/
    └── KustoWriter.scala        → Refactor into KustoBatchWrite/DataWriter

New V2 Structure:
├── KustoTableProvider.scala     → Main TableProvider implementation
├── catalog/
│   └── KustoTable.scala         → Table + SupportsRead + SupportsWrite
├── read/
│   ├── KustoScanBuilder.scala
│   ├── KustoScan.scala
│   ├── KustoBatch.scala
│   └── KustoPartitionReaderFactory.scala
└── write/
    ├── KustoWriteBuilder.scala
    ├── KustoWrite.scala
    ├── KustoBatchWrite.scala
    ├── KustoDataWriterFactory.scala
    ├── KustoDataWriter.scala
    └── KustoWriterCommitMessage.scala
```

## Implementation Priority

1. **Phase 1**: Create KustoTableProvider + KustoTable + basic ScanBuilder/ScanWriter
2. **Phase 2**: Implement PartitionReaderFactory for reads
3. **Phase 3**: Implement BatchWrite + DataWriter for writes
4. **Phase 4**: Add optimization mix-ins (PushDownFilters, RequiresDistribution, etc.)
5. **Phase 5**: Deprecate old V1 implementation


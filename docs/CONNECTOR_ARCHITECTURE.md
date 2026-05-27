# Azure Kusto Spark Connector — Architecture & Feature Documentation

## Table of Contents
1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Write Path (Data Sink)](#3-write-path-data-sink)
4. [Read Path (Data Source)](#4-read-path-data-source)
5. [Authentication](#5-authentication)
6. [Infrastructure & Utilities](#6-infrastructure--utilities)
7. [Configuration Options](#7-configuration-options)
8. [Efficiency Analysis & Improvement Recommendations](#8-efficiency-analysis--improvement-recommendations)

---

## 1. Overview

The Azure Kusto Spark Connector enables bidirectional data flow between Apache Spark and Azure Data Explorer (Kusto). It implements Spark's DataSource V1 API and provides:

- **Batch reads** — query Kusto and load results into Spark DataFrames
- **Batch writes** — write Spark DataFrames to Kusto tables
- **Structured streaming sink** — micro-batch streaming writes to Kusto

**Key dependencies:**
- `kusto-data` SDK v8.0.1 (query/command execution)
- `kusto-ingest` SDK v8.0.1 (queued + managed streaming ingestion)
- Azure Storage SDK (blob intermediary for reads/writes)
- Spark 4.x (master) / Spark 3.x (release/spark3)

**Registration:**
- Read: `spark.read.format("kusto")` or `com.microsoft.kusto.spark.datasource`
- Write: `df.write.format("kusto")` or `com.microsoft.kusto.spark.datasink`
- Streaming: `.format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")` (shortName: `KustoSink`)

---

## 2. Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                         Spark Application                          │
├────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐    ┌──────────────────┐   ┌───────────────┐  │
│  │   DefaultSource  │    │ KustoSinkProvider│   │ SparkExtension│  │
│  │   (Read Provider)│    │ (Write Provider) │   │  (SQL ext)    │  │
│  └────────┬─────────┘    └────────┬─────────┘   └───────────────┘  │
│           │                       │                                │
│  ┌────────▼─────────┐    ┌───────▼──────────┐                      │
│  │  KustoRelation   │    │   KustoWriter    │                      │
│  │  (schema, scan)  │    │  (orchestration) │                      │
│  └────────┬─────────┘    └───────┬──────────┘                      │
│           │                      │                                 │
│  ┌────────▼─────────┐    ┌───────▼──────────┐                      │
│  │   KustoReader    │    │  FinalizeHelper  │                      │
│  │ (single/distrib.)│    │ (extent mgmt)    │                      │
│  └────────┬─────────┘    └───────┬──────────┘                      │
├───────────┼──────────────────────┼─────────────────────────────────┤
│           │         Shared Infrastructure                          │
│  ┌────────▼───────────────────────▼──────────┐                     │
│  │         ExtendedKustoClient               │                     │
│  │  (wraps SDK engine + ingest clients)      │                     │
│  └────────┬──────────────────────────────────┘                     │
│           │                                                        │
│  ┌────────▼──────────┐  ┌──────────────────┐  ┌─────────────────┐  │
│  │ KustoClientCache  │  │ ContainerProvider│  │ CslCommandsGen  │  │
│  │ (singleton cache) │  │ (blob containers)│  │ (KQL commands)  │  │
│  └───────────────────┘  └──────────────────┘  └─────────────────┘  │
├────────────────────────────────────────────────────────────────────┤
│                    Kusto Java SDK (kusto-data + kusto-ingest)      │
│  ┌──────────────┐  ┌──────────────────┐  ┌───────────────────────┐ │
│  │ Client(query)│  │QueuedIngestClient│  │ManagedStreamingClient │ │
│  └──────────────┘  └──────────────────┘  └───────────────────────┘ │
├────────────────────────────────────────────────────────────────────┤
│            Azure Blob Storage (intermediary)                       │
└────────────────────────────────────────────────────────────────────┘
```

---

## 3. Write Path (Data Sink)

### 3.1 Write Modes

| Mode | Description | Temp Table | Atomicity |
|------|-------------|-----------|-----------|
| **Transactional** (default) | Write to temp table → move extents to target | Yes | Atomic (all-or-nothing) |
| **Queued** | Write directly to target via queued ingestion | No | Eventually consistent |
| **KustoStreaming** | Stream directly to Kusto engine (≤4MB batches) | No | Per-batch |

### 3.2 End-to-End Write Flow

```
DataFrame
  │
  ▼
KustoWriter.write()
  ├── Get/create ExtendedKustoClient (cached)
  ├── Generate temp table name
  ├── Fetch target schema from Kusto
  ├── Adjust schema / create CSV mapping
  ├── Create temp table (Transactional mode)
  │
  ▼
rdd.foreachPartition (per Spark partition)
  │
  ├─── [Queued/Transactional Path]
  │     ├── Create GZipped blob writer
  │     ├── Serialize rows to CSV (row-by-row)
  │     ├── Seal blob when size >= batchLimit * 1MB
  │     ├── Upload blob to Azure Storage
  │     └── Call ingestClient.ingestFromBlob()
  │
  └─── [Streaming Path]
        ├── Serialize rows to in-memory CSV buffer
        ├── Split if buffer > 4MB threshold
        └── Call managedStreamingClient.ingestFromStream()
  │
  ▼
FinalizeHelper (Transactional mode only)
  ├── Poll ingestion results until success/failure
  ├── Disable merge/rebuild on temp table
  ├── Move extents: temp table → destination
  └── Cleanup temp table and artifacts
```

### 3.3 CSV Serialization (`RowCSVWriterUtils`)

All data is serialized to CSV format before ingestion:
- **Primitives**: direct string representation
- **Binary**: Base64 encoded
- **Timestamps/Dates**: ISO format
- **Structs/Arrays/Maps**: JSON serialization via `EscapedWriter`
- **Decimals**: quoted non-scientific notation

### 3.4 Blob Ingestion Details

- One GZipped blob per Spark partition (or multiple if partition exceeds `batchLimit * 1MB`)
- Blobs are uploaded to containers obtained via `ContainerProvider`
- Containers are either service-managed (from Kusto DM) or user-provided
- `QueuedIngestClient.ingestFromBlob()` sends blob reference to Kusto DM for ingestion

### 3.5 Streaming Ingestion Details

- Uses `ManagedStreamingIngestClient` from the Java SDK
- Max uncompressed batch size: 4MB (configurable via `KustoConstants.DefaultMaxStreamingBytesUncompressed`)
- Large partitions are split into multiple streaming batches
- No blob intermediary — data flows directly to Kusto engine
- Certain properties are forbidden in streaming mode (tags, creation time, CSV mapping)

### 3.6 Transactional Finalization

1. Poll `.show operations` until all ingestion operations complete
2. Disable merge/rebuild policy on temp table
3. Optionally drop deduplication tags
4. Move extents from temp table to destination (batched, with retry/backoff)
5. Cleanup temp table and byproducts

### 3.7 Kusto Java SDK Usage (Write Path)

| SDK Class | Usage |
|-----------|-------|
| `IngestClientFactory.createClient()` | Creates `QueuedIngestClient` |
| `IngestClientFactory.createManagedStreamingIngestClient()` | Creates `ManagedStreamingIngestClient` |
| `QueuedIngestClient.ingestFromBlob()` | Blob-based queued ingestion |
| `ManagedStreamingIngestClient.ingestFromStream()` | Direct streaming ingestion |
| `IngestionProperties` | Ingestion configuration (format, mapping, tags) |
| `BlobSourceInfo` | Blob reference for queued ingestion |
| `StreamSourceInfo` | Stream reference for streaming ingestion |

---

## 4. Read Path (Data Source)

### 4.1 Read Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Single** | Direct query execution, in-memory deserialization | Small results (≤5000 rows default) |
| **Distributed** | Export to blob as Parquet, then Spark reads from blob | Large results |

### 4.2 End-to-End Read Flow

```
spark.read.format("kusto").load()
  │
  ▼
DefaultSource.createRelation()
  ├── Parse parameters (auth, storage, options)
  └── Return KustoRelation
  │
  ▼
KustoRelation.buildScan(columns, filters)
  ├── Apply predicate pushdown (KustoFilter)
  ├── Apply column pruning
  ├── Decide mode:
  │     - Explicit readMode wins
  │     - Else: estimate row count → single if ≤5000, else distributed
  │
  ├─── [Single Mode]
  │     ├── Execute query via SDK Client
  │     ├── KustoResponseDeserializer: rows → Spark Rows
  │     └── Return RDD[Row]
  │
  └─── [Distributed Mode]
        ├── Setup blob auth (WASBS/ABFS/SAS/Key)
        ├── Generate export partitions (hash-based)
        ├── Execute `.export` command per partition
        ├── Wait for async completion
        ├── Spark reads exported Parquet files
        └── Return RDD[Row]
```

### 4.3 Predicate Pushdown (`KustoFilter`)

Supported Spark filters pushed down to Kusto query:
- Comparison: `=`, `>`, `>=`, `<`, `<=`
- Null checks: `IS NULL`, `IS NOT NULL`
- Set membership: `IN`
- String ops: `startswith_cs`, `endswith_cs`, `contains_cs`
- Logical: `AND`, `OR`, `NOT`

### 4.4 Column Pruning

Only requested columns are projected in the Kusto query. Timespan columns are converted via `tostring()`.

### 4.5 Distributed Read — Export to Blob

- Partitioning: only `hash(column, N)` strategy supported
- Export format: Parquet
- Export commands issued per partition with unique directory
- Results cached optionally (`distributedReadModeTransientCache`)
- Spark reads exported Parquet via standard Spark Parquet reader

### 4.6 Schema Inference

- Custom schema: user provides via `customSchema` option (DDL string)
- Auto schema: fetches from Kusto via `.show table schema` / query result metadata
- Type mapping: `DataTypeMapping.KustoTypeToSparkTypeMap`

### 4.7 Kusto Java SDK Usage (Read Path)

| SDK Class | Usage |
|-----------|-------|
| `Client` | Execute queries and management commands |
| `ClientRequestProperties` | Query options (timeout, parameters) |
| `KustoResultSetTable` | Query result deserialization |
| `ConnectionStringBuilder` | Client authentication setup |

---

## 5. Authentication

All authentication modes supported:

| Mode | Class | Description |
|------|-------|-------------|
| AAD Application | `AadApplicationAuthentication` | Client ID + Secret + Authority |
| AAD Certificate | `AadApplicationCertificateAuthentication` | Client ID + PFX cert |
| Managed Identity | `ManagedIdentityAuthentication` | System or user-assigned MI |
| Access Token | `KustoAccessTokenAuthentication` | Pre-obtained token |
| Token Provider | `KustoTokenProviderAuthentication` | Callback function |
| User Prompt | `KustoUserPromptAuthentication` | Interactive device code |
| Key Vault | `KeyVaultAuthentication` | Fetch credentials from AKV |
| Azure CLI | (fallback) | Uses `az` CLI credential |

Authentication is used to construct `ConnectionStringBuilder` instances for both engine and ingest clients.

---

## 6. Infrastructure & Utilities

### 6.1 `KustoClientCache`
- `ConcurrentHashMap[ClusterAndAuth, ExtendedKustoClient]`
- Cache key: engine URL + auth + ingest URI + cluster alias
- Lazily creates clients on first access
- Ingest URI auto-derived: `https://ingest-<engine-host>` if not provided

### 6.2 `ExtendedKustoClient`
Wraps SDK clients with higher-level operations:
- Lazy client instantiation (engine, DM, queued ingest, streaming ingest)
- Table creation from Spark schema
- Retry with exponential backoff (resilience4j)
- Extent movement with adaptive batch shrinking
- Async operation polling
- Deduplication tag management
- Mapping propagation to staging tables
- Two `ContainerProvider` instances (ingestion + export)

### 6.3 `ContainerProvider`
Two modes:
1. **Service-managed**: fetches containers from Kusto DM resource manager
2. **User-provided**: uses storage params with SAS token generation/cache

Features:
- Round-robin container selection
- SAS token caching with expiry
- User-delegation SAS via Azure Storage SDK + MSI
- Retry with resilience4j

### 6.4 `CslCommandsGenerator`
Generates KQL management commands:
- Table create/drop
- Batching policy alter
- Move extents (sync/async)
- Export to Parquet with SAS/key/impersonation
- Retention/auto-delete/streaming policy
- Async operation status queries
- Temp storage/container commands

### 6.5 `KustoDataSourceUtils`
Main utility hub:
- Read/write option parsing
- Schema extraction and fetch
- Cluster URL normalization
- Async polling (`doWhile` with exponential backoff)
- Error reporting
- Connector version metadata

### 6.6 `DataTypeMapping`
Bidirectional type conversion:
- Kusto → Spark: `string→StringType`, `datetime→TimestampType`, `dynamic→StringType`, etc.
- Spark → Kusto: `DecimalType→decimal`, `ArrayType→dynamic`, `MapType→dynamic`, etc.

### 6.7 `KustoAzureFsSetupCache`
Caches Azure filesystem auth configuration for Spark:
- Storage account keys/SAS per account/container
- Prevents repeated Hadoop FS reconfiguration

### 6.8 `KustoConstants`
Key defaults:
| Constant | Value |
|----------|-------|
| Direct query row upper bound | 5,000 |
| Default batching limit | 300 |
| Max streaming bytes (uncompressed) | 4 MB |
| Storage SAS expiry | 2 hours |
| Move extents sleep cap | 3 minutes |
| Long-running wait timeout | 2 days |

---

## 7. Configuration Options

### 7.1 Read Options

| Option | Description |
|--------|-------------|
| `kustoCluster` | Cluster URL (required) |
| `kustoDatabase` | Database name (required) |
| `kustoQuery` | KQL query to execute |
| `customSchema` | User-provided schema (DDL) |
| `readMode` | `ForceSingleMode` / `ForceDistributedMode` |
| `transientStorage` | Blob storage for distributed reads (JSON) |
| `queryFilterPushDown` | Enable/disable predicate pushdown |
| `storageProtocol` | `wasbs` / `abfs` / `abfss` |
| `distributedReadModeTransientCache` | Cache export paths |
| `kustoExportOptionsJson` | Export options JSON |
| `clientRequestPropertiesJson` | CRP (timeout, parameters) |
| `timeout` | Operation timeout |

### 7.2 Write Options

| Option | Description |
|--------|-------------|
| `kustoCluster` | Cluster URL (required) |
| `kustoDatabase` | Database name (required) |
| `kustoTable` | Target table (required) |
| `writeMode` | `Transactional` / `Queued` / `KustoStreaming` |
| `tableCreateOptions` | `CreateIfNotExist` / `FailIfNotExist` |
| `adjustSchema` | `NoAdjustment` / `FailIfNotMatch` / `GenerateDynamicCsvMapping` |
| `pollingOnDriver` | Poll ingestion on driver (default: true) |
| `writeEnableAsync` | Async write (don't wait for completion) |
| `writeResultLimit` | Limit rows written |
| `clientBatchingLimit` | Blob size limit in MB (default: 300) |
| `sparkIngestionPropertiesJson` | Ingestion properties JSON |
| `tempTableName` | Custom temp table name |
| `streamingIngestSizeInMB` | Streaming batch size |
| `kustoIngestionStorageContainer` | User-provided storage |

### 7.3 Authentication Options

| Option | Description |
|--------|-------------|
| `kustoAadAppId` | AAD application ID |
| `kustoAadAppSecret` | AAD application secret |
| `kustoAadAuthorityID` | Tenant ID |
| `accessToken` | Pre-obtained access token |
| `tokenProviderCallback` | Token provider function |
| `keyVaultUri` | Key Vault URI |
| `keyVaultAppId/Secret` | Key Vault auth |
| `clientCertPath/Password` | Certificate auth |
| `managedIdentityClientId` | User-assigned MI client ID |

---

## 8. Efficiency Analysis & Improvement Recommendations

### 8.1 Current Bottlenecks

#### Write Path

| Bottleneck | Description | Impact |
|-----------|-------------|--------|
| **Row-by-row CSV serialization** | Each row serialized individually via `RowCSVWriterUtils.writeRowAsCSV()` | CPU-bound; no vectorized/batch encoding |
| **Sequential blob upload + ingest** | Blob is uploaded, then `ingestFromBlob()` called — not pipelined | Latency: upload wait + ingest wait per blob |
| **Single blob per partition** | Each Spark partition produces one blob (or splits at `batchLimit * 1MB`) | No parallelism within a partition |
| **Synchronous extent movement** | `moveExtents` is blocking with adaptive backoff | Can take minutes for large ingestions |
| **Polling-based ingestion status** | `verifyAsyncCommandCompletion` polls `.show operations` in a loop | Extra cluster load + wait time |
| **No Parquet/columnar ingestion** | Always uses CSV format for ingestion | CSV is slower to parse than Parquet/Avro |
| **GZip compression per partition** | Each partition compresses independently | No shared compression dictionary |

#### Read Path

| Bottleneck | Description | Impact |
|-----------|-------------|--------|
| **Single-mode row-by-row deserialization** | `KustoResponseDeserializer` processes rows individually | CPU-bound for large single-mode reads |
| **Only hash partitioning** | Distributed reads only support `hash(col, N)` partitioning | No range/predicate-based partitioning |
| **Export latency** | Distributed reads wait for `.export` async completion | Minutes of latency before Spark can start reading |
| **Repeated schema/count queries** | Schema fetch + count estimate before actual query | Extra round-trips |

#### General

| Bottleneck | Description | Impact |
|-----------|-------------|--------|
| **No connection pooling visibility** | Relies on SDK's internal HTTP client | Can't tune connection pool per workload |
| **Ingest URI hardcoded pattern** | `https://ingest-<host>` may not work for all clouds | Sovereign cloud issues |
| **No native retry on transient blob failures** | Blob upload relies on Azure SDK defaults | May fail on transient network issues |

### 8.2 Improvement Recommendations

#### High Impact

| # | Recommendation | Effort | Benefit |
|---|---------------|--------|---------|
| 1 | **Adopt ingest-v2 managed streaming** | High | Automatic streaming→queued fallback, no blob for small batches, REST-based status tracking |
| 2 | **Pipeline blob upload + ingestion** | Medium | Start ingesting blob N while uploading blob N+1 (async pipeline) |
| 3 | **Support Parquet/Avro ingestion format** | Medium | Faster server-side parsing, better compression, schema preservation |
| 4 | **Vectorized CSV serialization** | Medium | Batch-encode columns instead of row-by-row |
| 5 | **Add range partitioning for reads** | Medium | Better data locality, fewer exports for filtered queries |

#### Medium Impact

| # | Recommendation | Effort | Benefit |
|---|---------------|--------|---------|
| 6 | **Multi-blob batch ingestion** | Low | Submit multiple blobs in one ingest call (ingest-v2 supports 70 blobs/batch) |
| 7 | **Async extent movement** | Low | Don't block on extent moves; use fire-and-forget with status check |
| 8 | **Columnar deserialization** | Medium | Use Spark's InternalRow + vectorized batch for single-mode reads |
| 9 | **Parallel export partitions** | Low | Issue all `.export` commands concurrently instead of sequentially |
| 10 | **Cache schema across operations** | Low | Avoid repeated `.show table schema` calls |

#### Low Impact / Quality

| # | Recommendation | Effort | Benefit |
|---|---------------|--------|---------|
| 11 | **DataSource V2 migration** | High | Better Spark integration, micro-batch partition control |
| 12 | **Metrics/observability** | Medium | Expose write throughput, latency percentiles |
| 13 | **Configurable compression** | Low | Allow LZ4/Snappy for faster (de)compression |
| 14 | **Connection pool tuning** | Low | Expose HTTP client pool size options |

# Ingest-v2 Specification & Integration Approach

## Table of Contents
1. [What is Ingest-v2?](#1-what-is-ingest-v2)
2. [Architecture & Key Classes](#2-architecture--key-classes)
3. [Comparison: ingest-v1 vs ingest-v2](#3-comparison-ingest-v1-vs-ingest-v2)
4. [API Surface](#4-api-surface)
5. [Managed Streaming Decision Logic](#5-managed-streaming-decision-logic)
6. [Performance Characteristics](#6-performance-characteristics)
7. [Current Spark Connector SDK Usage Analysis](#7-current-spark-connector-sdk-usage-analysis)
8. [Integration Approach: SDK vs Native — Analysis](#8-integration-approach-sdk-vs-native--analysis)
9. [Finalized Implementation Architecture](#9-finalized-implementation-architecture)
10. [Parquet Ingestion Format](#10-parquet-ingestion-format-planned--part-of-v2)
11. [Future Enhancements](#11-future-enhancements-post-v2-stabilization)
12. [Risks & Mitigations](#12-risks--mitigations)

---

## 1. What is Ingest-v2?

`ingest-v2` is a **next-generation Kotlin library** (module version `0.0.1-beta`, introduced in SDK 8.0.0) that completely rewrites the Kusto ingestion client. It is NOT an incremental update — it's a ground-up redesign.

### Key Differentiators

| Aspect | ingest (v1) | ingest-v2 |
|--------|-------------|-----------|
| **Language** | Java | Kotlin |
| **HTTP stack** | Apache HttpClient → azure-core | Ktor HttpClient |
| **Concurrency** | Reactor Mono/Flux | Kotlin coroutines + CompletableFuture adapters |
| **API models** | Hand-written Java POJOs | OpenAPI-generated from `openapi.yaml` |
| **DM communication** | Azure Storage Queue messages | **REST API** (`POST /v1/rest/ingestion/queued/{db}/{table}`) |
| **Status tracking** | Azure Storage Queue polling | REST API (`GET /v1/rest/ingestion/queued/{db}/{table}/{operationId}`) |
| **Upload target** | Azure Blob Storage only | Azure Blob Storage **+ OneLake (Lake)** |
| **Private Link** | Not supported | Fabric Private Link (S2S token) |
| **Auth model** | ConnectionStringBuilder | azure-identity `TokenCredential` directly |
| **Min Java version** | Java 8 | Java 11 |

### Core Innovation: REST-Based Ingestion

The fundamental architectural change is eliminating Azure Storage Queues from the ingestion path. Instead of:
```
Client → Upload blob → Enqueue message to Azure Queue → DM polls queue → DM processes blob
```

ingest-v2 uses:
```
Client → Upload blob → POST to DM REST API → DM processes blob immediately
```

This eliminates queue polling latency and enables synchronous status tracking.

---

## 2. Architecture & Key Classes

### Source Tree
```
ingest-v2/src/main/kotlin/com/microsoft/azure/kusto/ingest/v2/
├── client/
│   ├── IngestClient.kt                 — Interface (suspend + CompletableFuture)
│   ├── QueuedIngestClient.kt           — Queued ingestion via REST DM API
│   ├── StreamingIngestClient.kt        — Direct streaming to engine
│   ├── ManagedStreamingIngestClient.kt — Auto streaming→queued fallback
│   └── policy/
│       ├── ManagedStreamingPolicy.kt   — Fallback policy interface
│       └── DefaultManagedStreamingPolicy.kt — Per-table state machine
├── builders/
│   ├── QueuedIngestClientBuilder.kt
│   ├── StreamingIngestClientBuilder.kt
│   └── ManagedStreamingIngestClientBuilder.kt
├── source/
│   ├── BlobSource.kt                   — Pre-staged blob reference
│   ├── FileSource.kt                   — Local file
│   └── StreamSource.kt                 — InputStream
├── uploader/
│   ├── ManagedUploader.kt              — Blob upload with round-robin containers
│   └── ContainerUploaderBase.kt        — Parallel upload, retry, size limits
└── common/
    ├── ConfigurationCache.kt           — Timed cache for /configuration endpoint
    ├── IngestRetryPolicy.kt            — Retry strategies
    └── models/
        ├── IngestRequestPropertiesBuilder.kt
        └── ExtendedResponseTypes.kt    — IngestKind + response
```

### Three Client Types

1. **QueuedIngestClient** — For blob-based queued ingestion (large data)
   - Uploads local sources to blob, then POSTs to DM REST API
   - Supports multi-blob batch (up to 70 blobs in one call)
   - Full operation status tracking via REST

2. **StreamingIngestClient** — For direct streaming (small data ≤10MB)
   - Sends data as HTTP body to engine cluster
   - No blob intermediary
   - No operation tracking (fire-and-forget)

3. **ManagedStreamingIngestClient** ⭐ — Combines both with intelligent fallback
   - Tries streaming first, falls back to queued on failure/size
   - Per-table backoff state machine
   - Configurable policy (size factor, retry, backoff durations)

---

## 3. Comparison: ingest-v1 vs ingest-v2

### Performance Gains

| Improvement | Description |
|------------|-------------|
| No Storage Queue round-trip | Direct REST API call eliminates queue polling latency |
| Parallel blob upload | Up to 4 concurrent blob uploads (configurable) |
| Multi-blob batch | 70 blobs in single HTTP POST (vs 1 message per blob in v1) |
| Round-robin container selection | Atomic counter, no synchronization overhead |
| Thundering-herd prevention | Single coroutine refreshes config cache |
| Streaming without blob | Data ≤10MB goes directly as HTTP body |
| Non-blocking I/O | Ktor coroutines, no blocking threads |

### Feature Gains

| Feature | Description |
|---------|-------------|
| OneLake upload | Upload to Azure Data Lake Gen2 (Fabric) |
| Fabric Private Link | S2S token header support |
| REST status tracking | No queue polling; direct `GET /operation/{id}` |
| Builder pattern | Clean, type-safe client construction |
| Policy engine | Configurable managed streaming fallback behavior |
| Custom uploaders | Bring-your-own blob upload implementation |

---

## 4. API Surface

### IngestClient Interface (Kotlin)
```kotlin
interface IngestClient : Closeable {
    suspend fun ingestAsync(database, table, source, props?): ExtendedIngestResponse
    suspend fun getOperationSummaryAsync(operation): Status
    suspend fun getOperationDetailsAsync(operation): StatusResponse

    // Java-friendly
    fun ingestAsyncJava(database, table, source, props?): CompletableFuture<ExtendedIngestResponse>
}
```

### MultiIngestClient (extends IngestClient)
```kotlin
interface MultiIngestClient : IngestClient {
    suspend fun ingestAsync(database, table, sources: List<BlobSource>, props?): ExtendedIngestResponse
    suspend fun getMaxSourcesPerMultiIngest(): Int  // default: 70
}
```

### Builder Pattern
```kotlin
val client = ManagedStreamingIngestClientBuilder.create(dmUrl)
    .withAuthentication(credential)           // TokenCredential
    .withClientDetails("SparkConnector", "7.0.6")
    .withManagedStreamingIngestPolicy(policy)
    .build()
```

### IngestRequestProperties
```kotlin
val props = IngestRequestPropertiesBuilder.create()
    .withFormat(Format.csv)
    .withIngestionMappingReference("myMapping")
    .withEnableTracking(true)
    .withTags(listOf("tag1"))
    .withSkipBatching(true)
    .build()
```

---

## 5. Managed Streaming Decision Logic

```
ingestAsync(source)
  │
  ▼
shouldDefaultToQueuedByPolicy(table)?  ──YES──▶ QUEUED
  │ NO
  ▼
shouldDefaultToQueuedBySize(source)?   ──YES──▶ QUEUED
(size > 10MB * dataSizeFactor)
  │ NO
  ▼
invokeStreamingIngestion()
  ├── Success ──▶ STREAMING ✓
  └── Failure:
      ├── Transient → retry (1s, 2s, 4s + jitter)
      ├── STREAMING_OFF → backoff 15min, then QUEUED
      ├── TABLE_CONFIG → backoff 15min, then QUEUED
      ├── THROTTLED → backoff 10s, then QUEUED
      └── Permanent → THROW
```

### Per-Table State Machine
- Each `"database-table"` key has independent backoff state
- After streaming failure → table enters backoff period
- During backoff → all requests for that table go to queued
- After backoff expires → streaming is attempted again
- Successful streaming resets the state

---

## 6. Performance Characteristics

### Size Thresholds

| Constant | Value | Purpose |
|----------|-------|---------|
| `STREAMING_MAX_REQ_BODY_SIZE` | 10 MB | Max streaming HTTP body |
| `MANAGED_STREAMING_DATA_SIZE_FACTOR_DEFAULT` | 1.0 | Multiplier for size threshold |
| `UPLOAD_MAX_SINGLE_SIZE_BYTES` | 256 MB | Max single-shot blob upload |
| `UPLOAD_BLOCK_SIZE_BYTES` | 4 MB | Block size for chunked upload |
| `UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES` | 4 GB | Max per container |
| `MAX_BLOBS_PER_BATCH` | 70 | Multi-blob batch limit |
| `UPLOAD_CONTAINER_MAX_CONCURRENCY` | 4 | Parallel upload threads |

### Retry Configuration
- Managed streaming: `[1s, 2s, 4s]` + up to 1000ms jitter
- Queued upload: configurable via `IngestRetryPolicy`
- Config cache refresh: 1 hour (or server-specified minimum)

---

## 7. Current Spark Connector SDK Usage Analysis

### How Extensively Does the Connector Use the Java SDK?

**Answer: Extensively for ingestion, moderately for queries.**

#### Ingestion (Heavy Usage)
| SDK Component | Connector Usage |
|--------------|----------------|
| `IngestClientFactory.createClient()` | Creates `QueuedIngestClient` (lazy, cached) |
| `IngestClientFactory.createManagedStreamingIngestClient()` | Creates managed streaming client (lazy, cached) |
| `QueuedIngestClient.ingestFromBlob()` | Core blob ingestion path |
| `ManagedStreamingIngestClient.ingestFromStream()` | Core streaming ingestion path |
| `IngestionProperties` | Configures format, mapping, tags, etc. |
| `BlobSourceInfo` / `StreamSourceInfo` | Wraps data sources |
| `IngestionResult` | Tracks ingestion status |

#### Query (Moderate Usage)
| SDK Component | Connector Usage |
|--------------|----------------|
| `Client` (engine) | Execute queries and management commands |
| `Client` (DM) | Execute DM commands (temp storage, containers) |
| `ClientRequestProperties` | Query timeout, parameters |
| `KustoResultSetTable` | Result deserialization |
| `ConnectionStringBuilder` | All client authentication |

#### What the Connector Does Natively (NOT via SDK)
| Operation | Why Native? |
|-----------|-------------|
| CSV serialization | Spark schema → CSV is connector-specific |
| Blob upload | Direct Azure Storage SDK (not via SDK's uploader) |
| Export to blob | `.export` command issued via engine client |
| Extent management | `.move extents` commands via DM client |
| Temp table lifecycle | Create/drop via management commands |
| Container discovery | Via management commands to DM |
| Async operation polling | `.show operations` command |

### Why Some Operations Are Native

1. **CSV serialization**: Must handle Spark's type system (StructType, ArrayType, MapType)
2. **Blob upload**: The connector manages its own blob lifecycle for both reads (export) and writes (ingestion staging)
3. **Extent management**: Transactional write mode requires precise control over extent movement
4. **Container discovery**: The connector needs containers for both ingestion AND export; SDK only provides ingestion containers

---

## 8. Integration Approach: SDK vs Native — Analysis

### Why the Connector Uses Native Blob Upload (Not SDK's Uploader)

The current connector deliberately bypasses the Java SDK's built-in blob upload for
ingestion. This is a critical design decision driven by Spark's distributed execution
model:

1. **Streaming-to-blob without full partition buffering**: Spark partitions can be
   arbitrarily large (multiple GBs). The connector serializes rows → GZip → blob in a
   streaming pipeline (`RowCSVWriterUtils` → `GZIPOutputStream` → `CloudBlockBlob`).
   The SDK's `StreamSource` requires an `InputStream` over the entire payload, which
   means buffering in memory or a temp file first.

2. **Partition-level parallelism via Spark**: Spark's `foreachPartition` already provides
   massive parallelism (hundreds of tasks across executors). The SDK's internal parallel
   upload (4 threads) is irrelevant when Spark is already uploading from N executors
   concurrently.

3. **Container lifecycle**: The connector manages blob containers for BOTH ingestion AND
   export (reads). It already has `ContainerProvider` with round-robin selection and SAS
   caching. The SDK manages containers only for ingestion.

4. **Size-based blob splitting**: The connector splits partitions into multiple blobs when
   they exceed `batchLimit * 1MB`. This is natively integrated with the CSV serialization
   loop (seal blob when threshold reached, start new blob).

### Why the Connector Uses Its Own CSV Serialization

The SDK does not handle Spark's type system. The connector must:
- Convert `InternalRow` (Spark's internal columnar format) to CSV strings
- Handle Spark-specific types: `StructType`, `ArrayType`, `MapType` → JSON
- Handle `DecimalType` precision, `TimestampType` → ISO format, `BinaryType` → Base64
- Integrate size-based blob splitting with the serialization loop

This is not a limitation — it's the correct boundary. **Serialization is connector-
specific; ingestion notification is SDK-specific.**

### Option Analysis (Updated)

| Option | Approach | Verdict |
|--------|----------|---------|
| A: Full ingest-v2 SDK (use SDK uploader) | Let SDK handle blob upload via `StreamSource`/`FileSource` | ❌ **Rejected** — requires buffering entire partition; loses streaming-to-blob efficiency; contradicts Spark's execution model |
| B: Native REST API implementation | Implement DM REST API directly without SDK | ❌ **Rejected** — duplicates SDK work; must maintain retry, config cache, managed streaming policy manually; fragile to API changes |
| C: Hybrid — Native blob upload + ingest-v2 SDK for ingestion notification | ⭐ Keep native streaming-to-blob → use ingest-v2 `BlobSource` for REST notification + multi-blob batch + status tracking | ✅ **CHOSEN** |

### Chosen Approach: Native Upload + ingest-v2 SDK Notification ⭐

```
Spark Partition (InternalRow iterator)
    │
    ▼  [Connector-owned: CSV serialization + blob upload]
RowCSVWriterUtils.writeRowAsCSV() → GZIPOutputStream → CloudBlockBlob
    │  (streaming pipeline, no full-partition buffering)
    │  (splits into multiple blobs at batchLimit threshold)
    │
    ▼  [ingest-v2 SDK: REST-based ingestion notification]
BlobSource(blobPath, Format.csv, uuid, CompressionType.GZIP)
    │
    ▼  [ingest-v2 SDK: Multi-blob batch via REST DM API]
QueuedIngestClient.ingestAsyncJava(db, table, List<BlobSource>, props)
    │  (up to 70 blobs per batch → single HTTP POST to DM)
    │
    ▼  [ingest-v2 SDK: REST-based status tracking]
QueuedIngestClient.pollForCompletion(operation, interval, timeout)
    │  (replaces queue-based polling of v1)
    │
    ▼  [Connector-owned: Transactional finalization]
ExtendedKustoClient.moveExtents() → cleanup temp table
```

**Why this is optimal for Spark:**

| Spark Capability | How We Leverage It |
|------------------|-------------------|
| Distributed `foreachPartition` | Each executor uploads blobs independently to different containers |
| Task-level parallelism | N partitions = N concurrent blob uploads, no SDK concurrency needed |
| Accumulator pattern | Collect `IngestionOperation` IDs from executors → finalize on driver |
| Memory management | Streaming pipeline = constant memory per task regardless of partition size |
| Fault tolerance | Task retry = re-upload + re-ingest single partition (idempotent with tags) |

**What ingest-v2 SDK gives us (that we CAN'T easily do natively):**

| SDK Feature | Benefit |
|-------------|---------|
| Multi-blob batch (70 per call) | Single HTTP POST notifies DM of all partition blobs |
| REST-based status tracking | No queue polling; direct `GET /operation/{id}` — faster + simpler |
| Managed streaming fallback | Per-table state machine: try stream → auto-fallback to queued |
| Configuration cache | SDK handles DM container/endpoint discovery + timed refresh |
| Future Fabric/OneLake support | When GA, we get it for free |

**What we keep native (and why):**

| Native Component | Why Not SDK |
|-----------------|-------------|
| CSV serialization | Spark types (InternalRow, StructType) are connector-specific |
| Blob upload | Streaming-to-blob avoids partition buffering; Spark parallelism replaces SDK's 4-thread upload |
| Container discovery for export | SDK only provides ingestion containers; we need export containers too |
| Temp table lifecycle | `.create table`, `.move extents`, `.drop table` are management commands — not ingestion |
| Extent management | Transactional mode requires precise control over extent movement timing |

---

## 9. Finalized Implementation Architecture

### 9.1 Package Structure

All v2 code lives in a self-contained package `com.microsoft.kusto.spark.datasink.v2`:

```
datasink/v2/
├── IngestV2Authentication.scala      — TokenCredential creation (self-contained)
├── IngestV2ClientProvider.scala      — Builds/caches ingest-v2 SDK clients
├── IngestV2QueuedWriter.scala        — Native blob upload + BlobSource batch
├── IngestV2StreamingWriter.scala     — StreamSource + ManagedStreamingIngestClient
├── IngestV2StatusTracker.scala       — REST-based operation polling
├── IngestV2FinalizeHelper.scala      — Transactional: poll → move → cleanup
└── IngestV2WriterOrchestrator.scala  — Top-level entry point (routing)
```

### 9.2 Config Switch & Routing

```scala
// KustoSinkOptions.scala
val KUSTO_USE_INGEST_V2 = KustoSinkOption("useIngestV2", ...)

// KustoWriter.write() — single routing check
if (writeOptions.useIngestV2) {
  v2.IngestV2WriterOrchestrator.write(data, coordinates, auth, writeOptions, tmpTable, crp)
  return
}
// ... existing v1 path unchanged ...
```

When `useIngestV2 = false` (default): **zero code path changes** — existing v1 flow runs
exactly as before. This guarantees backward compatibility.

### 9.3 Write Mode Mapping

| WriteMode | v1 Path | v2 Path |
|-----------|---------|---------|
| **Transactional** | Ingest to temp table → poll queue → move extents | Ingest to temp table → `pollForCompletion` REST → move extents |
| **Queued** | Direct ingest to target (fire-and-forget) | Direct ingest to target via multi-blob batch |
| **KustoStreaming** | `ManagedStreamingIngestClient.ingestFromStream()` (v1 SDK) | `ManagedStreamingIngestClient.ingestAsync(StreamSource)` (v2 SDK) |

### 9.4 Queued/Transactional Write Flow (Detailed)

```
                           Driver                                    Executor (per partition)
                             │                                              │
                             │  rdd.foreachPartition ─────────────────────▶ │
                             │                                              │
                             │                                    ┌─────────▼──────────┐
                             │                                    │ IngestV2QueuedWriter│
                             │                                    │  .ingestPartition() │
                             │                                    └─────────┬──────────┘
                             │                                              │
                             │                                    ┌─────────▼──────────┐
                             │                                    │ CSV serialize rows  │
                             │                                    │ → GZip → Blob upload│
                             │                                    │ (streaming pipeline)│
                             │                                    └─────────┬──────────┘
                             │                                              │
                             │                                    ┌─────────▼──────────┐
                             │                                    │ Seal blob at batch  │
                             │                                    │ limit; create new   │
                             │                                    │ blob; accumulate    │
                             │                                    │ BlobSource list     │
                             │                                    └─────────┬──────────┘
                             │                                              │
                             │                                    ┌─────────▼──────────┐
                             │                                    │ Flush: ingestAsync  │
                             │                                    │ (multi-blob batch)  │
                             │                                    │ up to 70 BlobSources│
                             │                                    │ per REST call       │
                             │                                    └─────────┬──────────┘
                             │                                              │
                             │  ◀──── CollectionAccumulator ────────────────┘
                             │         (IngestionOperation IDs)
                             │
                   ┌─────────▼──────────┐
                   │IngestV2FinalizeHelper│  (Transactional mode only)
                   │  .finalize()        │
                   └─────────┬──────────┘
                             │
                   ┌─────────▼──────────┐
                   │ pollForCompletion() │  (REST-based, not queue-based)
                   │ for ALL operations  │
                   └─────────┬──────────┘
                             │
                   ┌─────────▼──────────┐
                   │ moveExtents()       │  (temp table → destination)
                   │ cleanup temp table  │
                   └─────────┴──────────┘
```

### 9.5 Streaming Write Flow (Detailed)

```
Executor (per partition):
  IngestV2StreamingWriter.ingestPartition()
    │
    ├── Serialize rows to in-memory CSV buffer
    │   (splits at DefaultMaxStreamingBytesUncompressed = 4MB)
    │
    ├── For each chunk:
    │     StreamSource(ByteArrayInputStream, Format.csv)
    │     managedStreamingClient.ingestAsyncJava(db, table, source, props)
    │       │
    │       ├── SDK tries streaming (HTTP body ≤10MB to engine)
    │       │   └── Success → IngestKind.STREAMING
    │       │
    │       └── SDK fallback → queued upload + REST notification
    │           └── Success → IngestKind.QUEUED
    │
    └── Returns List[IngestionOperation] (for optional tracking)
```

### 9.6 Authentication Mapping

The ingest-v2 SDK uses Azure Identity `TokenCredential` directly (not
`ConnectionStringBuilder`). `IngestV2Authentication` maps:

| Connector Auth | TokenCredential |
|---------------|----------------|
| `AadApplicationAuthentication` | `ClientSecretCredentialBuilder` |
| `AadApplicationCertificateAuthentication` | `ClientCertificateCredentialBuilder` |
| `ManagedIdentityAuthentication` | `ManagedIdentityCredentialBuilder` |
| `KustoAccessTokenAuthentication` | Custom `TokenCredential` (returns pre-obtained token) |
| `KustoTokenProviderAuthentication` | Custom `TokenCredential` (wraps callback) |

### 9.7 Dependency & Shading

```xml
<!-- Added to connector/pom.xml -->
<dependency>
  <groupId>com.microsoft.azure.kusto</groupId>
  <artifactId>kusto-ingest-v2</artifactId>
  <version>${kusto.ingestv2.sdk.version}</version>  <!-- 0.0.1-beta -->
</dependency>

<!-- Shade rules added for ingest-v2's transitive deps -->
io.ktor    → ${kusto.shade.prefix}.io.ktor
kotlinx    → ${kusto.shade.prefix}.kotlinx
kotlin     → ${kusto.shade.prefix}.kotlin

<!-- Excluded from shading (connector references directly) -->
com.microsoft.azure.kusto.ingest.v2.**
```

### 9.8 Isolation Guarantee

The v2 code is **completely isolated** from v1:

- **No shared classes**: V2 has its own auth (`IngestV2Authentication`), client management
  (`IngestV2ClientProvider`), blob upload logic, status tracking, and finalization.
- **No bridges/adapters**: If v1 code changes, v2 is unaffected and vice versa.
- **Shared utilities only**: `RowCSVWriterUtils` (CSV serialization) and `KustoQueryUtils.simplifyName`
  are format/transport-agnostic and shared with v1.
- **Engine client shared**: `ExtendedKustoClient` is used for management commands (`.move extents`,
  temp table ops) — these are NOT ingestion and are shared infrastructure.
- **Future deletion**: When v1 is retired, delete the v1 datasink files. The v2 package
  stands alone. The routing check in `KustoWriter` becomes the only code to remove.

---

## 9.9 Comparison: v1 vs v2 Path (Side by Side)

| Aspect | v1 (current) | v2 (new, opt-in) |
|--------|-------------|------------------|
| **Ingestion notification** | `ingestFromBlob()` → SDK enqueues to Azure Storage Queue | `ingestAsyncJava(BlobSource)` → SDK POSTs to DM REST API |
| **Batch size** | 1 blob per ingest call | Up to 70 blobs per ingest call |
| **Status tracking** | Azure Storage Queue polling (via `IngestionResult`) | REST `GET /operation/{id}` via `pollForCompletion` |
| **Streaming** | `ManagedStreamingIngestClient.ingestFromStream()` (v1 SDK) | `ManagedStreamingIngestClient.ingestAsync(StreamSource)` (v2 SDK) |
| **Streaming fallback** | SDK-managed (limited configurability) | Policy engine with per-table state machine |
| **Auth model** | `ConnectionStringBuilder` | `TokenCredential` (Azure Identity) |
| **Blob upload** | Native (streaming-to-blob) | Native (streaming-to-blob) — **same** |
| **CSV serialization** | `RowCSVWriterUtils` | `RowCSVWriterUtils` — **same** |
| **Container discovery** | `ContainerProvider` (DM management commands) | `ContainerProvider` — **same** (for our blob upload) |
| **Extent movement** | `ExtendedKustoClient.moveExtents` | `ExtendedKustoClient.moveExtents` — **same** |

### 9.10 Performance Gains from v2

| Improvement | Mechanism | Expected Impact |
|-------------|-----------|-----------------|
| Eliminate queue latency | REST POST replaces queue enqueue + DM poll | ~2-5s per ingest call saved |
| Multi-blob batch | 70 blobs in 1 REST call instead of 70 separate calls | Reduces HTTP round-trips by 70x |
| Faster status tracking | Direct REST GET vs queue message polling | Immediate status; no polling delay |
| Managed streaming policy | Per-table state machine with backoff | Better streaming/queued routing |
| No Storage Queue dependency | REST-only path | Simpler networking; fewer firewall rules |

### 9.11 What This Does NOT Change

These aspects remain identical regardless of `useIngestV2` setting:

1. **CSV serialization** — same `RowCSVWriterUtils` (row-by-row, same types handling)
2. **Blob upload** — same native streaming-to-blob pipeline  
3. **Spark execution model** — same `foreachPartition`, same accumulator pattern
4. **Container management** — same `ContainerProvider` for both ingest and export
5. **Transactional semantics** — same temp table → move extents → cleanup
6. **Schema validation** — same `.show table schema` + `adjustSchema` logic
7. **Read path** — completely unaffected
8. **Authentication configuration** — same user-facing options (v2 handles mapping internally)

---

## 10. Parquet Ingestion Format (Planned — Part of v2)

### Motivation

The current write path uses **custom CSV serialization** (`RowCSVWriterUtils.writeRowAsCSV()`):
- Row-by-row processing — no vectorization
- Custom handling of every Spark type (Struct→JSON, Array→JSON, Decimal→quoted, Binary→Base64)
- Requires explicit CSV mapping for column ordering
- Server-side: Kusto must parse CSV text and apply mapping

**Parquet eliminates all of this**: Spark natively writes Parquet with full vectorization,
columnar compression, and embedded schema. Kusto natively reads Parquet with zero mapping.

### Architecture: Spark Native Parquet → Blob → Kusto

```
DataFrame
    │
    ▼  [Spark's native Parquet writer — vectorized, zero custom code]
data.write.parquet(blobContainerPath)
    │  (writes to Azure Blob Storage via Hadoop FS)
    │  (Spark handles: partitioning, row groups, Snappy compression, schema)
    │
    ▼  [List generated Parquet files from blob storage]
List<String> parquetBlobPaths
    │
    ▼  [ingest-v2 SDK: Multi-blob batch — no mapping needed]
BlobSource(path, Format.parquet, uuid, CompressionType.NONE)
QueuedIngestClient.ingestAsyncJava(db, table, List<BlobSource>, props)
    │  (70 Parquet files per batch)
    │
    ▼  [Same status tracking + finalization as CSV path]
```

### Configuration

```scala
// New option (only applies when useIngestV2 = true)
val KUSTO_INGESTION_FORMAT = KustoSinkOption("ingestionFormat", "csv") // "csv" | "parquet"
```

### Storage: Config-Driven

| Mode | How It Works |
|------|-------------|
| **Kusto DM containers** (default) | Get temp container from DM → configure Hadoop FS with SAS via `KustoAzureFsSetupCache` → Spark writes Parquet there |
| **User-provided storage** | User provides ABFS/WASBS path with credentials → Spark writes directly |

The read path already configures Hadoop FS for blob access (using `KustoAzureFsSetupCache`).
The Parquet write path reuses this exact same mechanism.

### Comparison: CSV vs Parquet Ingestion

| Dimension | CSV Path | Parquet Path |
|-----------|----------|-------------|
| **Serialization code** | ~200 lines custom (`RowCSVWriterUtils`) | Zero — Spark's built-in writer |
| **Vectorization** | None (row-by-row) | Full (Spark's columnar batch encoding) |
| **Compression** | GZip (entire blob) | Per-column Snappy/Zstd (better ratio) |
| **Schema** | Requires explicit CSV mapping | Embedded in file — auto-matched by column name |
| **Complex types** | JSON stringification (Struct/Array/Map) | Native Parquet nested types (LIST, MAP, STRUCT) |
| **Server-side parsing** | CSV text parsing + mapping application | Direct columnar read — significantly faster |
| **Memory model** | Streaming (constant ~16KB buffer) | Spark-managed row groups (default 128MB) |
| **Type safety** | Text-based; type errors caught late | Binary encoding; types validated at write time |
| **File size** | Larger (text + GZip) | Smaller (columnar + dictionary encoding) |

### When to Use Each

| Scenario | Recommended Format |
|----------|-------------------|
| Default / small data / memory-constrained executors | CSV (streaming, constant memory) |
| Large batch writes with adequate executor memory | **Parquet** (faster end-to-end) |
| Complex nested types (Struct, Map, Array) | **Parquet** (native nesting vs JSON hack) |
| Schema-less / dynamic columns | CSV with mapping |
| Maximum throughput | **Parquet** (vectorized write + faster ingest) |

### Trade-offs

| Concern | Impact | Mitigation |
|---------|--------|-----------|
| Memory usage | Parquet buffers row groups (~128MB default) | Tunable via `spark.sql.parquet.rowGroupSize`; adequate for most clusters |
| Hadoop FS config | Requires ABFS/WASBS filesystem setup on executors | Already implemented (`KustoAzureFsSetupCache`); same as read path |
| Ingestion mapping | Parquet uses column name matching (no explicit mapping) | Table schema must match DataFrame column names |
| Streaming mode | Parquet doesn't apply to KustoStreaming (which sends ≤10MB chunks) | KustoStreaming stays CSV; Parquet for Queued/Transactional only |

---

## 11. Future Enhancements (Post-v2 Stabilization)

Once ingest-v2 reaches GA and the opt-in path is validated in production:

### Pipeline Optimizations
1. **Async blob upload + ingest pipeline**: Upload blob N+1 while notifying DM about blob N
2. **Parallel blob uploads within partition**: For very large partitions, upload multiple blobs concurrently

### Make v2 Default
1. Flip default: `useIngestV2 = true`
2. Deprecate v1 path
3. Remove v1 code after deprecation period

### Leverage SDK's Full Capabilities
1. Use SDK's `ManagedUploader` for small partitions (< 256MB, where buffering is acceptable)
2. OneLake/Fabric Private Link support
3. Custom streaming policy configuration exposed as Spark options

---

## 12. Risks & Mitigations

### Technical Risks

| Risk | Mitigation |
|------|-----------|
| ingest-v2 is beta (0.0.1-beta) | Opt-in only (`useIngestV2 = false` default); v1 path unchanged |
| REST API changes in future SDK versions | Pin to `0.0.1-beta`; upgrade deliberately with testing |
| Kotlin/Ktor dependency in uber-jar | Fully shaded (`io.ktor`, `kotlinx`, `kotlin` relocated) |
| Shading conflicts with Databricks runtime | Added shade rules verified with `mvn compile`; tested on Databricks |
| `TokenCredential` auth mismatch | Self-contained `IngestV2Authentication` maps all auth types |

### Compatibility Risks

| Risk | Mitigation |
|------|-----------|
| Java 11 required by ingest-v2 | Spark 4 (master) already requires Java 11+; Spark 3 branch deferred |
| Existing customers unaffected | Config switch ensures zero behavioral change when disabled |
| Transactional mode correctness | Same `moveExtents` logic; only status tracking changes (REST vs queue) |
| Container SAS token expiry | Same `ContainerProvider` lifecycle; unchanged from v1 |

### Operational Risks

| Risk | Mitigation |
|------|-----------|
| REST DM endpoint not available on older clusters | Feature-detect via first ingest call; fall back to v1 if SDK throws |
| Multi-blob batch exceeds server limit | Cap at 70 (SDK constant); server returns error if exceeded |
| Streaming size limits vary per cluster | SDK's managed streaming policy handles this with per-table backoff |
| Network partitions during `pollForCompletion` | Timeout + cleanup temp table on failure (same as v1 error handling) |

---

## Appendix: Key Constants for Implementation

```
REST DM Ingestion:     POST /v1/rest/ingestion/queued/{db}/{table}
REST Configuration:    GET  /v1/rest/ingestion/configuration
REST Status:           GET  /v1/rest/ingestion/queued/{db}/{table}/{operationId}
Streaming Ingestion:   POST /v1/rest/ingest/{database}/{table}  (engine endpoint)

Max streaming body:    10 MB
Max blobs per batch:   70 (server-tunable)
Max single blob:       256 MB
Upload concurrency:    4
Config cache TTL:      1 hour
Streaming retry:       [1s, 2s, 4s] + jitter
Table backoff:         15 min (streaming off / table config)
Throttle backoff:      10 sec
```

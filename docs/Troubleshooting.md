# Azure Data Explorer Spark Connector — Troubleshooting Guide

## Connector Overview

The connector implements Spark Datasource V1. Format identifier: `com.microsoft.kusto.spark.datasource`.

---

## Read Paths

### Single Mode
Direct KQL query → result returned in-memory.  
Bound by [Kusto query limits](https://aka.ms/kustoquerylimits). Fast for small result sets.

### Distributed Mode
KQL `.export` to transient blob storage → Spark reads Parquet files from blob.  
Required when results exceed query limits. Adds storage COGS.

### Mode Selection
| Scenario | Behaviour |
|---|---|
| No `readMode` set | Connector estimates row count; picks Single with fallback to Distributed |
| `ForceSingleMode` | Single only — fails if result exceeds limits |
| `ForceDistributedMode` | Always exports to blob first |

### Distributed Mode Storage Auth
The connector sets Hadoop/Spark config for the storage protocol in use:

| Protocol | Auth config target | Notes |
|---|---|---|
| `wasbs` (default) | Hadoop `Configuration` | SAS token stripped of leading `?` |
| `abfss` / `abfs` | Spark `RuntimeConfig` | Account Key **not supported** — use SAS |

---

## Write Paths

All write modes use `df.write.format("com.microsoft.kusto.spark.datasource")`.

### Transactional (default)

```
Workers serialize rows → CSV → GZip → queued ingestion into TEMP table
    ↓
Driver polls ingestion status per partition
    ↓
Alter merge policy on temp table (disable merge/rebuild)
    ↓
.move extents from temp table → destination table  (atomic, metadata-only)
    ↓
Drop temp table
```

**Key properties:**
- Atomicity: all-or-nothing. On failure the temp table is dropped (unless `tempTableName` was set by user).
- Requires `admin` privileges on the table.
- Temp table name: `sparkTempTable_<app>_<table>_<batchId>_<requestId>`.
- Batching policy from the destination table is copied to the temp table.

### Queued

```
Workers serialize rows → CSV → GZip → queued ingestion directly into DESTINATION table
    ↓
Write call returns once workers finish uploading — ingestion may still be in progress on the service side.
```

**Key properties:**
- No temp table, no polling, no `.move extents`.
- Scales better for large loads.
- Service-side failures are **not** propagated back to Spark.
- Works well with Materialized Views.
- **Recommended for production big loads.**

### KustoStreaming

```
Workers serialize rows → CSV → 4 MB chunks → managed streaming ingestion
    ↓
If stream fails after 3 retries → fallback to queued blob ingestion
```

**Key properties:**
- [Streaming ingestion policy](https://learn.microsoft.com/azure/data-explorer/kusto/management/streamingingestionpolicy) must be enabled on table/database **and** cluster.
- 4 MB per-request limit; connector auto-chunks.
- Not recommended for high-throughput batch — use `Queued` instead.
- Spark Structured Streaming pairs well with `Queued`, not `KustoStreaming`.

---

## Common Write Options

| Option | Default | Notes |
|---|---|---|
| `writeMode` | `Transactional` | `Transactional`, `Queued`, `KustoStreaming` |
| `tableCreateOptions` | `FailIfNotExist` | `CreateIfNotExist` requires admin |
| `clientBatchingLimit` | `300` MB | Per-partition aggregation before ingest. Production: `1024` |
| `pollingOnDriver` | `false` | `true` recommended in production (avoids worker core occupation) |
| `isAsync` | `false` | If `true`, write returns immediately; errors not surfaced to driver |
| `tempTableName` | auto-generated | User-supplied temp table persists on failure for inspection |
| `adjustSchema` | `NoAdjustment` | `GenerateDynamicCsvMapping`, `FailIfNotMatch` |
| `timeoutLimit` | `172000`s (≈2 days) | Upper bound for the entire operation |

---

## Error → Cause → Resolution

### Write Errors

| Error / Symptom | Likely Cause | Resolution |
|---|---|---|
| `TimeoutAwaitingPendingOperationException` | Ingestion or `.move extents` exceeded timeout | Increase `timeoutLimit`. Check ADX cluster health and ingestion queue depth. Verify batching policy on the table. |
| `FailedOperationException` with State ≠ `Completed` | `.export` or async admin command failed on ADX side | Check `OperationId` in the error message via `.show operations <id>` on the cluster. |
| `IngestionServiceException` / repeated retries | Transient service error on ingestion endpoint | Connector retries up to 2× with exponential backoff. If persistent: check ADX ingestion endpoint health and network connectivity. |
| `NoStorageContainersException` — "Failed to allocate temporary storage" | `.get ingestion resources` returned empty | ADX ingest service has no temp storage provisioned or the principal lacks permissions. Verify the app has **ingestor** role and the cluster's managed storage is healthy. |
| `InvalidParameterException` — "Account Key authentication is not supported" | Using `abfss` protocol with account key | Switch to SAS-based storage credentials or use `wasbs` protocol. |
| Temp table `sparkTempTable_*` left behind | Transactional write failed mid-way; user supplied `tempTableName` | Inspect the temp table for partial data. Drop it manually or set an [auto-delete policy](https://docs.microsoft.com/azure/data-explorer/kusto/management/auto-delete-policy). |
| `Ingestion to Kusto failed on timeout failure` (Pending status) | Polling on partition ingestion result timed out | The partition's blob was queued but ADX didn't confirm completion in time. Check ADX ingestion latency and batching policy (`MaximumBatchingTimeSpan`). |
| Ingestion status `Skipped` | `ingestIfNotExists` tags already exist in destination | Expected when dedup is active. If unexpected, check `ingestIfNotExists` tags on the table's extents. |
| Ingestion status `PartiallySucceeded` or `Failed` | Bad data, schema mismatch, mapping error | Inspect `ingestionStatus.failureStatus` and `errorCode`. Common: CSV column count mismatch — enable `adjustSchema = GenerateDynamicCsvMapping`. |
| `The exception is not visible in the driver since we're in async mode` | `isAsync = true` — worker failure can't propagate | Check executor/worker logs. Switch to synchronous mode for debugging. |
| Streaming: `Total of N bytes were ingested...Please consider 'Queued' writeMode` | Partition data exceeds streaming sweet spot | Switch to `Queued` for larger batches. |

### Read Errors

| Error / Symptom | Likely Cause | Resolution |
|---|---|---|
| Query result truncation / empty DataFrame in Single mode | Result exceeds [Kusto query limits](https://aka.ms/kustoquerylimits) | Use `ForceDistributedMode` or let the connector auto-select. |
| `NoStorageContainersException` in Distributed mode | No export containers available | Provide explicit transient storage parameters or ensure the app has access to Kusto-managed export containers. Check [callout policies](https://learn.microsoft.com/azure/data-explorer/kusto/management/calloutpolicy). |
| `FailedOperationException` during export | `.export` command failed | Run `.show operations <id>` to diagnose. Common: callout policy blocks traffic to the storage account. |
| `TimeoutAwaitingPendingOperationException` during export | Export took longer than `timeoutLimit` | Increase timeout. Reduce query scope. Check cluster compute capacity. |
| Parquet read failure after export (Spark < 3.3.0) | New ADX Parquet encoding (delta byte array) unsupported by older Spark | Upgrade to Spark ≥ 3.3.0. The connector sets `useNativeParquetWriter=false` as a workaround but newer versions are recommended. |
| ABFS/ABFSS `SAS config key NOT found` warning | SAS token not propagated to SparkConf | Verify `storageProtocol` matches actual storage endpoint. Ensure `fs.azure.abfs.valid.endpoints` includes your storage domain. |
| `Storage protocol 'abfss' with Account Key authentication is not supported yet` | ABFS + account key combination | Use SAS auth with ABFS, or switch to `wasbs` protocol. |

### Authentication Errors

| Error / Symptom | Likely Cause | Resolution |
|---|---|---|
| 401/403 from Kusto engine endpoint | App lacks `viewer` / `admin` role on database/table | Grant appropriate roles via `.add database <db> viewers ('aadapp=<appId>')`. |
| 401/403 from Kusto ingest endpoint | App lacks `ingestor` role | Grant ingestor role: `.add database <db> ingestors ('aadapp=<appId>')`. |
| `HttpHostConnectException` on ingest endpoint | Network / firewall blocking `ingest-<cluster>` | Verify DNS resolution and firewall rules for the ingest endpoint. |
| Token expiry during long writes | AAD token lifetime < operation duration | Use app-based auth (client secret / certificate) instead of user/device tokens. |

---

## Diagnostic Checklist

1. **Check `requestId`** — every operation generates one. Search ADX logs: `.show commands | where ClientActivityId has "<requestId>"`.
2. **Check `.show operations`** — for async operations (export, move extents) the `OperationId` is logged.
3. **Enable debug logging** — set `log4j.logger.com.microsoft.kusto.spark=DEBUG` to see SAS config setup, polling status, retry attempts.
4. **Inspect temp tables** — `sparkTempTable_*` tables indicate incomplete Transactional writes. Use `.show tables | where TableName startswith "sparkTempTable_"`.
5. **Check ingestion queue** — `.show ingestion failures` on the cluster surfaces service-side ingestion errors that `Queued` mode won't propagate to Spark.
6. **Verify batching policy** — a long `MaximumBatchingTimeSpan` on the destination table delays ingestion completion, causing polling timeouts in Transactional mode.

---

## Quick Decision: Which Write Mode?

| Requirement | Use |
|---|---|
| Atomicity — all-or-nothing | `Transactional` |
| Large-scale production loads | `Queued` |
| Materialized Views downstream | `Queued` |
| Sub-second latency, small record rate | `KustoStreaming` |
| Spark Structured Streaming | `Queued` (not `KustoStreaming`) |
| Need to see service-side failures in Spark | `Transactional` |


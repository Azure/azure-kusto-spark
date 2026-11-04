# Ingest-v2 write path (OneLake / Fabric Private Link)

This document describes how the connector writes to Kusto through the `kusto-ingest-v2` Java SDK.
The path exists so that writes keep working where the service asks clients to use REST ingestion
and OneLake staging, for example Microsoft Fabric with Private Link. Behaviour follows the .NET
Kusto ingest SDK (`Kusto.Ingest.V2`).

## 1. Turning it on

The path is **off by default**. Open-source users keep the existing ingestion path unchanged.

| Option | Default | Meaning |
|---|---|---|
| `enableIngestV2` (hidden) | `false` | Allows the write to use ingest-v2 when the service asks for it. Set by the Fabric connector wrapper, the same way the read side sets `enableExportStorageApi`. |
| `storageTokenProviderClasspath` | none | Class implementing `com.microsoft.kusto.spark.datasink.StorageTokenProvider`. It returns the token used to upload CSV files to OneLake. Loaded only when `enableIngestV2` is `true`. It needs a constructor that takes the options map, like `tokenProviderClasspath`. |
| `ingestionFormat` | `CSV` | `CSV` or `Parquet`. Parquet is only used with OneLake lake folders (see 5). |

## 2. Which path a write takes

```
enableIngestV2 = false                         -> existing path
writeMode = KustoStreaming                     -> existing path (the configuration API is not called)
GET {dm}/v1/rest/ingestion/configuration fails -> existing path
service asks for neither "Rest" ingestion
  nor "Lake" upload (case-insensitive)         -> existing path
a setting ingest-v2 cannot honour (below)      -> existing path
otherwise                                      -> ingest-v2
```

`{dm}` is `ingestionUrl` when given, otherwise `https://ingest-<cluster host>`, the same URL the
existing path uses. The configuration is cached per DM URL and credentials for the service's
`refreshInterval` (at most 1 hour). Only one call per key runs at a time; other writers wait for
it. The first load is tried up to 3 times on transient errors and a failure is remembered for
10 seconds. If a later refresh fails, the last good configuration keeps being used and the refresh
is retried after 1 minute, so a short outage does not move writes back to the existing path.
The SDK client reads its configuration from this same cache.

Settings that keep the existing path:

- a user-provided ingestion storage (`ingestionStorage`)
- `ensureNoDuplicatedBlobs`
- the service returned lake folders but no `storageTokenProviderClasspath` is configured (CSV)
- both an inline CSV mapping and a mapping reference
- an inline CSV mapping not in the connector's JSON form (`columnName`, `columnType`, `properties`)
- a lake folder that is not `https` on a known OneLake host: the hosts already trusted by the
  read side (`ContainerProvider.isSupportedApiOneLakeHost`), or a Private Link workspace host `{workspaceid}.z{xy}.{dfs|blob|onelake}.fabric.microsoft.com`
  (with an optional environment prefix such as `daily-`). Tokens are never sent to other hosts.
- Parquet together with any CSV mapping, `GenerateDynamicCsvMapping` or `addSourceLocationTransform`

## 3. What ingest-v2 does

Table creation, schema adjustment, the staging (temporary) table and its policies are prepared
exactly as in the existing path. Only the "upload and ingest" step changes.

1. **Upload targets.** Chosen like the SDKs: only lake folders -> OneLake; only containers ->
   storage; both -> `preferredUploadMethod` decides (`Lake` -> OneLake, anything else -> storage).
2. **CSV.** Each Spark partition writes gzipped CSV blobs with the existing serializer.
   - Storage containers: written with the container SAS returned by the service.
   - OneLake: the lake folder URL is split like the .NET `LakeFolder` (`.dfs.` -> `.blob.`, first
     path segment is the container) and written with the token from `StorageTokenProvider`,
     in 4 MiB blocks with 4 parallel block uploads per blob.
   - A blob that reaches the size limit (`min(batchLimit MB, maxDataSize)`) is submitted with
     batching skipped, unless `disableFlushImmediately` is set; the last blob is batched normally.
     This matches the existing path.
   - Blobs are submitted with `/v1/rest/ingestion/queued` in groups of at most `maxBlobsPerBatch`
     (capped at 20, the service limit per request).
   - A submit is retried (up to 5 attempts, jittered backoff, one 10-minute deadline) only when
     it cannot have been accepted: HTTP 429/503 without an operation id, or a connection failure.
     Other errors fail the task, because the queued API is not idempotent.
3. **Parquet (OneLake only).** Spark writes the DataFrame over `abfss` into a new folder
   `spark-ingest-v2/<requestId>/<uuid>` under the lake folder, then the files are submitted in
   groups. The shared Spark/Hadoop configuration is not changed.
4. **Ingestion properties.** `skipBatching`, drop-by/ingest-by/additional tags,
   `ingestIfNotExists`, `creationTime`, the mapping reference and the inline mapping are sent.
   Tracking is turned on only for transactional writes, which need the status; queued writes
   do not ask for it, like the existing path.
5. **Queued mode** returns once all blobs are submitted, like the existing path. Operations are
   counted on the driver, not collected, so large writes do not grow driver memory.
6. **Transactional mode** waits for every operation, using one deadline for all of them
   (`timeout`). Status is read in parallel groups of 32; the write fails as soon as a group shows
   a failed or canceled blob or a permanent error (HTTP 400/401/403). An operation the service
   does not know yet (404 or all counts zero) is retried, but fails after 15 minutes.
   Then, like `FinalizeHelper`, merge is
   disabled on the staging table, extents are moved to the target and the staging table is dropped.
   The driver always polls; `pollingOnDriver` is not used by this path.
7. **Async mode** (`isAsync`) runs the same steps in the background and only logs errors.

## 4. Falling back to the existing path

If ingest-v2 fails, the write is retried on the existing path **only when that cannot load data
twice**:

- nothing was submitted yet, or
- the mode is transactional, the connector owns the staging table and moving extents has not
  started. The staging table is dropped and the retry uses a new staging table name.

Otherwise the error is returned to the caller. Async writes never fall back. If the retry fails
too, the ingest-v2 error is attached to it as a suppressed exception.

## 5. Known limits

- **Streaming** writes always use the existing path.
- **Parquet** needs OneLake lake folders. With storage containers the write falls back to CSV with
  a warning.
- **Staged files** in OneLake are not deleted by the connector. Neither the service nor the .NET
  SDK deletes them; folder retention should be managed on the Fabric side.
- **Spark task retries** can submit a partition's data twice: blobs submitted by a failed task
  attempt may still be ingested. The existing path has the same behaviour.
- **`ingestIfNotExists`** is cleared before the "already ingested" check on both paths
  (`KustoWriter.getSparkIngestionProperties`), so it never skips. This path keeps that
  behaviour; the fix for both paths is in #526.
- **SDK logging.** `kusto-ingest-v2` 0.0.1-beta logs the full ingestion configuration, including
  container URLs with SAS, at DEBUG (`ConfigurationClient`). Do not enable DEBUG for
  `com.microsoft.azure.kusto.ingest.v2` until the SDK redacts it.
- **Token audience.** The SDK asks for `<dm url>/.default`. The connector follows the .NET
  `Kusto.Ingest.V2` client (`IngestHttpClient.AuthenticateAsync`) instead:
  - the ingestion endpoint must be a trusted Kusto endpoint (same check as the V1 path);
  - Entra ID credentials (application, certificate, managed identity, Key Vault) ask for
    `<KustoServiceResourceId>/.default` from `/v1/rest/auth/metadata`, with no MFA rewrite;
  - if that endpoint answers with an error status or no AAD details, the public default
    `https://kusto.kusto.windows.net` is used; if it cannot be reached, the write fails;
  - user tokens and token providers are used as given, so they must already be for that resource.
- **Private Link headers.** No extra S2S actor header is sent; the SDK makes it optional.
  Confirm with the service team that this is enough in every Private Link setup.

## 6. Classes (`com.microsoft.kusto.spark.datasink.v2`)

| Class | Role |
|---|---|
| `IngestV2ConfigurationProvider` | Reads and caches the ingestion configuration. |
| `IngestionConfig`, `UploadTargets` | Routing decision and target selection. Never prints URLs. |
| `IngestV2WriterOrchestrator` | Plan, run, fallback rules, transactional finish. |
| `IngestV2QueuedWriter` | Per-partition CSV upload and submit. `LakeFolderLocation` parsing. |
| `IngestV2ParquetWriter` | Parquet staging over `abfss` and submit. |
| `IngestV2RequestProperties` | Maps `SparkIngestionProperties` to ingest-v2 properties. |
| `IngestV2StatusTracker` | Waits for operations with one shared deadline. |
| `IngestV2ClientCache`, `IngestV2ClientProvider` | Cached queued ingest clients. |
| `IngestV2Authentication` | Maps connector authentication to Azure `TokenCredential`s. |
| `IngestV2FallbackException`, `IngestV2SubmittedOperation` | Fallback signal; serializable operation record. |

## 7. Wrapper checklist (azure-kusto-tools / Fabric)

1. Set `enableIngestV2=true` for writes.
2. Set `storageTokenProviderClasspath` to a provider that returns a OneLake storage token.
3. Validate on a Private Link workspace in both queued and transactional modes, and with
   `ingestionFormat=Parquet`.

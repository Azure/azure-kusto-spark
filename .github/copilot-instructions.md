# Copilot Instructions — Azure Data Explorer Connector for Apache Spark

## Build & Test

```shell
# Build (skip tests)
mvn clean package -DskipTests

# Run all unit tests (E2E tests are excluded by default via the KustoE2E tag)
mvn clean verify -DskipTests=false

# Run a single test class
mvn test -pl connector -Dsuites="com.microsoft.kusto.spark.KustoFilterTests"

# Format Scala code (Spotless + scalafmt)
mvn spotless:apply

# Check formatting without applying
mvn spotless:check

# Run scalafix lint rules
mvn scalafix:scalafix -pl connector
```

E2E tests require a live Kusto cluster and Azure credentials. They are tagged `KustoE2E` and excluded from normal runs. CI passes cluster/storage config via environment variables and Maven system properties (see `.github/workflows/build.yml`).

## Architecture

This is a **Spark DataSource V1** connector registered as the `"kusto"` format. It is a multi-module Maven project:

- **`connector/`** — the published library (`kusto-spark_<sparkVersion>_<scalaVersion>`)
- **`samples/`** — usage examples in Scala, Python, and notebook formats

### Read Path

`DefaultSource` → `KustoRelation` → `KustoReader`

The reader has two modes selected automatically (or forced via `readMode` option):
- **Single mode** — direct query for small results (≤5000 rows). Results deserialized in-driver via `KustoResponseDeserializer`.
- **Distributed mode** — Kusto exports data to temporary blob storage, then Spark reads it in parallel partitions.

`KustoFilter` converts Spark `Filter` objects into KQL `where`/`project` clauses for filter and column pushdown.

### Write Path

`DefaultSource` / `KustoSinkProvider` → `KustoWriter`

Three write modes:
- **Transactional** — creates a temp table, ingests, polls for completion, then atomically moves extents to the target table.
- **Queued** — fire-and-forget ingestion via the Kusto ingestion service.
- **KustoStreaming** — low-latency streaming ingestion in configurable chunk sizes.

Data is serialized as gzip-compressed CSV, uploaded to transient blob containers managed by the Kusto service, then ingested. `FinalizeHelper` handles post-ingestion cleanup and extent moves.

`KustoSink` / `KustoSinkProvider` implement Spark Structured Streaming support.

### Key Infrastructure Classes

- **`ExtendedKustoClient`** — wraps the Kusto Java SDK with lazy client initialization, retry logic (resilience4j), and transient storage management.
- **`KustoClientCache`** — thread-safe `ConcurrentHashMap` cache of clients keyed by cluster+auth.
- **`ContainerProvider`** — fetches and caches temporary blob containers from the Kusto service with retry.
- **`CslCommandsGenerator`** — generates KQL management commands (`.create table`, `.show schema`, `.export`, etc.).
- **`KustoDataSourceUtils`** — central config parser that resolves auth, coordinates, and storage parameters from the Spark options map.

### Authentication

`KustoAuthentication` is a sealed trait with implementations:
- `AadApplicationAuthentication` — service principal (app ID + secret)
- `AadApplicationCertificateAuthentication` — PFX certificate
- `ManagedIdentityAuthentication` — Azure Managed Identity
- `KeyVaultAppAuthentication` — credentials fetched from Azure Key Vault
- `KustoAccessTokenAuthentication` — pre-acquired bearer token

### Shading

The connector publishes an **uber-jar** with aggressive dependency shading (Maven Shade Plugin). All transitive dependencies (Netty, Azure SDK, Jackson, Guava, Kusto SDK internals, etc.) are relocated under `kusto_connector_shaded.*`. The public API packages (`authentication`, `common`, `datasink`, `datasource`, `utils`) are **not** shaded. When adding a new dependency, ensure it is included in the shade relocations in `connector/pom.xml` to avoid classpath conflicts with Spark runtimes.

### Reactor/Shading Pitfall

Do not use `reactor.core.publisher.Mono` directly in executor-side code—Reactor is shaded in the uber-jar. Use `CloudInfo.retrieveCloudInfoForCluster` for cache pre-warming rather than `CloudInfo.manuallyAddToCache(url, Mono.just(...))`.

## Key Conventions

- **Scala version**: 2.13, targeting Spark 4.0 and Java 21.
- **Test framework**: ScalaTest `AnyFlatSpec` style with `Matchers` and `MockFactory` (ScalaMock). E2E tests are tagged with `KustoE2E`.
- **Scalafix rules** are strict: no `var`, no `null`, no `throw`, no `return`, no `while`, no `asInstanceOf`/`isInstanceOf`, no default args (see `.scalafix.conf`).
- **Formatting**: scalafmt via Spotless Maven plugin. Config in `.scalafmt.conf` — max 98 columns, no alignment.
- **Line endings**: Unix (LF) enforced by Spotless.
- **Configuration keys** are defined as string constants in `KustoOptions` (common), `KustoSourceOptions` (read), and `KustoSinkOptions` (write). Add new options there, not as inline strings.
- **Type mapping** between Spark and Kusto lives in `DataTypeMapping`. Kusto `timespan` maps to Spark `StringType` (not a duration type). Complex Spark types (Array, Struct, Map) map to Kusto `dynamic`.
- **Retry logic** uses resilience4j (`RetryConfig`). Max 2 retry attempts for ingestion. Commands use exponential backoff.

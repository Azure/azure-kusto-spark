# Auto-Detection Flow: Complete Implementation Guide

**Purpose**: Understand how the auto-detection works from user write to ingestion  
**Date**: 2026-05-27  
**Branch**: feature/ingestv2-config-api-integration  
**Status**: Implementation Complete

---

## Table of Contents

1. [Overview](#overview)
2. [Complete Flow Diagram](#complete-flow-diagram)
3. [File-by-File Changes](#file-by-file-changes)
4. [Step-by-Step Execution Flow](#step-by-step-execution-flow)
5. [Validation Checklist](#validation-checklist)
6. [Testing the Flow](#testing-the-flow)

---

## Overview

### What Was Implemented

**Goal**: Auto-detect V2 support via config API, eliminating manual `useIngestV2` flag.

**Contract**: 
```
Query /v1/rest/ingestion/configuration
IF preferredIngestionMethod == "REST" → Use V2
ELSE → Use V1
```

**Architecture Principle**: 
- ✅ V2 code isolated in `v2` package
- ✅ V1 code 100% untouched (enables clean deletion)
- ✅ One minimal integration point (routing in KustoWriter)

---

## Complete Flow Diagram

```
User DataFrame Write
       ↓
KustoDataSink.addBatch()
       ↓
KustoWriter.write()                           ← FILE: KustoWriter.scala (CHANGED)
       ↓
       ├─── Manual override check
       │    if (writeOptions.useIngestV2)
       │         └─→ shouldUseV2 = true
       │
       └─── Auto-detection
            if (!writeOptions.useIngestV2)
                 ↓
            IngestV2Detector.isV2Supported()  ← FILE: IngestV2Detector.scala (NEW)
                 ↓
            IngestV2ConfigurationProvider      ← FILE: IngestV2ConfigurationProvider.scala (NEW)
              .getConfiguration()
                 ↓
            Query: GET /v1/rest/ingestion/configuration
                 ↓
            Parse response                     ← FILE: IngestionConfig.scala (NEW)
                 ↓
            Extract preferredIngestionMethod
                 ↓
            ├─── "REST" → return true
            └─── else → return false

       ↓
if (shouldUseV2) {
    ↓
    IngestV2WriterOrchestrator.write()        ← FILE: IngestV2WriterOrchestrator.scala (CHANGED)
         ↓
    Validate config API response
         ↓
    if (preferredIngestionMethod != "REST")
         └─→ throw error
         ↓
    Create V2 clients
         ↓
    IngestV2QueuedWriter.write()              ← FILE: IngestV2QueuedWriter.scala (CHANGED)
         ↓
    Use maxBlobsPerBatch from config
         ↓
    V2 ingestion (REST-based)
} else {
    ↓
    V1 ingestion (queue-based)                ← ALL V1 CODE UNCHANGED
}
```

---

## File-by-File Changes

### NEW Files (Created)

#### 1. IngestV2Detector.scala
**Location**: `connector/src/main/scala/com/microsoft/kusto/spark/datasink/v2/IngestV2Detector.scala`  
**Lines**: 75  
**Purpose**: Auto-detection logic - queries config API and returns boolean

**Key Method**:
```scala
def isV2Supported(
    coordinates: KustoCoordinates,
    authentication: KustoAuthentication): Boolean
```

**What It Does**:
1. Constructs DM URL from coordinates
2. Calls `IngestV2ConfigurationProvider.getConfiguration()`
3. Checks `preferredIngestionMethod` field
4. Returns `true` if "REST", `false` otherwise
5. Logs decision clearly

**Code Flow**:
```scala
Line 33-43: isV2Supported() method signature and DM URL construction
Line 45-48: Query config API via IngestV2ConfigurationProvider
Line 50-69: Match on config response
  Line 51-55: If preferredIngestionMethod == "REST" → true
  Line 57-61: If preferredIngestionMethod != "REST" → false
  Line 63-67: If config unavailable → false (fallback)
```

---

#### 2. IngestV2ConfigurationProvider.scala
**Location**: `connector/src/main/scala/com/microsoft/kusto/spark/datasink/v2/IngestV2ConfigurationProvider.scala`  
**Lines**: 150  
**Purpose**: Query config API and cache results

**Key Method**:
```scala
def getConfiguration(
    dmUrl: String,
    authentication: KustoAuthentication): Option[IngestionConfig]
```

**What It Does**:
1. Checks cache first (per DM URL)
2. If not cached, creates ConfigurationClient
3. Queries `/v1/rest/ingestion/configuration` endpoint
4. Parses response into IngestionConfig
5. Caches result for session lifetime
6. Returns Option[IngestionConfig] (None on error)

**Code Flow**:
```scala
Line 29-31: Cache declaration (ConcurrentHashMap)
Line 40-50: getConfiguration() entry point
Line 42-47: Check cache, return if present
Line 49: Query config API if not cached
Line 66-101: queryConfigApi() - makes HTTP request
  Line 76-82: Create ConfigurationClient with auth
  Line 84-91: Call getConfigurationDetails() (Kotlin suspend function)
  Line 93-99: Parse response into IngestionConfig
Line 103-124: parseAndCacheConfig() - parse and store
  Line 109-120: Parse ConfigurationResponse
  Line 122: Cache result
Line 116-124: Logging at INFO level for debugging
```

---

#### 3. IngestionConfig.scala
**Location**: `connector/src/main/scala/com/microsoft/kusto/spark/datasink/v2/IngestionConfig.scala`  
**Lines**: 120  
**Purpose**: Parse ConfigurationResponse from V2 SDK

**Key Class**:
```scala
case class IngestionConfig(
  preferredIngestionMethod: String,  // "REST" or "Legacy"
  preferredUploadMethod: String,      // "Blob" or "Lake"
  maxBlobsPerBatch: Int,
  maxDataSizeBytes: Long,
  blobPaths: Seq[String],
  oneLakePaths: Seq[String]
)
```

**What It Does**:
1. Defines case class for config data
2. Provides fromConfigurationResponse() parser
3. Uses reflection to access Kotlin data class properties
4. Handles missing fields with defaults

**Code Flow**:
```scala
Line 12-26: Case class definition
Line 28-100: Companion object with parser
Line 39-100: fromConfigurationResponse() method
  Line 46-67: Extract fields using reflection (Kotlin interop)
  Line 46-50: Get preferredIngestionMethod
  Line 51-55: Get preferredUploadMethod
  Line 56-60: Get maxBlobsPerBatch
  Line 69-99: Create IngestionConfig instance with defaults
```

---

### CHANGED Files (Modified)

#### 4. KustoWriter.scala
**Location**: `connector/src/main/scala/com/microsoft/kusto/spark/datasink/KustoWriter.scala`  
**Lines Changed**: 11 lines (routing logic only)  
**Purpose**: Add auto-detection to routing decision

**What Changed**:

**BEFORE** (commit 901461f):
```scala
// Line 83-84
if (writeOptions.useIngestV2) {
  // V2 path
  v2.IngestV2WriterOrchestrator.write(...)
  return
}
// V1 path continues below
```

**AFTER** (commit f5c45dd):
```scala
// Line 84-93: Auto-detection with manual override
val shouldUseV2 = if (writeOptions.useIngestV2) {
  KDSU.logInfo(
    className,
    "Using V2 ingestion path (manual override: useIngestV2=true)")
  true
} else {
  // Auto-detection via config API
  v2.IngestV2Detector.isV2Supported(tableCoordinates, authentication)
}

// Line 95-113: Route based on detection
if (shouldUseV2) {
  val table = tableCoordinates.table.get
  val batchIdIfExists = batchId.map(b => s"${b.toString}").getOrElse("")
  val tmpTableName: String = KDSU.generateTempTableName(...)
  v2.IngestV2WriterOrchestrator.write(
    data, tableCoordinates, authentication, writeOptions, tmpTableName, crp)
  return
}

// Line 115-118: V1 path (UNCHANGED)
// ==========================================
// V1 INGESTION PATH (queue-based)
// ALL CODE BELOW IS UNCHANGED - enables clean V1 deletion later
// ==========================================
```

**Code Flow**:
```scala
Line 75-81: write() method signature (unchanged)
Line 84-93: NEW - Auto-detection logic
  Line 85-89: Check manual override first
  Line 91-92: If no override, call IngestV2Detector
Line 95-113: Routing based on detection result
  Line 96-112: V2 path (existing code, just indented)
  Line 113: return (exit early if V2)
Line 115-end: V1 path (100% UNCHANGED)
```

**What Stayed the Same**:
- ✅ V2 orchestrator call (same parameters)
- ✅ All V1 code below routing point (zero changes)
- ✅ Method signature and error handling

---

#### 5. IngestV2WriterOrchestrator.scala
**Location**: `connector/src/main/scala/com/microsoft/kusto/spark/datasink/v2/IngestV2WriterOrchestrator.scala`  
**Lines Changed**: Added config API validation  
**Purpose**: Query config API and validate V2 support at write start

**What Changed**:

**Code Flow**:
```scala
Line 40-63: write() method (unchanged signature)
Line 64-98: NEW - Query config API and validate
  Line 65-69: Query config API via IngestV2ConfigurationProvider
  Line 71-84: Validate preferredIngestionMethod
    Line 72-75: If "REST" → proceed
    Line 77-83: If not "REST" → throw error with message
  Line 86-97: Extract dmConfig for executors
Line 100-end: Rest of orchestration logic (existing)
```

**Key Addition**:
```scala
// Line 65-69: Query config API
val configOpt = IngestV2ConfigurationProvider.getConfiguration(dmUrl, authentication)

// Line 71-84: Validate config
configOpt match {
  case Some(config) if config.preferredIngestionMethod == "REST" =>
    // Proceed with V2
  case Some(config) =>
    throw new IllegalStateException(
      s"V2 ingestion not supported. Config API returned preferredIngestionMethod=${config.preferredIngestionMethod}. " +
      s"Please remove 'useIngestV2=true' or use a V2-enabled cluster.")
  case None =>
    throw new IllegalStateException("Config API not available")
}
```

---

#### 6. IngestV2QueuedWriter.scala
**Location**: `connector/src/main/scala/com/microsoft/kusto/spark/datasink/v2/IngestV2QueuedWriter.scala`  
**Lines Changed**: Added dmConfig parameter  
**Purpose**: Use maxBlobsPerBatch from config API

**What Changed**:

**Code Flow**:
```scala
Line 52-88: write() method - added dmConfig parameter
Line 86: Extract maxBlobsPerBatch from config
  val maxBlobsPerBatch = dmConfig.map(_.maxBlobsPerBatch).getOrElse(70)
Line 125: Use maxBlobsPerBatch instead of hardcoded value
```

**Before**:
```scala
val maxBlobsPerBatch = 70  // Hardcoded
```

**After**:
```scala
val maxBlobsPerBatch = dmConfig.map(_.maxBlobsPerBatch).getOrElse(70)  // From config
```

---

### UNCHANGED Files (V1 Code)

**All V1 ingestion code remains 100% unchanged**, including:

- ✅ `KustoClientCache.scala` - V1 client caching
- ✅ `ExtendedKustoClient.scala` - V1 client implementation
- ✅ `KustoIngestionUtils.scala` - V1 ingestion utilities
- ✅ `KustoWriter.scala` (below line 115) - All V1 ingestion logic
- ✅ `SparkIngestionProperties.scala` - V1 properties
- ✅ All other V1 files

**This ensures**:
- No regression in V1 functionality
- Clean deletion path (remove code below routing point)
- Zero coupling between V1 and new V2 detection code

---

## Step-by-Step Execution Flow

### Scenario 1: V2 Auto-Detection (No Manual Flag)

**User Code**:
```scala
df.write
  .format("com.microsoft.kusto.spark.datasource")
  .option("kustoCluster", "https://v2cluster.kusto.windows.net")
  .option("kustoDatabase", "TestDB")
  .option("kustoTable", "TestTable")
  .option("kustoAADAppID", appId)
  .option("kustoAADAppSecret", appSecret)
  .option("kustoAADAuthorityID", tenantId)
  // NO useIngestV2 option!
  .mode("Append")
  .save()
```

**Execution Flow**:

1. **DataSource Entry Point**
   ```
   File: KustoDataSink.scala (unchanged)
   → addBatch() called
   → Calls KustoWriter.write()
   ```

2. **KustoWriter Routing**
   ```
   File: KustoWriter.scala
   Line 84: shouldUseV2 calculation starts
   Line 85: Check writeOptions.useIngestV2
   Result: false (not set)
   Line 91: Call IngestV2Detector.isV2Supported()
   ```

3. **Auto-Detection**
   ```
   File: IngestV2Detector.scala
   Line 38: Extract dmUrl from coordinates
   Result: "https://ingest-v2cluster.kusto.windows.net"
   
   Line 45: Call IngestV2ConfigurationProvider.getConfiguration()
   ```

4. **Config API Query**
   ```
   File: IngestV2ConfigurationProvider.scala
   Line 42: Check cache for dmUrl
   Result: Not cached (first call)
   
   Line 49: Call queryConfigApi()
   Line 76: Create ConfigurationClient with dmUrl + auth
   Line 84: Call client.getConfigurationDetails()
   HTTP: GET https://ingest-v2cluster.kusto.windows.net/v1/rest/ingestion/configuration
   
   Response received (JSON):
   {
     "containerSettings": {...},
     "ingestionSettings": {
       "preferredIngestionMethod": "REST",
       "maxBlobsPerBatch": 70
     }
   }
   ```

5. **Config Parsing**
   ```
   File: IngestionConfig.scala
   Line 39: fromConfigurationResponse() called
   Line 46-67: Extract fields using reflection
   Result: IngestionConfig(
     preferredIngestionMethod = "REST",
     preferredUploadMethod = "Blob",
     maxBlobsPerBatch = 70,
     ...
   )
   ```

6. **Cache and Return**
   ```
   File: IngestV2ConfigurationProvider.scala
   Line 122: Cache config for dmUrl
   Line 116: Log config at INFO level
   LOG: "Config API response - preferredIngestionMethod: REST, maxBlobsPerBatch: 70"
   
   Return: Some(IngestionConfig(...))
   ```

7. **Detection Decision**
   ```
   File: IngestV2Detector.scala
   Line 51: Match on config.preferredIngestionMethod
   Result: "REST"
   Line 52-54: Log and return true
   LOG: "Config API: preferredIngestionMethod=REST → V2 ingestion enabled (auto-detected)"
   
   Return: true
   ```

8. **Back to KustoWriter**
   ```
   File: KustoWriter.scala
   Line 92: shouldUseV2 = true (from detector)
   Line 96: Check if (shouldUseV2)
   Result: true → enter V2 path
   Line 105: Call IngestV2WriterOrchestrator.write()
   ```

9. **V2 Orchestrator Validation**
   ```
   File: IngestV2WriterOrchestrator.scala
   Line 65: Query config API again (uses cache!)
   Line 72: Validate preferredIngestionMethod == "REST"
   Result: Valid, proceed
   Line 86: Extract dmConfig for executors
   ```

10. **V2 Queued Writer**
    ```
    File: IngestV2QueuedWriter.scala
    Line 86: Extract maxBlobsPerBatch from dmConfig
    Result: 70 (from config API)
    Line 125: Use maxBlobsPerBatch for batching
    → V2 ingestion proceeds with REST-based ingestion
    ```

**Result**: ✅ V2 ingestion used (auto-detected)

---

### Scenario 2: V1 Auto-Fallback (Config Returns "Legacy")

**User Code**: Same as Scenario 1, but different cluster

**Execution Flow**:

1-4. Same as Scenario 1 through config API query

5. **Config API Response**
   ```
   HTTP: GET https://ingest-v1cluster.kusto.windows.net/v1/rest/ingestion/configuration
   
   Response received (JSON):
   {
     "containerSettings": {...},
     "ingestionSettings": {
       "preferredIngestionMethod": "Legacy",  ← Different!
       "maxBlobsPerBatch": 70
     }
   }
   ```

6. **Detection Decision**
   ```
   File: IngestV2Detector.scala
   Line 51: Match on config.preferredIngestionMethod
   Result: "Legacy"
   Line 57-60: Log and return false
   LOG: "Config API: preferredIngestionMethod=Legacy → V1 ingestion (auto-fallback)"
   
   Return: false
   ```

7. **Back to KustoWriter**
   ```
   File: KustoWriter.scala
   Line 92: shouldUseV2 = false (from detector)
   Line 96: Check if (shouldUseV2)
   Result: false → skip V2 path
   Line 113: return not executed
   Line 115+: Continue to V1 path
   ```

8. **V1 Ingestion (Unchanged Code)**
   ```
   File: KustoWriter.scala (lines 120+)
   → All existing V1 logic executes
   → Queue-based ingestion
   → No changes to V1 behavior
   ```

**Result**: ✅ V1 ingestion used (auto-fallback)

---

### Scenario 3: Manual Override (useIngestV2=true)

**User Code**:
```scala
df.write
  .format("com.microsoft.kusto.spark.datasource")
  .option("kustoCluster", "https://v2cluster.kusto.windows.net")
  .option("kustoDatabase", "TestDB")
  .option("kustoTable", "TestTable")
  .option("kustoAADAppID", appId)
  .option("kustoAADAppSecret", appSecret)
  .option("kustoAADAuthorityID", tenantId)
  .option("useIngestV2", "true")  ← Manual override!
  .mode("Append")
  .save()
```

**Execution Flow**:

1-2. Same as Scenario 1 through KustoWriter entry

3. **Manual Override Check**
   ```
   File: KustoWriter.scala
   Line 85: Check writeOptions.useIngestV2
   Result: true (set by user)
   Line 86-88: Log manual override
   LOG: "Using V2 ingestion path (manual override: useIngestV2=true)"
   Line 89: Return true immediately
   ```

4. **Skip Auto-Detection**
   ```
   Line 91: IngestV2Detector.isV2Supported() NOT CALLED
   Config API: NOT QUERIED (manual override takes precedence)
   ```

5. **V2 Path**
   ```
   Line 92: shouldUseV2 = true
   Line 96: if (shouldUseV2) → true
   Line 105: Call IngestV2WriterOrchestrator.write()
   → V2 ingestion proceeds
   ```

**Result**: ✅ V2 ingestion used (manual override)

---

### Scenario 4: Config API Unavailable (404 or Network Error)

**Execution Flow**:

1-4. Same as Scenario 1 through config API query

5. **Config API Fails**
   ```
   File: IngestV2ConfigurationProvider.scala
   Line 84: Call client.getConfigurationDetails()
   HTTP: GET https://ingest-oldcluster.kusto.windows.net/v1/rest/ingestion/configuration
   Response: 404 Not Found (endpoint doesn't exist)
   
   Line 91: Exception caught
   Line 113: Log error at DEBUG level
   LOG: "Config API query failed: 404 Not Found"
   Return: None
   ```

6. **Detection Decision**
   ```
   File: IngestV2Detector.scala
   Line 51: Match on config
   Result: None (config unavailable)
   Line 63-66: Log and return false
   LOG: "Config API unavailable for <dmUrl> → V1 ingestion (auto-fallback)"
   
   Return: false
   ```

7. **V1 Fallback**
   ```
   File: KustoWriter.scala
   Line 92: shouldUseV2 = false
   Line 96: if (shouldUseV2) → false
   → Continue to V1 path
   ```

**Result**: ✅ V1 ingestion used (graceful fallback)

---

## Validation Checklist

### Code Changes Validation

Use this checklist to verify the implementation:

**✅ 1. NEW Files Created**
- [ ] `IngestV2Detector.scala` exists in `v2` package
- [ ] `IngestV2ConfigurationProvider.scala` exists in `v2` package
- [ ] `IngestionConfig.scala` exists in `v2` package
- [ ] All 3 files compile without errors
- [ ] All 3 files in built JAR (check with `jar tf`)

**✅ 2. CHANGED Files Modified Correctly**
- [ ] `KustoWriter.scala` has auto-detection logic (line 84-93)
- [ ] `KustoWriter.scala` has V1 unchanged comment (line 115-118)
- [ ] `IngestV2WriterOrchestrator.scala` has config validation (line 64-98)
- [ ] `IngestV2QueuedWriter.scala` uses maxBlobsPerBatch from config (line 86)

**✅ 3. UNCHANGED Files Not Modified**
- [ ] `KustoClientCache.scala` unchanged
- [ ] `ExtendedKustoClient.scala` unchanged
- [ ] `KustoWriter.scala` lines 120+ unchanged (V1 path)
- [ ] Git diff shows only expected files changed

**✅ 4. Architecture Principles**
- [ ] V2 code isolated in `v2` package (no V2 code in root datasink)
- [ ] V1 code 100% untouched (can be deleted cleanly)
- [ ] Only one integration point (KustoWriter routing)
- [ ] No abstractions coupling V1 to new code

**✅ 5. Config API Contract**
- [ ] IngestV2Detector queries config API
- [ ] Checks `preferredIngestionMethod` field
- [ ] Returns true for "REST", false otherwise
- [ ] Caches config per DM URL
- [ ] Logs decision clearly

**✅ 6. Build Validation**
- [ ] `mvn clean compile` succeeds
- [ ] `mvn package` succeeds
- [ ] JAR file created (39 MB expected)
- [ ] Only 3 pre-existing deprecation warnings
- [ ] No new compilation errors

---

## Testing the Flow

### Quick Validation Test (No V2 Cluster Needed)

You can validate the code structure without a V2 cluster:

**1. Check Files Exist**
```bash
cd /home/asaharn/work/projects/azure-kusto-spark

# NEW files
ls -lh connector/src/main/scala/com/microsoft/kusto/spark/datasink/v2/IngestV2Detector.scala
ls -lh connector/src/main/scala/com/microsoft/kusto/spark/datasink/v2/IngestV2ConfigurationProvider.scala
ls -lh connector/src/main/scala/com/microsoft/kusto/spark/datasink/v2/IngestionConfig.scala

# Should show 3 files with dates
```

**2. Check Code Locations**
```bash
# Auto-detection logic in KustoWriter
grep -n "IngestV2Detector.isV2Supported" connector/src/main/scala/com/microsoft/kusto/spark/datasink/KustoWriter.scala
# Expected: Line 92

# V1 unchanged comment
grep -n "V1 INGESTION PATH" connector/src/main/scala/com/microsoft/kusto/spark/datasink/KustoWriter.scala
# Expected: Line 116

# Config validation in orchestrator
grep -n "preferredIngestionMethod" connector/src/main/scala/com/microsoft/kusto/spark/datasink/v2/IngestV2WriterOrchestrator.scala
# Expected: Multiple lines
```

**3. Check Build**
```bash
# Compile
mvn clean compile -DskipTests -pl connector -am

# Package
mvn package -DskipTests -pl connector -am

# Check JAR
ls -lh connector/target/kusto-spark_4.0_2.13-7.0.6.jar
# Expected: ~39 MB

# Verify V2 classes in JAR
jar tf connector/target/kusto-spark_4.0_2.13-7.0.6.jar | grep IngestV2Detector
# Expected: Shows IngestV2Detector class files
```

**4. Check Git Status**
```bash
# View commits
git log --oneline -5
# Expected: Shows 5 commits on feature branch

# View changed files in latest commit
git diff --stat HEAD~1..HEAD
# Expected: Shows 6 files changed (+109, -297)

# Verify branch
git branch --show-current
# Expected: feature/ingestv2-config-api-integration
```

---

### Full Integration Test (Requires V2 Cluster)

When you have V2 cluster access:

**Test 1: V2 Auto-Detection**
```scala
val df = Seq((1, "test")).toDF("id", "name")

df.write
  .format("com.microsoft.kusto.spark.datasource")
  .option("kustoCluster", "https://v2cluster.kusto.windows.net")
  .option("kustoDatabase", "TestDB")
  .option("kustoTable", "TestTable")
  .option("kustoAADAppID", appId)
  .option("kustoAADAppSecret", appSecret)
  .option("kustoAADAuthorityID", tenantId)
  // NO useIngestV2!
  .mode("Append")
  .save()

// Check Spark logs for:
// "Config API: preferredIngestionMethod=REST → V2 ingestion enabled (auto-detected)"
```

**Validate**:
- [ ] Write succeeds
- [ ] Log shows auto-detection
- [ ] Data appears in table
- [ ] No errors about V2

---

## Summary

### Implementation Complete

**What Was Built**:
- ✅ 3 new files (Detector, ConfigProvider, IngestionConfig)
- ✅ 3 modified files (KustoWriter routing, V2 orchestrator validation, V2 writer config)
- ✅ 0 V1 files changed (clean separation)

**How It Works**:
1. User writes without `useIngestV2` flag
2. KustoWriter checks manual override (if set, honor it)
3. If no override, call IngestV2Detector
4. Detector queries config API and caches result
5. Parse `preferredIngestionMethod` field
6. If "REST" → V2 path
7. If "Legacy"/404/error → V1 path (unchanged code)

**Why This Architecture**:
- V1 code untouched (enables clean deletion)
- V2 code isolated (in v2 package)
- One integration point (routing in KustoWriter)
- Config API contract honored (preferredIngestionMethod)

**Validation**:
- ✅ Code compiles
- ✅ JAR builds (39 MB)
- ✅ V2 classes packaged
- ⏳ Awaiting V2 cluster for testing

---

**Document Status**: Complete  
**Last Updated**: 2026-05-27 14:13 UTC  
**Ready For**: Validation and Testing

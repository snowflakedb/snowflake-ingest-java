# Plan: Remove `snowflake-jdbc-thin` Dependency

## Background

The SDK depends on `net.snowflake:snowflake-jdbc-thin:3.25.1` for several purposes.
The goal is to replicate all required classes/functionality directly in this repo and
eliminate the JDBC dependency entirely.

---

## Guiding Principle: Replication Only — No Functional Changes

**This project is a pure mechanical migration. There must be no functional changes.**

- Copy JDBC classes verbatim into this repo. Compare against the actual source file
  in the JDBC repo (not decompiled output). Do not refactor, simplify, rename fields,
  change method signatures, strip comments/Javadoc, or improve logic — even where
  improvements seem obvious.
- **Replicate recursively:** If a replicated class depends on another JDBC class
  (e.g. `SFLogger`, `FileUtil`, `ArgSupplier`), replicate that dependency too rather
  than replacing it with an alternative. This keeps the replicated files identical to
  the JDBC source (only the `package` line changes).
- Do not change the runtime behavior of any existing feature. The before and after must
  be functionally identical from the perspective of callers.
- Do not fix bugs encountered in JDBC code during the migration. If a bug is found,
  file a separate ticket and replicate the buggy behavior as-is for now.
- Do not add new functionality, new configuration options, or new error handling paths.
- The only permitted changes to replicated classes are:
  1. `package` line (different package).
  2. `@SnowflakeJdbcInternalApi` annotation removed (JDBC-internal marker).
  3. Formatting differences enforced by `./format.sh` (google-java-format).
- The only permitted changes to the surrounding ingest code are:
  1. Updating `import` statements to point to the new ingest-owned classes.
  2. Removing the JDBC dependency from `pom.xml` and the shade plugin configuration.
  3. Updating test imports to match.

Any improvement ideas discovered during the migration should be tracked as follow-up
tickets, not implemented here. Keeping this PR a pure replication makes it easy to
review, diff against the JDBC source, and roll back if needed.

---

## Replication Verification: Diff Report

After every class is replicated in each phase, Claude must fetch the original source
from the JDBC repo and emit a structured diff report before the phase PR is opened.

### How to run

For each replicated class:

1. **Fetch the JDBC original** from `snowflakedb/snowflake-jdbc` at tag `v3.25.1`
   (the pinned version in `pom.xml`) using the GitHub MCP tool:
   ```
   repo: snowflakedb/snowflake-jdbc
   ref:  refs/tags/v3.25.1
   path: src/main/java/net/snowflake/client/<subpackage>/<ClassName>.java
   ```

2. **Normalise both sides** before diffing — strip the following expected mechanical
   differences so they don't obscure real divergence:
   - `package` declaration (old: `net.snowflake.client.*`, new: `net.snowflake.ingest.*`)
   - `import` lines that are direct replacements of JDBC types with ingest-owned types
   - Copyright header year changes (if any)

3. **Diff the normalised bodies** line-by-line. Any remaining difference is a potential
   functional change and must be explained.

### Report format

Emit one entry per class in the following format:

```
## <ClassName>
JDBC source : net/snowflake/client/<subpackage>/<ClassName>.java @ v3.25.1
Ingest copy : src/main/java/net/snowflake/ingest/<subpackage>/<ClassName>.java

Permitted differences (mechanical):
  - package declaration changed
  - <N> import lines substituted (list each old→new pair)

Unexpected differences: NONE
  — or —
Unexpected differences:
  Line <N>: <description of what changed and why it is safe / needs review>
```

If **any** unexpected difference exists that is not purely mechanical (package/imports),
it must be flagged for human review before the PR is merged. The diff report must be
pasted as a comment on the PR so reviewers can verify fidelity at a glance.

### Permitted vs. unexpected differences

| Type of change | Permitted? |
|---|---|
| `package` line updated to ingest package | Yes |
| `import` lines swapped for ingest-owned equivalents | Yes |
| Copyright year / header text | Yes |
| Whitespace / blank line normalisation | Yes |
| Field renamed or reordered | **No — flag for review** |
| Method body logic changed | **No — flag for review** |
| Method added or removed | **No — flag for review** |
| Exception type changed | **No — flag for review** |
| Constant value changed | **No — flag for review** |

---

## Why JDBC Is Used

### 1. File Transfer Infrastructure (heaviest use)
The core upload path depends on JDBC's `SnowflakeFileTransferAgent` and its supporting
types to stage files to cloud storage (S3, Azure, GCS).

**Non-Iceberg path** (`InternalStage.putRemote` — `else` branch):
- `SnowflakeFileTransferAgent.getFileTransferMetadatas(JsonNode)` — parses stage credentials
  from the configure response into a metadata object.
- `SnowflakeFileTransferAgent.uploadWithoutConnection(SnowflakeFileTransferConfig)` — full
  upload pipeline with retries, encryption, etc.
- `SnowflakeFileTransferConfig` — config builder passed to the above.
- `SnowflakeFileTransferMetadataV1` — rich metadata object wrapping stage credentials,
  encryption material, presigned URL, stage info, and command type.
- `RemoteStoreFileEncryptionMaterial` — holds encryption key, query ID, SMK ID.

**Iceberg path** (`IcebergFileTransferAgent` and storage clients) — already partially
decoupled with its own S3/Azure/GCS clients — but still depends on:
- `StageInfo` — cloud stage type, location, credentials, region, endpoint, presigned URL.
- `SnowflakeFileTransferMetadataV1` / `FileBackedOutputStream` (from JDBC).
- `HttpClientSettingsKey`, `OCSPMode`, `SFSSLConnectionSocketFactory` (from `client.core`).
- Exception types: `SnowflakeSQLException`, `SnowflakeSQLLoggedException`.
- Error/state types: `ErrorCode`, `SqlState`.
- Utility: `SnowflakeUtil`, `SFPair`, `Stopwatch`.

### 2. Proxy / Session Properties
`SFSessionProperty` (an enum of string property keys) is used in:
- `HttpUtil.generateProxyPropertiesForJDBC()` — packs proxy settings into a `Properties` map
  using JDBC property key names, which is then consumed by the JDBC file transfer agent.
- `Utils.createProperties()` — stores the private key under `SFSessionProperty.PRIVATE_KEY`.
- `IcebergS3Client` / `IcebergAzureClient` — reads proxy settings back out using the same
  `SFSessionProperty` keys.
- `SnowflakeStreamingIngestClientInternal` — reads the private key from properties.

### 3. Telemetry
- `TelemetryClient` + `TelemetryUtil` — used in `TelemetryService` to batch and send
  SDK telemetry to Snowflake over HTTP using a sessionless telemetry endpoint.

### 4. Data Validation Utilities
- `Power10` (`intTable`, `sb16Table`, `sb16Size`) — integer and BigInteger lookup tables for
  powers of 10. Used in `TimestampWrapper` and `DataValidationUtil`.
- `SnowflakeDateTimeFormat` — only referenced in comments for constant alignment, not called.
  The constants (`SECONDS_LIMIT_FOR_EPOCH`, etc.) can be inlined.

---

## Complete List of JDBC Classes to Replicate

| JDBC Class | Package | Used In | Complexity |
|---|---|---|---|
| `SFSessionProperty` | `client.core` | `Utils`, `HttpUtil`, `IcebergS3Client`, `IcebergAzureClient`, `SnowflakeStreamingIngestClientInternal` | Low — enum of string keys |
| `OCSPMode` | `client.core` | `InternalStage`, `IcebergFileTransferAgent` | Low — simple enum |
| `HttpUtil` (JDBC's) | `client.core` | `IcebergStorageClientFactory`, `IcebergS3Client` (via `isSocksProxyDisabled()`) | Low — single method |
| `Constants.CLOUD_STORAGE_CREDENTIALS_EXPIRED` | `client.core` | `IcebergS3Client`, `IcebergAzureClient` | Low — inline as int constant |
| `SFSSLConnectionSocketFactory` | `client.core` | `IcebergS3Client` | Medium — SSL socket factory |
| `HttpClientSettingsKey` | `client.core` | `IcebergFileTransferAgent`, `IcebergGCSClient` | Medium — key object for HTTP settings |
| `SnowflakeFileTransferAgent` | `client.jdbc` | `InternalStage` | **High** — parse metadata + upload |
| `SnowflakeFileTransferConfig` | `client.jdbc` | `InternalStage` | Medium — config builder |
| `SnowflakeFileTransferMetadataV1` | `client.jdbc` | `InternalStage`, `SnowflakeFileTransferMetadataWithAge` | **High** — rich metadata object |
| `SnowflakeSQLException` | `client.jdbc` | `InternalStage`, storage clients, `InternalStageManager` | Low — extend `SQLException` |
| `SnowflakeSQLLoggedException` | `client.jdbc` | Storage clients | Low — extend `SnowflakeSQLException` |
| `FileBackedOutputStream` | `client.jdbc` | `IcebergFileTransferAgent`, `StorageHelper`, storage clients | Medium — Guava-backed stream |
| `SnowflakeUtil` | `client.jdbc` | `IcebergFileTransferAgent`, `IcebergCommonObjectMetadata`, `IcebergS3Client`, `IcebergAzureClient`, `IcebergGCSClient` | Medium — 5 methods: `createCaseInsensitiveMap`, `convertProxyPropertiesToHttpClientKey`, `getRootCause`, `isBlank`, `createDefaultExecutorService` |
| `ErrorCode` | `client.jdbc` | Storage clients | Low — error code enum |
| `StageInfo` | `client.jdbc.cloud.storage` | `InternalStage`, `IcebergFileTransferAgent`, storage clients, `IcebergStorageClientFactory` | **High** — rich stage descriptor |
| `StorageObjectMetadata` | `client.jdbc.cloud.storage` | `IcebergFileTransferAgent`, storage clients, `IcebergStorageClientFactory`, `IcebergCommonObjectMetadata` | Medium — interface |
| `RemoteStoreFileEncryptionMaterial` | `client.jdbc.internal.snowflake.common.core` | `InternalStage` | Low — data holder |
| `SnowflakeDateTimeFormat` | `client.jdbc.internal.snowflake.common.core` | `DataValidationUtil` (comments only) | None — inline constants |
| `SqlState` | `client.jdbc.internal.snowflake.common.core` | Storage clients | Low — string constants |
| `Power10` | `client.jdbc.internal.snowflake.common.util` | `TimestampWrapper`, `DataValidationUtil` | Low — lookup table |
| `TelemetryClient` | `client.jdbc.telemetry` | `TelemetryService` | **High** — HTTP telemetry batching |
| `TelemetryUtil` | `client.jdbc.telemetry` | `TelemetryService` | Low — JSON builder |
| `SFPair` | `client.util` | S3, Azure, GCS clients | Low — generic pair/tuple |
| `Stopwatch` | `client.util` | S3, Azure, GCS clients | Low — timing utility |

---

## Migration Plan — Iterative Removal

Replace JDBC classes one at a time, iteratively. At each step, pick a JDBC class
that can be **fully** removed from all files that use it. The replacement class
may temporarily depend on other JDBC classes that haven't been replaced yet —
that's fine. As more JDBC classes get replaced, update earlier replacements to
use ingest versions.

**Key constraint:** A JDBC class can only be replaced when **no call site passes
it to another JDBC method**. If our `ErrorCode` is passed to JDBC's
`SnowflakeSQLException(ErrorCode, ...)`, we must replace `SnowflakeSQLException`
first (or replace both together). Otherwise we'd have to change behavior at the
call site, which violates the replication-only principle.

**Naming rule:** Replicated classes keep the same name as in JDBC. Where a name
collides with an existing ingest class (e.g. `ErrorCode`), use package separation
— the replicated class goes into the `fileTransferAgent` package.

**PR size limit:** Each PR should target around 200 lines of production code
changes (new classes + import swaps). Test code is not counted toward this limit.
Exception: if a single replicated class is larger than 200 lines, it's OK to
exceed the limit. A step may be split across multiple PRs if needed.

### Dependency Graph

```
Independent — can replace any time, not passed to any JDBC method:
  Power10              ← TimestampWrapper, DataValidationUtil
  SFPair               ← S3, Azure, GCS clients
  Stopwatch            ← S3, Azure, GCS clients
  SFSessionProperty    ← HttpUtil, Utils, S3, Azure clients, ClientInternal
  SqlState             ← S3, Azure, GCS, StorageClient interface
  CLOUD_STORAGE_CREDENTIALS_EXPIRED ← S3, Azure clients
  StorageObjectMetadata ← interface; all storage clients, FileTransferAgent
  FileBackedOutputStream ← all storage clients, FileTransferAgent, StorageHelper
  HttpUtil (JDBC)       ← isSocksProxyDisabled(); S3, StorageClientFactory
  SFSSLConnectionSocketFactory ← S3 client only
  SnowflakeUtil methods ← S3, Azure, GCS, FileTransferAgent (except
                           convertProxyPropertiesToHttpClientKey — see below)
  TelemetryClient/Util  ← TelemetryService only

Must replace together — passed to each other's JDBC constructors:
  ErrorCode (JDBC)     ← passed to SnowflakeSQLException(ErrorCode, ...) constructor
  SnowflakeSQLException ← constructor takes JDBC ErrorCode; thrown by storage clients
  SnowflakeSQLLoggedException ← extends SnowflakeSQLException

Can replace — replacement may temporarily import other JDBC classes:
  OCSPMode             ← IcebergFileTransferAgent, InternalStage*
  HttpClientSettingsKey ← depends on OCSPMode; FileTransferAgent, GCS, StorageClient

Blocked — returned by / passed to SnowflakeFileTransferAgent:
  StageInfo            ← returned by getFileTransferMetadatas() via metadata.getStageInfo()
  RemoteStoreFileEncryptionMaterial ← accessed via SnowflakeFileTransferMetadataV1
  SnowflakeFileTransferMetadataV1 ← returned by / passed to SnowflakeFileTransferAgent
  SnowflakeFileTransferConfig ← config builder for SnowflakeFileTransferAgent
  OCSPMode in InternalStage ← passed to SnowflakeFileTransferConfig.setOcspMode()

JDBC entry points — must be replaced to unblock the above:
  SnowflakeFileTransferAgent:
    .getFileTransferMetadatas()  → InternalStage (parse configure response)
    .uploadWithoutConnection()   → InternalStage (non-Iceberg upload)
    .throwJCEMissingError()      → S3, Azure, GCS clients
    .throwNoSpaceLeftError()     → S3, Azure, GCS clients
```

*`InternalStage` passes `OCSPMode` to `SnowflakeFileTransferConfig.setOcspMode()`.
The `OCSPMode` swap in `InternalStage` is blocked until `SnowflakeFileTransferAgent`
is replaced.

---

### Progress Summary (updated 2026-03-30)

| Step | Status | PR |
|---|---|---|
| Step 1 — Simple utilities | ✅ MERGED | #1100 |
| Step 2 — Enums, constants, proxy utils | ✅ MERGED | #1109 |
| Step 2b — SFLogger infrastructure | ✅ MERGED | #1112 |
| Step 3 — Storage data types + utils | ✅ MERGED | #1110 |
| Step 4 — HTTP settings + SSL factory | ✅ MERGED | #1111 |
| Step 5a — ErrorCode, ResourceBundleManager | ✅ MERGED | #1113 |
| Step 5b — SnowflakeSQLException, SnowflakeSQLLoggedException | ✅ MERGED | #1114 |
| Step 6 — Telemetry | ✅ MERGED | #1115 |
| Step 7a — Inline error helpers, swap exceptions | ✅ MERGED | #1116 |
| Step 7b — Replicate getFileTransferMetadatas | ✅ MERGED | #1119 |
| Step 8a — Replicate StageInfo, RemoteStoreFileEncryptionMaterial | ✅ MERGED | #1120 |
| Step 8b — Replicate MetadataV1, ObjectMapperFactory, SqlState | ✅ MERGED | #1121 |
| Step 8c — Replicate storage helpers + interface | ✅ Open | #1123 |
| Step 8d — Replicate storage client implementations | ✅ Open | #1124 |
| Step 8e — Swap ALL imports at once | ✅ Open | #1127 |
| Step 9a — Mechanical import swaps in replicated classes | ✅ Open | #1128 |
| Step 9b — Replicate OOB telemetry infrastructure | ⬜ TODO | — |
| Step 9c — Swap telemetry imports in SnowflakeSQLLoggedException + TelemetryClient | ⬜ TODO | — |
| Step 10 — Remove JDBC dependency | ⬜ TODO | — |

**Closed PRs:** #1117 (reverted 7b approach), #1122 (reverted 8c approach)
**Other PRs:** #1118 (error/exception tests on master)

---

### Step 1 — Simple utilities ✅ MERGED

**Done:** `Power10`, `SFPair`, `Stopwatch`. Swap imports.

**Files fully freed of JDBC imports:** `DataValidationUtil`, `TimestampWrapper`

---

### Step 2 — Enums, constants, proxy utils ✅ (PR #1109)

**Done:**
- `SFSessionProperty` — enum with 9 property keys, swap all imports
- `SqlState` — SQL state constants (same package, no import needed)
- `OCSPMode` — enum class created (swap blocked — see Step 4)
- `HttpUtil.isSocksProxyDisabled()` — add to ingest's HttpUtil, swap import
- Rename `generateProxyPropertiesForJDBC()` → `generateProxyProperties()`

**Left for later:** `ErrorCode` + `CLOUD_STORAGE_CREDENTIALS_EXPIRED` (passed as
type to JDBC's `SnowflakeSQLException(ErrorCode, ...)` — must replace together
with exceptions, see Step 5).

**Files fully freed:** `Utils`, `HttpUtil`, `SnowflakeStreamingIngestClientInternal`

---

### Step 2b — SFLogger infrastructure ✅ (PR #1112)

**Done:** Replicated 17 logging classes recursively so replicated classes use
`SFLogger`/`SFLoggerFactory` verbatim:
- Core: `SFLogger`, `SFLoggerFactory`, `SLF4JLogger`, `JDK14Logger`, `ArgSupplier`
- JDK14 deps: `SFFormatter`, `StdOutConsoleHandler`,
  `StdErrOutThresholdAwareConsoleHandler`, `UnknownJavaUtilLoggingLevelException`
- Event/security: `SecretDetector`, `EventHandler`, `EventUtil`, `Event`,
  `BasicEvent`, `UUIDUtils`
- Helpers: `LogUtil` (replaces `SnowflakeUtil.systemGetProperty`/`isNullOrEmpty`)
- `IncidentUtil` stubbed to constants (full class requires `javax.servlet`)
- Added `json-smart` as direct dependency (used by `SecretDetector`)

---

### Step 3 — Storage data types + utils ✅ (PR #1110)

**Done:**
- `StorageObjectMetadata` — own interface (same package)
- `FileBackedOutputStream` — verbatim copy (zero diff from JDBC source)
- `FileUtil` — `logFileUsage` method replicated
- `StorageClientUtil` — `getRootCause`, `isBlank`, `createCaseInsensitiveMap`,
  `systemGetProperty`, `isNullOrEmpty`, `isWindows`

**Files fully freed:** `IcebergCommonObjectMetadata`, `IcebergS3ObjectMetadata`,
`StorageHelper`

---

### Step 4 — HTTP settings + SSL factory ✅ (PR #1111)

**Done:**
- `HttpClientSettingsKey` — verbatim copy with `isNullOrEmpty` from
  `StorageClientUtil`
- `HttpProtocol` — enum replicated
- `IngestSSLConnectionSocketFactory` — replicated (renamed from
  `SFSSLConnectionSocketFactory`)
- `StorageClientUtil` additions: `convertProxyPropertiesToHttpClientKey`
  (temporarily throws JDBC's `SnowflakeSQLException`),
  `createDefaultExecutorService`
- Swapped `OCSPMode` in `IcebergFileTransferAgent`

**Left:** `StorageClientUtil.convertProxyPropertiesToHttpClientKey` still throws
JDBC's `SnowflakeSQLException` — will be updated when exceptions are replaced.

**JDBC imports removed:** `HttpClientSettingsKey`, `SFSSLConnectionSocketFactory`,
`SnowflakeUtil`, `HttpUtil` (JDBC), `OCSPMode` (in `IcebergFileTransferAgent`)

---

### Step 5a — ErrorCode, ResourceBundleManager ✅ (PR #1113)

**Done:**
- `ErrorCode` — full enum with all 69 error codes (verbatim copy). `SqlState`
  import temporarily uses JDBC shaded path. `errorMessageResource` path updated.
- `ResourceBundleManager` — from snowflake-common (decompiled, no public source)
- `jdbc_error_messages.properties` — error message templates from JDBC

No import swaps — storage clients still import JDBC's `ErrorCode` (explicit import
takes precedence over same-package class).

---

### Step 5b — SnowflakeSQLException, SnowflakeSQLLoggedException ✅ (PR #1114)

**Done:**
- `SnowflakeSQLException` — verbatim copy. `SFException` import kept from JDBC
  temporarily (one constructor, never called by ingest).
- `SnowflakeSQLLoggedException` — verbatim copy. JDBC telemetry/session imports
  kept temporarily (all callers pass null for session).
- `CLOUD_STORAGE_CREDENTIALS_EXPIRED` constant added to `ErrorCode`.

**Left for Step 7:** Import swaps in storage clients deferred — swapping would
require widening `throws SnowflakeSQLException` to `throws SQLException` because
`SnowflakeFileTransferAgent` still throws JDBC's version. Once Step 7 replaces
`SnowflakeFileTransferAgent`, nothing throws JDBC's exception and the swap can be
done cleanly.

---

### Step 6 — Telemetry ✅ (PR #1115)

**Done:** Replicated 5 JDBC telemetry classes into
`net.snowflake.ingest.connection.telemetry`:
- `TelemetryClient` — batches and sends telemetry via HTTP POST. JDBC imports
  kept temporarily (`HttpUtil`, `ObjectMapperFactory`, `SFSession`,
  `SnowflakeConnectionV1`, `SnowflakeSQLException`, `TelemetryThreadPool`).
- `TelemetryUtil`, `Telemetry` (interface), `TelemetryData`, `TelemetryField`

`TelemetryService` now imports from ingest telemetry package.

**Files fully freed:** `TelemetryService`

---

### Step 7a — Inline error helpers, swap exception imports ✅ (PR #1116)

**Done:**
- Inlined `throwJCEMissingError()`/`throwNoSpaceLeftError()` from
  `SnowflakeFileTransferAgent` into `StorageClientUtil`
- Swapped `ErrorCode`, `SnowflakeSQLException`, `SnowflakeSQLLoggedException`,
  `CLOUD_STORAGE_CREDENTIALS_EXPIRED` imports in storage clients
- Widened `throws` clauses to `SQLException` (covers both JDBC and ingest
  exception types during transition)
- Removed `SnowflakeFileTransferAgent` import from all storage clients

**JDBC imports removed from storage clients:** `SnowflakeFileTransferAgent`,
`ErrorCode`, `SnowflakeSQLException`, `SnowflakeSQLLoggedException`,
`CLOUD_STORAGE_CREDENTIALS_EXPIRED`

---

### Step 7b — Replicate getFileTransferMetadatas ⬜ TODO

Replicate the static parsing methods from `SnowflakeFileTransferAgent` that
`InternalStage` uses to convert the configure response JSON into
`SnowflakeFileTransferMetadataV1`. Verbatim copy of JDBC source.

**Methods to replicate:**
- `getFileTransferMetadatas(JsonNode)` — 1-arg wrapper
- `getFileTransferMetadatas(JsonNode, String)` — main parser
- `getStageInfo(JsonNode, SFSession)` — extracts StageInfo from JSON
- `extractStageCreds(JsonNode, String)` — parses credentials
- `getEncryptionMaterial(CommandType, JsonNode)` — parses encryption material
- `expandFileNames(String[], String)` — resolves file path globs
- `setupUseRegionalUrl(JsonNode, StageInfo)` — sets regional URL flag
- `setupUseVirtualUrl(JsonNode, StageInfo)` — sets virtual URL flag

**Also replicate:**
- `CommandType` enum (from `SFBaseFileTransferAgent`)
- `SnowflakeFileTransferMetadata` interface (return type)

**Swap in `InternalStage`:** `SnowflakeFileTransferAgent.getFileTransferMetadatas()` →
ingest's replicated version.

**JDBC imports removed from `InternalStage`:** `SnowflakeFileTransferAgent` (partially —
`uploadWithoutConnection` still uses JDBC, see Step 7c)

---

### Step 8a — Replicate StageInfo, RemoteStoreFileEncryptionMaterial ⬜ TODO

Replicate the two data types that are used across the most files.

- `StageInfo` (~229 lines) — stage descriptor with `StageType` enum, credentials,
  region, endpoint, presigned URL. Used in 6 files.
- `RemoteStoreFileEncryptionMaterial` (~40 lines) — simple data holder with
  `queryStageMasterKey`, `queryId`, `smkId`. From snowflake-common (decompiled).

Swap imports in: `IcebergFileTransferAgent`, `IcebergS3Client`, `IcebergAzureClient`,
`IcebergGCSClient`, `IcebergStorageClientFactory`, replicated `SnowflakeFileTransferAgent`.

**Not swapped yet in `InternalStage`** — depends on `SnowflakeFileTransferMetadataV1`
which still returns JDBC's `StageInfo` from `getStageInfo()`.

---

### Step 8b — Replicate SnowflakeFileTransferMetadataV1, ObjectMapperFactory ⬜ TODO

- `SnowflakeFileTransferMetadataV1` (~109 lines) — file transfer metadata.
  Depends on `StageInfo`, `RemoteStoreFileEncryptionMaterial` (from 8a),
  `CommandType` (from 7b).
- `ObjectMapperFactory` (~38 lines) — singleton `ObjectMapper` factory.
  Used in 5 files (replicated `SnowflakeFileTransferAgent`, `TelemetryClient`,
  `TelemetryUtil`, `TelemetryData`, `SnowflakeSQLLoggedException`).

Swap imports everywhere. Now `InternalStage` can use ingest's types throughout.

**Also swap in `InternalStage`:**
- `getFileTransferMetadatas()` → ingest's replicated version (deferred from 7b)
- `SnowflakeSQLException` → ingest's version
- `OCSPMode` → ingest's version
- Remove `parseConfigureResponseMapper` / `parseFileLocationInfo` (Jackson workaround)

**Also swap:** `SnowflakeSQLException` in `InternalStageManager`,
`SnowflakeFileTransferMetadataV1` in `SnowflakeFileTransferMetadataWithAge`.

---

### Step 8c — Replicate storage helper classes and interface ✅ OPEN (PR #1123)

**Done:** Verbatim replication of JDBC storage infrastructure:
- `SnowflakeFileTransferConfig` (237 lines) — config builder
- `SnowflakeStorageClient` (452 lines) — storage client interface
- `MatDesc` (101 lines) — encryption material descriptor
- `EncryptionProvider` (214 lines) — AES CBC encryption
- `GcmEncryptionProvider` (254 lines) — AES GCM encryption
- `StorageProviderException` (51 lines) — storage error wrapper
- `StorageObjectSummary` (161 lines) — storage object properties
- `StorageObjectSummaryCollection` (60 lines) — collection with iterator
- `S3ObjectSummariesIterator` (41 lines) — S3 summary iterator
- `AzureObjectSummariesIterator` (58 lines) — Azure summary iterator
- `GcsObjectSummariesIterator` (28 lines) — GCS summary iterator
- Added `aws-java-sdk-kms` dependency (for iterator exception type)

---

### Step 8d — Replicate storage client implementations ✅ OPEN (PR #1124)

**Done:** Verbatim replication of JDBC storage client implementations:
- `StorageClientFactory` (~234 lines) — creates cloud-specific clients
- `SnowflakeS3Client` (~1038 lines) — S3 upload/download with encryption
- `SnowflakeAzureClient` (~1055 lines) — Azure Blob upload/download
- `SnowflakeGCSClient` (~1282 lines) — GCS upload/download with presigned URLs
- `S3ObjectMetadata` / `CommonObjectMetadata` — `StorageObjectMetadata` impls
- `HttpHeadersCustomizer` interface + `HeaderCustomizerHttpRequestInterceptor`
- `FileCompressionType` — file compression type enum
- `GCSAccessStrategy`, `GCSAccessStrategyAwsSdk`, `GCSDefaultAccessStrategy`
- `S3HttpUtil` — S3-specific HTTP/proxy configuration
- `uploadWithoutConnection`, `pushFileToRemoteStore`,
  `pushFileToRemoteStoreWithPresignedUrl`, `renewExpiredToken`,
  `parseCommandInGS`, `getLocalFilePathFromCommand` added to
  replicated `SnowflakeFileTransferAgent`

Both Iceberg clients and replicated Snowflake clients coexist.
`SFSession` references kept temporarily (always null from callers).

---

### Step 8e — Swap ALL imports at once ✅ OPEN (PR #1127)

**Done:**
- `InternalStage`: swapped all 7 JDBC imports, removed
  `parseConfigureResponseMapper` (no longer needed — replicated agent uses
  same Jackson version), simplified `parseFileLocationInfo` →
  `createFileTransferNode`
- `InternalStageManager`: swapped `SnowflakeSQLException`
- `SnowflakeFileTransferMetadataWithAge`: swapped `SnowflakeFileTransferMetadataV1`
- Iceberg clients: removed `StageInfo`, `MetadataV1` imports (same package)
- `InternalStageTest`: updated all JDBC imports

**InternalStage and all `streaming/internal/` files now fully free of JDBC
imports.**

---

### Step 9a — Mechanical import swaps in replicated classes ✅ OPEN (PR #1128)

**Done:** Mechanical import swaps only — no logic changes:
- `SqlState` in `ErrorCode`, `SnowflakeFileTransferAgent` → removed (same package)
- `ObjectMapperFactory` in `TelemetryClient`, `TelemetryData`, `TelemetryUtil` →
  swapped to ingest's replicated version
- `SecretDetector` in `TelemetryData` → swapped to ingest's replicated version
- `SFLoggerUtil.isVariableProvided` in `S3HttpUtil` → inlined (semantically
  equivalent ternary, avoids replicating SFLoggerUtil for one trivial method)
- `SnowflakeUtil` in S3/Azure/GCS clients → `StorageClientUtil` (added
  `assureOnlyUserAccessibleFilePermissions` verbatim from JDBC)
- `SFSessionProperty` in S3/Azure/GCS clients → ingest version (added
  `PUT_GET_MAX_RETRIES` to ingest `SFSessionProperty`)
- `SnowflakeUtil.createCaseInsensitiveMap` in `GCSDefaultAccessStrategy` →
  `StorageClientUtil`

---

### Step 9b — Replicate OOB telemetry infrastructure ⬜ TODO

Replicate the OOB (out-of-band) telemetry classes that
`SnowflakeSQLLoggedException` depends on. Verbatim replication — no stubs,
no logic changes.

**Classes to replicate (~1170 lines total):**
- `TelemetryThreadPool` (61 lines) — singleton thread pool for async telemetry.
  No JDBC deps beyond package.
- `SFTimestamp` (20 lines) — UTC timestamp formatter. No JDBC deps.
- `SnowflakeConnectString` (256 lines) — connection string parser. Deps:
  `SFSessionProperty` (already replicated), `SecretDetector` (already
  replicated), `SFLogger` (already replicated).
- `TelemetryEvent` (182 lines) — telemetry event builder. Deps:
  `SFException` (JDBC, kept temporarily), `UUIDUtils` (already replicated),
  `SFTimestamp` (replicated above), `SecretDetector` (already replicated),
  `ResourceBundleManager` (already replicated).
- `TelemetryService` (651 lines) — OOB telemetry singleton. Deps:
  `SnowflakeConnectString` (replicated above), `SecretDetector` (already
  replicated), `Stopwatch` (already replicated), `TelemetryThreadPool`
  (replicated above).

**Also add constants:**
- `LoginInfoDTO.SF_JDBC_APP_ID` = `"JDBC"` (from snowflake-common)
- `SnowflakeDriver.implementVersion` = `"3.25.1"` (version string)

**Suggested PR split (~200 lines production code per PR):**
- PR 1: `TelemetryThreadPool`, `SFTimestamp`, `SnowflakeConnectString`,
  constants (~340 lines)
- PR 2: `TelemetryEvent`, `TelemetryService` (~830 lines — single class
  exceeds 200-line limit, OK per plan rules)

---

### Step 9c — Swap telemetry imports in SnowflakeSQLLoggedException + TelemetryClient ⬜ TODO

Mechanical import swaps only — no logic changes, no method removal:
- `SnowflakeSQLLoggedException`: swap all 12 JDBC telemetry imports to
  ingest replicated versions (from Step 9b). Full `sendTelemetryData()`
  body preserved verbatim.
- `TelemetryClient`: swap `TelemetryThreadPool` to ingest version
  (from Step 9b). Remaining JDBC imports (`HttpUtil`, `SFSession`,
  `SnowflakeConnectionV1`, `SnowflakeSQLException`) kept until Step 10.

---

### Step 10 — Remove JDBC Dependency ⬜ TODO

Remaining JDBC imports after Step 9c (all in `fileTransferAgent/`):
- `SFSession`/`SFBaseSession` (15 imports) — always null from callers.
  Need stubs or parameter type changes.
- `SFException` (2 imports) — used in one `SnowflakeSQLException` constructor
  (never called) and caught in `parseCommandInGS` (calls JDBC's `SFStatement`).
- `SnowflakeConnectionV1`, `SnowflakeSQLException` in `TelemetryClient`
  — session-based code path, never used by ingest.
- `HttpUtil` in `TelemetryClient` — `executeGeneralRequest` for both
  session and sessionless paths.
- GCS HTTP types (4 imports) — `ExecTimeTelemetryData`, `HttpResponseContextDto`,
  `HttpUtil`, `RestRequest` in `SnowflakeGCSClient`.

Then:
1. Create `SFSession`/`SFBaseSession` stubs or change parameter types
2. Replace remaining JDBC HTTP utilities with direct Apache HttpClient
3. Change `snowflake-jdbc-thin` scope to `test` in `pom.xml`
4. Remove JDBC shade relocation rules from Maven Shade plugin
5. Remove `snowflake-jdbc-thin` from `public_pom.xml`
6. Run full test suite

---

## Files Affected Summary

| File | Change |
|---|---|
| `pom.xml` | Remove `snowflake-jdbc-thin` dependency and shade rule |
| `utils/HttpUtil.java` | Remove `SFSessionProperty` import; add `isSocksProxyDisabled()` |
| `utils/Utils.java` | Replace `SFSessionProperty` with own class |
| `connection/TelemetryService.java` | Replace `TelemetryClient`/`TelemetryUtil` with own HTTP sender |
| `streaming/internal/InternalStage.java` | Replace JDBC agent + metadata with own classes |
| `streaming/internal/InternalStageManager.java` | Replace `SnowflakeSQLException` import |
| `streaming/internal/SnowflakeFileTransferMetadataWithAge.java` | Replace `SnowflakeFileTransferMetadataV1` |
| `streaming/internal/DataValidationUtil.java` | Replace `Power10` import; inline epoch constants |
| `streaming/internal/TimestampWrapper.java` | Replace `Power10` import |
| `streaming/internal/SnowflakeStreamingIngestClientInternal.java` | Replace `SFSessionProperty` |
| `streaming/internal/fileTransferAgent/IcebergFileTransferAgent.java` | Replace all JDBC types |
| `streaming/internal/fileTransferAgent/IcebergStorageClientFactory.java` | Replace `StageInfo`, `HttpUtil`, exceptions |
| `streaming/internal/fileTransferAgent/IcebergStorageClient.java` | Replace exception types |
| `streaming/internal/fileTransferAgent/IcebergS3Client.java` | Replace all JDBC types |
| `streaming/internal/fileTransferAgent/IcebergAzureClient.java` | Replace all JDBC types |
| `streaming/internal/fileTransferAgent/IcebergGCSClient.java` | Replace all JDBC types |
| `streaming/internal/fileTransferAgent/IcebergS3ObjectMetadata.java` | Replace `StorageObjectMetadata`, `SnowflakeUtil` |
| `streaming/internal/fileTransferAgent/IcebergCommonObjectMetadata.java` | Replace `StorageObjectMetadata`, `SnowflakeUtil` |
| `streaming/internal/fileTransferAgent/StorageHelper.java` | Replace `FileBackedOutputStream` |
| `streaming/internal/InternalStageTest.java` | Update all JDBC imports; extend with ported JDBC file-transfer tests |
| `streaming/internal/SnowflakeStreamingIngestClientTest.java` | Replace `SFSessionProperty` import |
| `streaming/internal/SnowflakeStreamingIngestChannelTest.java` | Replace `SFSessionProperty` import |
| *(new)* `utils/Power10Test.java` | Port from JDBC |
| *(new)* `utils/SnowflakeSQLExceptionTest.java` | Port from JDBC |
| *(new)* `fileTransferAgent/StageInfoTest.java` | Port from JDBC |
| *(new)* `streaming/internal/SnowflakeFileTransferMetadataV1Test.java` | Port from JDBC |
| *(new)* `connection/TelemetryHttpSenderTest.java` | Port from JDBC |

---

## New Classes to Create

| New Class | Purpose |
|---|---|
| `utils/Power10.java` | Powers-of-10 lookup tables |
| `utils/SFPair.java` | Generic pair/tuple |
| `utils/Stopwatch.java` | Nanosecond-resolution timer |
| `utils/OCSPMode.java` | Enum: `FAIL_OPEN`, `FAIL_CLOSED` |
| `utils/SFSessionProperty.java` | Property key constants |
| `utils/SnowflakeSQLException.java` | Checked exception extending `SQLException` |
| `utils/SnowflakeSQLLoggedException.java` | Extends `SnowflakeSQLException` |
| `fileTransferAgent/SqlState.java` | SQL state string constants |
| `fileTransferAgent/ErrorCode.java` | Error codes for storage clients (same name as JDBC, different package from `utils.ErrorCode`) |
| `fileTransferAgent/HttpClientSettingsKey.java` | HTTP client settings key |
| `fileTransferAgent/StorageObjectMetadata.java` | Own interface (replaces JDBC interface) |
| `fileTransferAgent/StageInfo.java` | Cloud stage descriptor + `StageType` enum |
| `streaming/internal/RemoteStoreFileEncryptionMaterial.java` | Encryption material holder |
| `streaming/internal/SnowflakeFileTransferMetadataV1.java` | File transfer metadata |
| `connection/TelemetryHttpSender.java` | Replaces `TelemetryClient` / `TelemetryUtil` |

---

## Test Migration Policy

For every class replicated from JDBC, the corresponding unit tests from the JDBC repo
(`snowflakedb/snowflake-jdbc`) must also be ported into this repo. This ensures the
replicated implementation is correct and that behavioral regressions are caught locally
without relying on the JDBC test suite.

### Process for each class

1. **Find the test class** — In the JDBC repo, test classes live under
   `src/test/java/net/snowflake/client/`. The test class for `Foo.java` is typically
   `FooTest.java` or `FooLatestIT.java` in the corresponding sub-package.

2. **Copy the relevant test methods** — Unit tests only (not integration tests that
   require a live Snowflake connection). Skip JDBC-internal tests that exercise
   behaviour we are not replicating.

3. **Update imports and package declarations** — Change:
   - `package net.snowflake.client.*` → appropriate `net.snowflake.ingest.*` package
   - All `import net.snowflake.client.*` → corresponding ingest imports
   - Any JDBC-specific test helpers → ingest equivalents or inline stubs

4. **Place the test file** — Mirror the production class location under
   `src/test/java/`. For example, a class created at
   `src/main/java/net/snowflake/ingest/utils/Power10.java` gets a test at
   `src/test/java/net/snowflake/ingest/utils/Power10Test.java`.

5. **Add regression comments** — Where a test was ported from JDBC, add a comment:
   ```java
   // Ported from snowflake-jdbc: net.snowflake.client.XxxTest
   ```

### Per-class test mapping

| Replicated Class | JDBC Test to Port | Priority |
|---|---|---|
| `utils/Power10.java` | `client.jdbc.internal.snowflake.common.util.Power10Test` | High — used in timestamp and data validation |
| `utils/SFPair.java` | `client.util.SFPairTest` (if exists) | Low — trivial data class |
| `utils/Stopwatch.java` | `client.util.StopwatchTest` (if exists) | Low — trivial timing class |
| `utils/SnowflakeSQLException.java` | `client.jdbc.SnowflakeSQLExceptionTest` | Medium |
| `fileTransferAgent/StageInfo.java` | `client.jdbc.cloud.storage.StageInfoTest` | High — complex data class with JSON parsing |
| `fileTransferAgent/StorageObjectMetadata.java` | Covered by `IcebergCommonObjectMetadataTest` (existing) | Medium |
| `streaming/internal/RemoteStoreFileEncryptionMaterial.java` | `client.jdbc.internal.snowflake.common.core.RemoteStoreFileEncryptionMaterialTest` | Medium |
| `streaming/internal/SnowflakeFileTransferMetadataV1.java` | `client.jdbc.SnowflakeFileTransferMetadataV1Test` | High — central to upload path |
| `connection/TelemetryHttpSender.java` | `client.jdbc.telemetry.TelemetryTest` | High — verify batching and flush behaviour |
| `IcebergFileTransferAgent` (updated) | `InternalStageTest.java` already exists in this repo — extend it | High |

### Tests already in this repo that must be updated

The following existing test files import JDBC types directly and must have their imports
updated in lock-step with the corresponding production class migration:

| Test File | JDBC Imports to Replace |
|---|---|
| `streaming/internal/InternalStageTest.java` | `SnowflakeFileTransferAgent`, `SnowflakeFileTransferConfig`, `SnowflakeFileTransferMetadataV1`, `SnowflakeSQLException`, `StageInfo`, `OCSPMode`, `SFSessionProperty`, `HttpUtil` (JDBC's), `CLOUD_STORAGE_CREDENTIALS_EXPIRED` |
| `streaming/internal/SnowflakeStreamingIngestClientTest.java` | `SFSessionProperty` |
| `streaming/internal/SnowflakeStreamingIngestChannelTest.java` | `SFSessionProperty` |

---

## Risks and Considerations

- **Non-Iceberg upload path** — The non-Iceberg path uses `SnowflakeFileTransferAgent`
  which handles client-side encryption of files before upload. Replicating this correctly
  is critical for data integrity. Thoroughly review the encryption logic before and after.

- **Jackson version alignment** — The current code works around a Jackson version mismatch
  between ingest's Jackson and JDBC's shaded Jackson (SNOW-1493470). Removing JDBC will
  also resolve this workaround since we'll own the full JSON parsing.

- **Shade plugin** — JDBC is currently shaded into the final jar under
  `net.snowflake.ingest.internal.net.snowflake.client`. After removal the shade rules for
  this must also be removed.

- **Test coverage** — Integration tests (`IcebergIT`, `SimpleIngestIT`) exercise all
  three cloud providers. These must pass before the JDBC removal is complete.

- **IT JDBC connection dependency** — `TestUtils.java` loads `SnowflakeDriver` via
  reflection to create JDBC connections for querying Snowflake during ITs. Rather than
  removing `snowflake-jdbc-thin` entirely, Phase 5 demotes it to `test` scope so the
  driver remains available at IT runtime without being included in the shipped jar.

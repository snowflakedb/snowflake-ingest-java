# Plan: Remove `snowflake-jdbc-thin` Dependency

## Background

The SDK depends on `net.snowflake:snowflake-jdbc-thin:3.25.1` for several purposes.
The goal is to replicate all required classes/functionality directly in this repo and
eliminate the JDBC dependency entirely.

---

## Guiding Principle: Replication Only — No Functional Changes

**This project is a pure mechanical migration. There must be no functional changes.**

- Copy JDBC classes verbatim into this repo. Do not refactor, simplify, rename fields,
  change method signatures, or improve logic — even where improvements seem obvious.
- Do not change the runtime behavior of any existing feature. The before and after must
  be functionally identical from the perspective of callers.
- Do not fix bugs encountered in JDBC code during the migration. If a bug is found,
  file a separate ticket and replicate the buggy behavior as-is for now.
- Do not add new functionality, new configuration options, or new error handling paths.
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

**Naming rule:** Replicated classes keep the same name as in JDBC. Where a name
collides with an existing ingest class (e.g. `ErrorCode`), use package separation
— the replicated class goes into the `fileTransferAgent` package.

**PR size limit:** Each PR should be around 300 lines of production code changes
(new classes + import swaps). Test code is not counted toward this limit. A step
may be split across multiple PRs if needed to stay within this budget.

### Dependency Graph

```
Independent — can replace any time, no interaction with other JDBC classes:
  Power10              ← TimestampWrapper, DataValidationUtil
  SFPair               ← S3, Azure, GCS clients
  Stopwatch            ← S3, Azure, GCS clients
  SFSessionProperty    ← HttpUtil, Utils, S3, Azure clients, ClientInternal
  SqlState             ← S3, Azure, GCS, StorageClient interface
  ErrorCode (JDBC)     ← S3, Azure, GCS, StorageClient interface
  CLOUD_STORAGE_CREDENTIALS_EXPIRED ← S3, Azure clients
  StorageObjectMetadata ← interface; all storage clients, FileTransferAgent
  FileBackedOutputStream ← all storage clients, FileTransferAgent, StorageHelper
  HttpUtil (JDBC)       ← isSocksProxyDisabled(); S3, StorageClientFactory
  SFSSLConnectionSocketFactory ← S3 client only
  SnowflakeUtil methods ← S3, Azure, GCS, FileTransferAgent
  SnowflakeSQLException ← thrown by storage clients; caught as SQLException
  SnowflakeSQLLoggedException ← extends SnowflakeSQLException
  TelemetryClient/Util  ← TelemetryService only

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

### Step 1 — Simple utilities ✅

`Power10`, `SFPair`, `Stopwatch`. Swap imports.

**Files fully freed of JDBC imports:** `DataValidationUtil`, `TimestampWrapper`

---

### Step 2 — Enums, constants, proxy utils

- `SFSessionProperty` — enum with 9 property keys, swap all imports
- `SqlState` — SQL state constants (same package, no import needed)
- `ErrorCode` (in `fileTransferAgent` package) + `CLOUD_STORAGE_CREDENTIALS_EXPIRED`
- `OCSPMode` — enum, swap in `IcebergFileTransferAgent` (keep JDBC import in
  `InternalStage` — blocked by `SnowflakeFileTransferConfig`)
- `HttpUtil.isSocksProxyDisabled()` — add to ingest's HttpUtil, swap import
- Rename `generateProxyPropertiesForJDBC()` → `generateProxyProperties()`

**Files fully freed:** `Utils`, `HttpUtil`, `SnowflakeStreamingIngestClientInternal`

---

### Step 3 — Storage data types + utils

- `StorageObjectMetadata` — own interface (same package)
- `FileBackedOutputStream` — own class (same package)
- `SnowflakeUtil` → `StorageClientUtil` (`getRootCause`, `isBlank`,
  `createCaseInsensitiveMap`)

**Files fully freed:** `IcebergCommonObjectMetadata`, `IcebergS3ObjectMetadata`,
`StorageHelper`

---

### Step 4 — HTTP settings + SSL factory

- `HttpClientSettingsKey` — value class (may temporarily import JDBC's `OCSPMode`,
  or use ingest's `OCSPMode` if Step 2 is done first)
- `SFSSLConnectionSocketFactory` replacement
- `StorageClientUtil` additions: `convertProxyPropertiesToHttpClientKey`,
  `createDefaultExecutorService`

**JDBC imports removed:** `HttpClientSettingsKey`, `SFSSLConnectionSocketFactory`,
`SnowflakeUtil`, `HttpUtil` (JDBC)

---

### Step 5 — Exceptions

- `SnowflakeSQLException` / `SnowflakeSQLLoggedException` — own classes
- These never leak to public API (always caught and wrapped in `SFException`)
- JDBC's `SnowflakeFileTransferAgent` also throws JDBC's `SnowflakeSQLException`,
  but catch sites already use `catch (SQLException ...)` which catches both

**JDBC imports removed:** All `SnowflakeSQLException`/`SnowflakeSQLLoggedException`
imports from storage clients

---

### Step 6 — Telemetry (independent, can be done in parallel)

- Replace `TelemetryClient`/`TelemetryUtil` with own HTTP sender

**Files fully freed:** `TelemetryService`

---

### Step 7 — Replace SnowflakeFileTransferAgent (unblocks remaining types)

- Inline `throwJCEMissingError()` / `throwNoSpaceLeftError()` in storage clients
- Replace `getFileTransferMetadatas()` → own JSON parser in `InternalStage`
- Replace `uploadWithoutConnection()` → use `IcebergFileTransferAgent` path
- Remove `SnowflakeFileTransferConfig`

**JDBC imports removed:** `SnowflakeFileTransferAgent`, `SnowflakeFileTransferConfig`

---

### Step 8 — Replace blocked types (now unblocked after Step 7)

- `StageInfo` — own data class with `StageType` enum
- `RemoteStoreFileEncryptionMaterial` — simple data holder
- `SnowflakeFileTransferMetadataV1` — own data class
- Swap `OCSPMode` in `InternalStage` (no longer passed to JDBC)

**JDBC imports removed:** All remaining JDBC imports

---

### Step 9 — Remove JDBC Dependency

1. Change `snowflake-jdbc-thin` scope to `test` in `pom.xml` (`TestUtils.java` needs
   `SnowflakeDriver` for IT result verification)
2. Remove JDBC shade relocation rules from Maven Shade plugin
3. Remove `snowflake-jdbc-thin` from `public_pom.xml`
4. Run full test suite

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

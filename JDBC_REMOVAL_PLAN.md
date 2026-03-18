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
| `SFSSLConnectionSocketFactory` | `client.core` | `IcebergS3Client` | Medium — SSL socket factory |
| `HttpClientSettingsKey` | `client.core` | `IcebergFileTransferAgent`, `IcebergGCSClient` | Medium — key object for HTTP settings |
| `SnowflakeFileTransferAgent` | `client.jdbc` | `InternalStage` | **High** — parse metadata + upload |
| `SnowflakeFileTransferConfig` | `client.jdbc` | `InternalStage` | Medium — config builder |
| `SnowflakeFileTransferMetadataV1` | `client.jdbc` | `InternalStage`, `SnowflakeFileTransferMetadataWithAge` | **High** — rich metadata object |
| `SnowflakeSQLException` | `client.jdbc` | `InternalStage`, storage clients, `InternalStageManager` | Low — extend `SQLException` |
| `SnowflakeSQLLoggedException` | `client.jdbc` | Storage clients | Low — extend `SnowflakeSQLException` |
| `FileBackedOutputStream` | `client.jdbc` | `IcebergFileTransferAgent`, `StorageHelper`, storage clients | Medium — Guava-backed stream |
| `SnowflakeUtil` | `client.jdbc` | `IcebergFileTransferAgent`, `IcebergCommonObjectMetadata`, `IcebergAzureClient` | Medium — utility methods |
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

## Migration Plan

The work is split into 4 phases, ordered from least to most complex.

---

### Phase 1 — Replicate Simple Utilities and Enums

**Target package:** `net.snowflake.ingest.utils` (or sub-packages)

**Tasks:**

1. **`Power10`** — Create `net.snowflake.ingest.utils.Power10` with the same `intTable`,
   `sb16Table`, and `sb16Size` fields. Values are stable and can be copied verbatim.
   Update `TimestampWrapper` and `DataValidationUtil` imports.

2. **`SFPair<L, R>`** — Create `net.snowflake.ingest.utils.SFPair` as a simple generic
   pair class. Update S3, Azure, GCS clients.

3. **`Stopwatch`** — Create `net.snowflake.ingest.utils.Stopwatch` wrapping
   `System.nanoTime()`. Update S3, Azure, GCS clients.

4. **`OCSPMode`** — Create a minimal enum with `FAIL_OPEN` and `FAIL_CLOSED`. Update
   `InternalStage` and `IcebergFileTransferAgent`.

5. **`SqlState`** — Create a constants class with the SQL state strings actually used.
   Update storage clients.

6. **`ErrorCode`** (JDBC's) — Replicate only the error codes used in storage clients.
   Note: ingest already has its own `net.snowflake.ingest.utils.ErrorCode`; use a
   separate class name if needed (e.g., `JdbcErrorCode`).

7. **`SnowflakeSQLException`** — Create `net.snowflake.ingest.utils.SnowflakeSQLException`
   extending `java.sql.SQLException`. Replicate the constructors used.

8. **`SnowflakeSQLLoggedException`** — Extend the above. Update all storage clients.

9. **Inline `SnowflakeDateTimeFormat` constants** — The three epoch-limit constants in
   `DataValidationUtil` are documented as aligned with JDBC values. Inline them with
   a comment explaining the alignment. No import needed.

---

### Phase 2 — Replicate Property Keys and Proxy Infrastructure

**Tasks:**

1. **`SFSessionProperty`** — Create `net.snowflake.ingest.utils.SFSessionProperty` (or
   `ProxyProperties`) as an enum/constants class exposing the string property keys that
   are used:
   - `PRIVATE_KEY`, `USE_PROXY`, `PROXY_HOST`, `PROXY_PORT`, `PROXY_USER`,
     `PROXY_PASSWORD`, `NON_PROXY_HOSTS`, `PROXY_PROTOCOL`, `ALLOW_UNDERSCORES_IN_HOST`

   The key string values must match JDBC's values exactly (e.g. `"privateKey"`,
   `"useProxy"`, etc.) since they are also consumed by JDBC in the non-Iceberg upload
   path. Once JDBC is fully removed the key names just need to be internally consistent.

2. **`HttpUtil.isSocksProxyDisabled()`** — Used from JDBC's `HttpUtil` by
   `IcebergStorageClientFactory` and `IcebergS3Client`. Add this static method to the
   ingest's own `net.snowflake.ingest.utils.HttpUtil`. The implementation checks the
   `socksNonProxyHosts` / socks proxy system property.

3. **Rename `generateProxyPropertiesForJDBC()`** — Once `SFSessionProperty` is replaced,
   rename this method to `generateProxyProperties()` since it no longer generates
   JDBC-specific keys.

---

### Phase 3 — Replicate Data and Metadata Classes

**Tasks:**

1. **`FileBackedOutputStream`** — JDBC's `FileBackedOutputStream` is itself based on
   Guava's `com.google.common.io.FileBackedOutputStream`. Replace usages with Guava's
   version directly (already a dependency). Update `IcebergFileTransferAgent`,
   `StorageHelper`, and all storage clients.

2. **`SnowflakeUtil` methods used:**
   - `createCaseInsensitiveMap(Map)` — Replace with `new TreeMap<>(String.CASE_INSENSITIVE_ORDER);`
     inline in `IcebergCommonObjectMetadata`.
   - `convertProxyPropertiesToHttpClientKey(OCSPMode, Properties)` — Returns an
     `HttpClientSettingsKey`. Once `HttpClientSettingsKey` is replicated (see below),
     implement this conversion in ingest's own utility.

3. **`HttpClientSettingsKey`** — Used by `IcebergFileTransferAgent` (passed to GCS client)
   and `IcebergGCSClient`. Create a simple value class wrapping `OCSPMode` + proxy
   properties. The GCS client uses it to configure the HTTP connection.

4. **`SFSSLConnectionSocketFactory`** — Used in `IcebergS3Client` to set up an SSL socket
   factory. Replace with Apache HttpClient's `SSLConnectionSocketFactory` (already
   used in ingest's own `HttpUtil`).

5. **`RemoteStoreFileEncryptionMaterial`** — Simple data holder with `queryStageMasterKey`,
   `queryId`, and `smkId` fields. Create
   `net.snowflake.ingest.streaming.internal.RemoteStoreFileEncryptionMaterial`.
   Update `InternalStage`.

6. **`StorageObjectMetadata` interface** — Define own interface in
   `net.snowflake.ingest.streaming.internal.fileTransferAgent`. `IcebergCommonObjectMetadata`
   and `IcebergS3ObjectMetadata` already implement the interface; just change the
   `implements` clause.

7. **`StageInfo`** — This is the most significant data class. Create own
   `net.snowflake.ingest.streaming.internal.fileTransferAgent.StageInfo` with:
   - `StageType` enum: `S3`, `AZURE`, `GCS`, `LOCAL_FS`
   - Fields: `stageType`, `location`, `credentials`, `region`, `endPoint`,
     `isClientSideEncrypted`, `useS3RegionalUrl`, `presignedUrl`, `proxyProperties`
   - Parse from JDBC-format JSON (the `stageInfo` node in the configure response)

   This impacts `InternalStage.createFileTransferMetadataWithAge()`,
   `InternalStage.parseFileLocationInfo()`, and all storage clients.

---

### Phase 4 — Replace `SnowflakeFileTransferAgent`, `SnowflakeFileTransferMetadataV1`, and Telemetry

**Tasks:**

1. **`SnowflakeFileTransferMetadataV1`** — Own data class holding:
   - `presignedUrl`, `presignedUrlFileName`
   - `commandType` (UPLOAD/DOWNLOAD enum)
   - `stageInfo` (own `StageInfo`)
   - `encryptionMaterial` (own `RemoteStoreFileEncryptionMaterial`)

   Update `InternalStage`, `SnowflakeFileTransferMetadataWithAge`,
   `IcebergFileTransferAgent`, and tests.

2. **`SnowflakeFileTransferAgent.getFileTransferMetadatas(JsonNode)`** — Used to parse
   the stage configure response into a list of `SnowflakeFileTransferMetadataV1`. Replace
   with own JSON parser in `InternalStage.parseFileLocationInfo()` that:
   - Reads `stageInfo` from the response
   - Constructs own `StageInfo` and `SnowflakeFileTransferMetadataV1`
   - Eliminates the Jackson version workaround (SNOW-1493470) since we own both sides

3. **`SnowflakeFileTransferAgent.uploadWithoutConnection(config)`** — Used in the
   **non-Iceberg** upload path only. Two options:

   **Option A (recommended):** Migrate the non-Iceberg path to use the same
   `IcebergFileTransferAgent` infrastructure (S3/Azure/GCS clients already exist).
   The non-Iceberg path still needs file encryption, which the Iceberg path currently
   skips — this would require adding client-side encryption support to the Iceberg
   storage clients.

   **Option B:** Implement own `SnowflakeFileTransferAgent`-equivalent class covering
   the subset of functionality used: parse stage info, apply encryption, upload to
   S3/Azure/GCS. More work but a clean cut.

4. **`SnowflakeFileTransferConfig`** — Used only with `SnowflakeFileTransferAgent`.
   Eliminated once Phase 4.3 is complete.

5. **`TelemetryClient` / `TelemetryUtil`** — `TelemetryService` uses:
   - `TelemetryClient.createSessionlessTelemetry(httpClient, url)` — creates a client
     that POSTs telemetry to `<url>/telemetry/send`.
   - `telemetry.addLogToBatch(TelemetryData)` — batches a log entry.
   - `TelemetryUtil.buildJobData(ObjectNode)` — wraps a JSON object into a `TelemetryData`.
   - `telemetry.close()` — flushes and closes.
   - `telemetry.refreshToken(token)` — updates the JWT for the HTTP client.

   Replace by implementing a simple `TelemetryService` that directly POSTs JSON to the
   telemetry endpoint using the existing `CloseableHttpClient`. No external dependency
   needed.

---

### Phase 5 — Remove JDBC Dependency

1. Remove `snowflake-jdbc-thin` from `pom.xml` (dependency + version property).
2. Remove the JDBC shade relocation rules from the Maven Shade plugin configuration.
3. Update `public_pom.xml` if it also references JDBC.
4. Run the full test suite and fix any remaining compilation or runtime failures.

---

## Files Affected Summary

| File | Change |
|---|---|
| `pom.xml` | Remove `snowflake-jdbc-thin` dependency and shade rule |
| `utils/HttpUtil.java` | Remove `SFSessionProperty` import; add `isSocksProxyDisabled()` |
| `utils/Utils.java` | Replace `SFSessionProperty` with own class |
| `connection/TelemetryService.java` | Replace `TelemetryClient`/`TelemetryUtil` with own HTTP sender |
| `streaming/internal/InternalStage.java` | Replace JDBC agent + metadata with own classes |
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
| `fileTransferAgent/StorageErrorCode.java` | Error codes for storage clients |
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
| `streaming/internal/InternalStageTest.java` | `SnowflakeFileTransferAgent`, `SnowflakeFileTransferConfig`, `SnowflakeFileTransferMetadataV1`, `SnowflakeSQLException`, `StageInfo`, `OCSPMode`, `SFSessionProperty` |
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

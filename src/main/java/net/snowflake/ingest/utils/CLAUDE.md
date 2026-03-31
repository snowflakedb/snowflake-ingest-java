# utils/

Shared utilities used across the entire SDK (both `connection/` and `streaming/`).

## Key Classes

**Errors:**
- `ErrorCode` — enum of all SDK error codes with message templates.
- `SFException` — unchecked exception carrying an `ErrorCode`. All SDK-thrown exceptions are `SFException`.

**HTTP:**
- `HttpUtil` — builds `CloseableHttpClient` with pooled connections, retry handler, service-unavailable retry strategy, SSL context (BouncyCastle), and optional proxy (`HttpHost` + credentials). `generateProxyPropertiesForJDBC()` packs proxy settings into a `Properties` map using `SFSessionProperty` key names for the JDBC file transfer agent. `MAX_CONNECTIONS` = 10 per route, 100 total.

**Auth / key material:**
- `Utils` — RSA private key loading from PKCS8 PEM (unencrypted and encrypted via BouncyCastle), memory inspection helpers (`getVirtualMemoryUsage`, `getDirectMemoryUsage`), stack trace formatting.
- `SnowflakeURL` — parses and normalizes Snowflake account URLs (`https://<account>.snowflakecomputing.com`).

**Parameters:**
- `Constants` — all SDK-wide constants and default values for tunable parameters (flush interval, blob size limits, retry counts, telemetry intervals, etc.).
- `ParameterProvider` — resolves runtime parameters, merging client-supplied `Properties` with server-returned overrides from the `configure` response.

**Data structures:**
- `Pair<L, R>` — generic immutable pair/tuple. Used throughout streaming internals.
- `RingBuffer<T>` — fixed-capacity circular buffer with `O(1)` add/get. Used for sliding-window deduplication.

**Misc:**
- `Logging` — thin SLF4J wrapper that prepends a class-name prefix to all messages.
- `SubColumnFinder` — traverses Parquet nested-field paths (dot-separated) to locate sub-columns in the schema for repeated/nested types.
- `ThreadFactoryUtil` — creates named daemon `ThreadFactory` instances via Apache Commons `BasicThreadFactory`.
- `StagedFileWrapper` — wraps a staged file name and optional stream for the legacy file ingest path.
- `IcebergDataTypeParser` (in `streaming/internal/`) — note: Iceberg-specific parsing lives in `internal/`, not here.

## JDBC Dependencies (Removal In Progress)

- `HttpUtil` imports `SFSessionProperty` from JDBC for proxy property key names (Phase 2 of JDBC removal).
- `Utils` stores the private key under `SFSessionProperty.PRIVATE_KEY` in the properties map (Phase 2).

## Gotchas

- `HttpUtil` creates a single shared `CloseableHttpClient` instance (singleton pattern via `synchronized`). Callers must not close it.
- `Utils.createProperties()` encodes the RSA `PrivateKey` object directly into a `Properties` map entry — this is a JDBC convention that will be cleaned up during JDBC removal.
- BouncyCastle (`BC` or `BCFIPS`) is registered as a JVM security provider in `Utils` static initializer — this runs once per JVM, not once per client instance.

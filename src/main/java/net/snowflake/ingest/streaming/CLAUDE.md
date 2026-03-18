# streaming/

Public API for the Snowflake Streaming Ingest SDK. All classes here are customer-facing.

## Public API Surface

**Entry point:**
`SnowflakeStreamingIngestClientFactory.builder(name, props).build()` → `SnowflakeStreamingIngestClient`
→ `openChannel(OpenChannelRequest)` → `SnowflakeStreamingIngestChannel`
→ `insertRows(rows, endOffsetToken)` / `insertRow(row, endOffsetToken)`

**Interfaces:**
- `SnowflakeStreamingIngestClient` — one client per Snowflake account. Thread-safe. Manages channels, drives flushes, exposes `setRefreshToken()` for OAuth renewal.
- `SnowflakeStreamingIngestChannel` — one channel per table partition. Thread-safe. `insertRows` validates and buffers rows; `close(dropOnClose)` flushes and optionally drops the server-side channel.

**Request/response types:**
- `OpenChannelRequest` — builder with `dbName`, `schemaName`, `tableName`, `channelName`, `onErrorOption`, `offsetToken`, `encryptionKeyId`.
- `DropChannelRequest` — builder with table + channel coordinates.
- `InsertValidationResponse` — list of per-row `InsertError` objects with column name, exception, row index, and extra columns.
- `OffsetTokenVerificationFunction` — functional interface for `waitForOffset` callbacks.

**On-error options (`OpenChannelRequest.OnErrorOption`):**
- `CONTINUE` — skip invalid rows, collect errors in `InsertValidationResponse`.
- `ABORT` — throw `SFException` on first validation error.
- `SKIP_BATCH` — discard the entire batch if any row is invalid.

## Implementations

All interfaces are implemented in `streaming/internal/`. The public package contains only interfaces, enums, and request/response value objects. Do not add internal logic here.

## Examples

`streaming/example/` — standalone usage examples (not shipped in the SDK jar).

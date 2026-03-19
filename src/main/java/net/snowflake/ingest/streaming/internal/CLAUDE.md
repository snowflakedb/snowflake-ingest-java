# streaming/internal/

Core internal implementation of the Streaming Ingest SDK. Package-private except where annotated `@VisibleForTesting`.

## Architecture: Flush Pipeline

```
insertRows()
  └─ AbstractRowBuffer / ParquetRowBuffer     (validate + buffer in-memory)
       └─ FlushService (background thread)
            ├─ BlobBuilder                    (serialize channel data → Parquet blob)
            ├─ InternalStage / IStorage       (upload blob to cloud stage)
            └─ RegisterService                (register blob with Snowflake)
```

## Key Classes

**Client and channel:**
- `SnowflakeStreamingIngestClientInternal` — owns `ChannelCache`, `FlushService`, `RegisterService`, `SnowflakeServiceClient`, and Codahale metric registries (JMX + Slf4j reporters). Entry point for all channel lifecycle calls.
- `SnowflakeStreamingIngestChannelInternal` — holds a `RowBuffer`, offset token state, and channel metadata. `insertRows` delegates to `IngestionStrategy`.
- `SnowflakeStreamingIngestChannelFactory` — constructs channel instances.

**Buffering:**
- `AbstractRowBuffer` / `ParquetRowBuffer` — validate incoming rows against column schema, convert to typed Parquet values, track stats (`RowBufferStats`). Thread-safe via read/write lock.
- `IngestionStrategy` — two implementations: abort-on-error and continue-on-error (SKIP_BATCH).
- `ChannelData` / `ParquetChunkData` — snapshot of buffered data handed off to `FlushService`.

**Flushing:**
- `FlushService` — periodic background flush (configurable interval) plus on-demand flush. Calls `BlobBuilder`, then `IStorageManager.upload()`, then `RegisterService.registerBlobs()`. Tracks in-flight blobs per channel.
- `BlobBuilder` — serializes a list of `ChannelData` into a Parquet file (`BlobMetadata` + byte array). Uses `ParquetFlusher`.
- `ParquetFlusher` — writes Parquet rows using the Apache Parquet library.

**Stage upload:**
- `IStorage` / `IStorageManager` — interfaces for stage upload.
- `InternalStage` — non-Iceberg upload path: uses JDBC's `SnowflakeFileTransferAgent` for encryption + upload. Has a secondary `parseConfigureResponseMapper` to work around Jackson version mismatch (SNOW-1493470).
- `InternalStageManager` — manages `InternalStage` instances per table.
- `SubscopedTokenExternalVolumeManager` — manages external volume (Iceberg) stage uploads using `IcebergFileTransferAgent` and presigned URLs.

**Snowflake API client:**
- `SnowflakeServiceClient` — wraps all Snowflake streaming API calls: `configure`, `open_channel`, `drop_channel`, `register_blob`, `channel_status`, `refresh_table_info`, `generate_presigned_urls`. Uses `RequestBuilder` for auth.

**Schema / type system:**
- `ParquetTypeGenerator` — maps Snowflake column metadata to Parquet `Type` definitions.
- `DataValidationUtil` — validates and coerces row values to column types (numeric ranges, timestamps, semi-structured, etc.). Uses JDBC's `Power10` for precision tables.
- `TimestampWrapper` — parses and normalizes timestamp strings/objects. Uses JDBC's `Power10`.
- `IcebergDataTypeParser` — parses Iceberg schema JSON into `ColumnMetadata`.
- `IcebergParquetValueParser` / `SnowflakeParquetValueParser` — per-type value → Parquet conversion.

**Metrics:**
- Codahale `MetricRegistry` shared across clients via `SharedMetricRegistries`.
- JMX reporter under `snowpipe_streaming.*`.
- `TelemetryService` (in `connection/`) periodically reads metrics and sends to Snowflake.

## Gotchas

- `InternalStage` maintains **two** `ObjectMapper` instances because JDBC's `SnowflakeFileTransferAgent.getFileTransferMetadatas()` requires a different Jackson version than the rest of the SDK (workaround for SNOW-1493470). This will be eliminated by the JDBC removal project.
- Non-Iceberg path goes through `InternalStage` → JDBC `SnowflakeFileTransferAgent` (handles client-side encryption). Iceberg path goes through `SubscopedTokenExternalVolumeManager` → `IcebergFileTransferAgent` (no client-side encryption).
- `FlushService` catches all exceptions per-channel to avoid one bad channel poisoning the flush loop.
- `ParameterProvider` / `InternalParameterProvider` allow server-side parameter overrides returned in the `configure` response.

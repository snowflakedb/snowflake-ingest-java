# src/test/java/net/snowflake/ingest/streaming/internal/

Unit tests and one broad integration test for the core streaming internals.

## Unit Tests (no live Snowflake)

- `BlobBuilderTest.java` — serializes channel data into a Parquet blob, verifies metadata and byte layout.
- `BinaryStringUtilsTest.java` — hex/binary string conversion helpers.
- `ChannelCacheTest.java` — channel map eviction and lookup.
- `ChannelDataTest.java` — snapshot creation from row buffers.
- `DataValidationUtilTest.java` / `DataValidationUtilBenchmarkTest.java` — validates/coerces every column type; benchmark for hot-path throughput.
- `FileColumnPropertiesTest.java` — Parquet column properties encoding.
- `FlushServiceTest.java` — flush scheduling, blob splitting, and error-isolation (one bad channel must not stop others).
- `IcebergDataTypeParserTest.java` — parses Iceberg schema JSON to `ColumnMetadata`.
- `IcebergParquetValueParserTest.java` / `SnowflakeParquetValueParserTest.java` — per-type value → Parquet conversion (covers edge cases: nulls, overflow, timezone handling).
- `InternalStageTest.java` — stage credential parsing, presigned URL refresh, upload delegation. Mocks JDBC file transfer agent.
- `MockSnowflakeServiceClient.java` — in-memory stub for `SnowflakeServiceClient`, used across many tests.
- `OAuthBasicTest.java` — OAuth token refresh and header injection without a live server.
- `ParameterProviderTest.java` — server-side parameter override merging.
- `ParquetTypeGeneratorTest.java` — Snowflake column metadata → Parquet type mapping.
- `RegisterServiceTest.java` — blob registration retry logic and error classification.
- `RowBufferTest.java` / `RowBufferStatsTest.java` — row buffering, stats accumulation, thread safety.
- `SnowflakeServiceClientTest.java` — HTTP request serialization for each API endpoint.
- `SnowflakeStreamingIngestClientTest.java` / `SnowflakeStreamingIngestChannelTest.java` — client/channel lifecycle, parameter wiring.
- `SnowflakeStreamingIngestClientTmkRefreshTest.java` — table master key refresh flow.
- `SnowflakeURLTest.java` — account URL normalization.
- `StreamingIngestResponseCodeTest.java` — response code enum completeness.
- `StreamingIngestUtilsTest.java` — retry/sleep utilities.
- `SubscopedTokenExternalVolumeManagerTest.java` — external volume manager token scoping.
- `VolumeEncryptionModeTest.java` — encryption mode enum.

## Integration Test (requires `profile.json`)

- `StreamingIngestIT.java` — broad end-to-end streaming IT. Parameterized across compression
  algorithms. Covers: single/multi-channel inserts, offset token tracking, `waitForOffset`,
  schema evolution, blob registration, and concurrent inserts. Not tagged `@Category(IcebergIT.class)` — runs in the standard `build` job.

## Helpers

- `StubChunkData.java` — minimal `ParquetChunkData` stub for builder tests.
- `ParquetValueParserAssertionBuilder.java` — fluent assertion DSL for Parquet value parser tests.
- `ColumnMetadataBuilder.java` — builder for `ColumnMetadata` test fixtures.

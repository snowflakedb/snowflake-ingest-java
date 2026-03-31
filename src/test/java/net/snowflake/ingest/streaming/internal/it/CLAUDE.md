# it/

Miscellaneous integration tests for Streaming Ingest scenarios beyond basic data types.
All tests require a live Snowflake connection (`profile.json`).

## Files

| File | Category | What it tests |
|---|---|---|
| `StreamingIngestBigFilesIT.java` | — | Inserts large volumes of data to verify multi-blob splits, blob size limits, and register throughput |
| `ManyTablesIT.java` | — | Opens channels across many tables concurrently; validates per-table isolation |
| `DynamicTablePartitionIT.java` | — | Streaming ingest into dynamic table partitions |
| `FdnColumnNamesIT.java` | — | Fully-qualified/double-quoted column name handling |
| `ColumnNamesITBase.java` | — | Shared base for column-name IT tests |
| `IcebergOpenChannelIT.java` | `IcebergIT` | Open/close channel lifecycle for Iceberg tables; parameterized across `COMPATIBLE`/`OPTIMIZED` serialization policies. Currently `@Ignore` pending feature flag rollout |
| `IcebergColumnNamesIT.java` | `IcebergIT` | Column name edge cases for Iceberg tables |
| `IcebergSchemaEvolutionIT.java` | `IcebergIT` | Adds/drops/renames columns on a live Iceberg table and verifies the SDK adapts |
| `IcebergBigFilesIT.java` | `IcebergIT` | Large-file upload path for Iceberg (presigned URL flow) |
| `IcebergKmsEncryptionIT.java` | `IcebergIT` | SSE-KMS encrypted external volumes; requires KMS-enabled Snowflake account |
| `SubscopedTokenRefreshIT.java` | `IcebergIT` | Verifies subscopedToken refresh mid-session for external volume uploads |

## Iceberg vs Standard

All `Iceberg*` tests are tagged `@Category(IcebergIT.class)` and run only in the `build-iceberg`
CI job (`-Dfailsafe.groups="net.snowflake.ingest.IcebergIT"`). Non-Iceberg tests run in the
standard `build` job.

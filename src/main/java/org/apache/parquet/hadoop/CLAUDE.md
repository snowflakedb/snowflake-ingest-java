# org/apache/parquet/hadoop/

Snowflake-specific Parquet reader and writer. Lives under the `org.apache.parquet.hadoop`
package — **not** `net.snowflake` — because both classes depend on Parquet internals
(`InternalParquetRecordReader`, `InternalParquetRecordWriter`, `CodecFactory`) that are
package-private in the Apache Parquet library.

## Files

- `BdecParquetReader` — reads BDEC (Snowflake's Parquet-based blob format) from a raw
  `byte[]`. Wraps `InternalParquetRecordReader` with a custom `ReadSupport` and `InputFile`
  that reads from an in-memory `ByteArrayInputStream`. Used in tests to verify blobs built
  by `BlobBuilder`.

- `SnowflakeParquetWriter` — writes Parquet blobs for both FDN (non-Iceberg, BDEC format)
  and Iceberg tables. Wraps `InternalParquetRecordWriter` and `CodecFactory`. Supports
  configurable codec (ZSTD, SNAPPY, etc.), dictionary encoding, Parquet writer version, and
  an optional row-group limit (exceeding the limit throws `SFException`). Used by
  `ParquetFlusher`.

## Gotchas

- Any changes here require awareness of Parquet's package-private API surface. Upgrading
  the `parquet-*` dependency may break compilation if internal class signatures change.
- Do not move these classes to the `net.snowflake` package — they will lose access to the
  package-private Parquet internals they depend on.

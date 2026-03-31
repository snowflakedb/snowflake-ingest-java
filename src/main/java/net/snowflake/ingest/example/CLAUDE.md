# example/

Runnable examples for the legacy Snowpipe file-based ingest API. Not part of the shipped SDK jar — for developer reference only.

## Files

- `IngestExampleHelper.java` — shared helper: generates RSA key pairs, writes CSV files to disk, creates JDBC connections, and runs DDL statements (CREATE PIPE, CREATE STAGE, etc.). Used by both example classes.

- `SnowflakeIngestBasicExample.java` — demonstrates the legacy `SimpleIngestManager` API: stage a CSV file, call `insertFile()`, and poll `getHistory()` until the file is ingested.

- `SnowflakeStreamingIngestExample.java` — demonstrates the Streaming Ingest API: build a `SnowflakeStreamingIngestClient`, open a channel, call `insertRow()`, and wait for the offset to commit.

## Gotchas

- These examples use hardcoded connection properties and are meant to be run manually against a real Snowflake account. They are not executed in CI.
- `IngestExampleHelper` uses the full JDBC driver (not JDBC-thin) for setup queries — it is not subject to the JDBC removal project.

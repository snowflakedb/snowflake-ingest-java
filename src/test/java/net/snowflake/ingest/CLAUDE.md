# src/test/java/net/snowflake/ingest/

Root test package. Contains shared test infrastructure and top-level integration tests.

## Running Tests

```bash
# Unit tests only (no live Snowflake required)
mvn test

# All integration tests (requires profile.json)
mvn verify -DghActionsIT

# Iceberg ITs only
mvn verify -DghActionsIT -Dfailsafe.groups="net.snowflake.ingest.IcebergIT"

# Exclude Iceberg ITs
mvn verify -DghActionsIT -Dfailsafe.excludedGroups="net.snowflake.ingest.IcebergIT"
```

Integration tests require `profile.json` in the repo root (see `profile.json.example` and
`profile_streaming.json.example`). The file holds Snowflake account URL, user, role,
private key, warehouse, database, and schema.

## Key Files

- `TestUtils.java` — shared IT helper used by virtually every integration test. Reads
  `profile.json`, creates JDBC connections (`getConnection()`), builds
  `SnowflakeStreamingIngestClient` instances with configurable parameters (`setUp()`),
  and provides `waitForOffset()` / `verifyOffset()` polling helpers.

- `IcebergIT.java` — empty marker interface. Tests tagged `@Category(IcebergIT.class)`
  are Iceberg-specific and run only when `failsafe.groups=net.snowflake.ingest.IcebergIT`.

- `SimpleIngestIT.java` — integration test for the legacy Snowpipe file-based ingest API
  (`SimpleIngestManager`). Exercises `insertFile`, history polling, and User-Agent headers.
  Uses `profile.json` for credentials.

## Test Categories

| Annotation | Meaning | Run by default in CI? |
|---|---|---|
| `@Category(IcebergIT.class)` | Requires Iceberg-enabled table/account | Only in `build-iceberg` job |
| *(none)* | Standard streaming or unit test | Yes, in `build` job |
| `@Ignore` | Disabled (usually pending feature flag rollout) | Never |

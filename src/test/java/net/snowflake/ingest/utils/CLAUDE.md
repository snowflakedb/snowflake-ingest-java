# src/test/java/net/snowflake/ingest/utils/

Unit tests for the `utils/` package. No live Snowflake connection required.

## Files

- `HttpUtilTest.java` — tests `HttpUtil` proxy configuration: verifies that proxy host/port/user/password
  are correctly injected into the `CloseableHttpClient`, and that `generateProxyPropertiesForJDBC()`
  produces the expected `Properties` map with the correct `SFSessionProperty` key names.

- `SubColumnFinderTest.java` — tests `SubColumnFinder` path resolution for nested Parquet
  fields: dot-separated column paths, repeated fields, and missing-field error cases.

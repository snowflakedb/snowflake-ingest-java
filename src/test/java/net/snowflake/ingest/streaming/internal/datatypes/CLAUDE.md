# datatypes/

Integration tests for every Snowflake and Iceberg column type. All tests require a live
Snowflake connection (`profile.json`). Each test inserts rows via the Streaming Ingest SDK
and reads them back via JDBC to verify round-trip correctness.

## Base Class

`AbstractDataTypeTest` — common fixture for all data type ITs. Sets up a database/schema,
opens a streaming channel, inserts values, flushes, then queries via JDBC and compares.
Supports comparing SDK-inserted values against JDBC-inserted reference values side-by-side.

## Test Files

| File | Column types covered |
|---|---|
| `NumericTypesIT.java` | `NUMBER`, `DECIMAL`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE` |
| `StringsIT.java` | `VARCHAR`, `CHAR`, `TEXT`, `BINARY`, `VARBINARY` |
| `DateTimeIT.java` | `DATE`, `TIME`, `TIMESTAMP_LTZ`, `TIMESTAMP_NTZ`, `TIMESTAMP_TZ` |
| `LogicalTypesIT.java` | `BOOLEAN`, `VARIANT`, `OBJECT`, `ARRAY` |
| `NullIT.java` | `NULL` values across all types |
| `BinaryIT.java` | Binary encoding edge cases |
| `SemiStructuredIT.java` | `VARIANT`, nested JSON, arrays-of-objects |
| `IcebergNumericTypesIT.java` | Iceberg `int`, `long`, `float`, `double`, `decimal` |
| `IcebergStringIT.java` | Iceberg `string`, `binary`, `fixed`, `uuid` |
| `IcebergDateTimeIT.java` | Iceberg `date`, `time`, `timestamp`, `timestamptz` |
| `IcebergLogicalTypesIT.java` | Iceberg `boolean`, `list`, `map`, `struct` |
| `IcebergStructuredIT.java` | Iceberg nested/repeated fields, struct-in-struct |

Iceberg tests are tagged `@Category(IcebergIT.class)` and only run in the `build-iceberg` CI job.

## Gotchas

- `Provider.java` — JUnit `@Parameters` provider shared across parametrized data type tests.
- The `AbstractDataTypeTest` fixture creates a fresh database per test class and drops it in `@After` — do not run multiple IT classes in the same JVM concurrently without isolation.

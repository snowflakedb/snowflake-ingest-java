# CLAUDE.md

## Build & Test

- Build: `mvn compile`
- Run all tests: `mvn test`
- Run a single test: `mvn test -Dtest=ClassName`
- Run specific test method: `mvn test -Dtest=ClassName#methodName`

## Required Steps Before Committing

- **Always run `./format.sh`** before committing. This sorts pom.xml and formats all Java sources with google-java-format. Commits with unformatted code will fail CI.

## Code Style

- Java sources are formatted with google-java-format 1.20.0
- Imports are sorted alphabetically (enforced by the formatter)
- pom.xml is sorted by sortpom-maven-plugin

## Project Structure

- `src/main/java/net/snowflake/ingest/` — main source
  - `utils/` — shared utilities (ErrorCode, HttpUtil, SFException, etc.)
  - `connection/` — HTTP connection and telemetry
  - `streaming/internal/` — streaming ingest internals
  - `streaming/internal/fileTransferAgent/` — cloud storage upload (S3, Azure, GCS)
- `src/test/java/net/snowflake/ingest/` — tests mirror the main structure

## JDBC Removal Project

When replicating classes from `snowflake-jdbc`:
- Copy verbatim — do not refactor, rename fields, or change logic
- Add a header comment with the source URL: `Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/...`
- Include a Replication Verification Diff Report in the PR description
- Port corresponding unit tests
- For classes from `snowflake-common`, prefer adding a direct dependency over copying

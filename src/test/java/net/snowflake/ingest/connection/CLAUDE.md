# src/test/java/net/snowflake/ingest/connection/

Unit tests for the `connection/` package. No live Snowflake connection required.

## Files

- `SecurityManagerTest.java` — tests `SecurityManager` and `JWTManager`: JWT generation with
  a fresh RSA key pair, token expiry/refresh timing, and invalid-parameter handling.

- `TelemetryServiceTest.java` — tests `TelemetryService` metric reporting: latency histograms,
  throughput meters, CPU/memory snapshots, and rate-limiter throttling. Uses a mock
  `TelemetryClient` (no real HTTP calls).

- `UserAgentTest.java` — tests `RequestBuilder` User-Agent header construction: verifies the
  SDK name, version, OS info, and any user-supplied suffix are assembled correctly.

- `MockOAuthClient.java` — test double for `OAuthClient`. Returns a configurable token
  without making real HTTP calls. Used by OAuth-related tests in `streaming/internal/`.

# connection/

HTTP authentication, request building, and telemetry for the Snowflake Ingest SDK.

## Key Classes

**Auth:**
- `SecurityManager` — decides between JWT and OAuth; refreshes tokens in the background.
- `JWTManager` — signs JWTs with an RSA private key. Token lifetime is 59 minutes; refreshed at 54 minutes.
- `OAuthManager` / `OAuthClient` / `OAuthCredential` — OAuth2 client-credentials and refresh-token flows. `OAuthClient` POSTs to the token endpoint; `OAuthManager` drives background renewal.

**Request building:**
- `RequestBuilder` — assembles `HttpUriRequest` objects (GET/POST) with `Authorization`, `User-Agent`, and `X-Snowflake-Authorization-Token-Type` headers. One instance per client.
- `ServiceResponseHandler` — deserializes JSON responses and maps HTTP errors to `IngestResponseException`.
- `IngestResponse` / `IngestResponseException` — response envelope and error type.

**Telemetry:**
- `TelemetryService` — batches SDK metrics (latency, throughput, CPU/memory, failures) and sends them to Snowflake via JDBC's `TelemetryClient`. Uses a Guava `RateLimiter` to cap telemetry POSTs. Metrics collected via Codahale.

**Legacy file ingest:**
- `HistoryResponse` / `HistoryRangeResponse` — response types for the legacy Snowpipe file ingest history API (used by `SimpleIngestManager`).
- `IngestStatus` — per-file status enum in history responses.

## Gotchas

- `TelemetryService` depends on JDBC's `TelemetryClient` and `TelemetryUtil` — these are targets of the JDBC removal project (Phase 4).
- `RequestBuilder` carries a reference to `TelemetryService` to report auth-level metrics.
- Token refresh is best-effort; a failed refresh logs a warning but does not immediately fail inflight requests.

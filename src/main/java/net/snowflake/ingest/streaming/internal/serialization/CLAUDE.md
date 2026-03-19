# serialization/

Custom Jackson serializers registered on the SDK's `ObjectMapper` instances.

## Classes

- `ByteArraySerializer` — serializes `byte[]` as a JSON array of signed integers (e.g. `[1, -2, 3]`) instead of Jackson's default base64 string. Used for binary column payloads in the register-blob request.
- `DuplicateKeyValidatingSerializer` / `DuplicateKeyValidatedObject` — wrapper that detects duplicate keys in a `Map<String, Object>` row before serialization. Throws `SFException(DUPLICATE_COLUMN_NAME)` on collision. Used when `insertRow` / `insertRows` is called.
- `ZonedDateTimeSerializer` — serializes `ZonedDateTime` as an ISO-8601 string with offset (e.g. `2024-01-15T10:30:00+05:30`). Registered on the mapper used for Parquet metadata blobs.

## Gotchas

- These serializers are registered globally on the shared `ObjectMapper` in `BlobBuilder` / `SnowflakeServiceClient`. Do not register them on the configure-response mapper in `InternalStage` — that mapper must stay vanilla to satisfy the JDBC Jackson version workaround (SNOW-1493470).

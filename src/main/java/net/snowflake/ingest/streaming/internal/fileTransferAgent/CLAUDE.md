# fileTransferAgent/

Iceberg cloud storage upload infrastructure. Used by `SubscopedTokenExternalVolumeManager` for the Iceberg (external volume) upload path. Not used for the non-Iceberg path.

## Upload Flow

```
IcebergFileTransferAgent.uploadWithoutConnection(metadata, data)
  ├─ compress stream + compute SHA-256 digest (FileBackedOutputStream, up to 128 MB before spilling)
  └─ IcebergStorageClientFactory.createClient(stageInfo)
       ├─ IcebergS3Client      (S3 via AWS SDK)
       ├─ IcebergAzureClient   (Azure Blob via azure-storage SDK)
       └─ IcebergGCSClient     (GCS via google-cloud-storage SDK)
            └─ IcebergStorageClient.upload(metadata, stream)
```

## Key Classes

- `IcebergFileTransferAgent` — orchestrates upload: reads `SnowflakeFileTransferMetadataV1` for stage credentials, compresses the input stream into a `FileBackedOutputStream` (max 128 MB in-memory buffer before disk spill), computes SHA-256 digest, then delegates to the appropriate storage client.
- `IcebergStorageClientFactory` — singleton factory; creates `IcebergS3Client`, `IcebergAzureClient`, or `IcebergGCSClient` based on `StageInfo.StageType`.
- `IcebergStorageClient` — interface: `upload()`, `uploadWithPresignedUrl()`, `getObjectMetadata()`, `renameObject()`.
- `IcebergS3Client` — S3 upload using AWS SDK. Supports SSE-S3, SSE-KMS, and CSE-C. Reads proxy settings via JDBC's `SFSessionProperty`.
- `IcebergAzureClient` — Azure Blob upload using the Azure Storage SDK. Supports presigned URL uploads.
- `IcebergGCSClient` — GCS upload using the Google Cloud Storage SDK. Supports presigned URL and authenticated uploads.
- `StorageHelper` — shared helpers: stream utilities, retry logic.
- `IcebergCommonObjectMetadata` / `IcebergS3ObjectMetadata` — implement `StorageObjectMetadata` for metadata reads/writes.

## JDBC Dependency (Removal In Progress)

This package still depends heavily on JDBC types. These are all targets of the JDBC removal project (Phase 3–4):

| JDBC Type | Used For |
|---|---|
| `StageInfo` / `StageInfo.StageType` | Cloud provider routing and stage credentials |
| `SnowflakeFileTransferMetadataV1` | Stage credentials + presigned URL passed to agent |
| `FileBackedOutputStream` | In-memory-then-disk stream buffer |
| `HttpClientSettingsKey` | HTTP client configuration key for GCS |
| `HttpUtil.isSocksProxyDisabled()` | SOCKS proxy detection |
| `SFSessionProperty` | Proxy property key names |
| `OCSPMode` | SSL certificate validation mode |
| `SnowflakeSQLException` / `SnowflakeSQLLoggedException` | Exception types |
| `StorageObjectMetadata` | Metadata interface |
| `SnowflakeUtil` | `createCaseInsensitiveMap()`, `convertProxyPropertiesToHttpClientKey()` |
| `SFPair` / `Stopwatch` | Utility types |
| `ErrorCode` / `SqlState` | Error constants |

## Gotchas

- `IcebergFileTransferAgent.MAX_BUFFER_SIZE` is 128 MB (1 << 27). Streams larger than this spill to a temp file via `FileBackedOutputStream`.
- The Iceberg path does **not** perform client-side encryption (unlike the non-Iceberg path through JDBC's `SnowflakeFileTransferAgent`). Encryption is handled server-side via SSE-KMS or SSE-S3.
- `IcebergStorageClientFactory` is a non-thread-safe singleton initialized lazily — safe only because it is always initialized during client startup before parallel flushes begin.

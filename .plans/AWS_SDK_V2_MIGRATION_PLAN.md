# Plan: Migrate from AWS SDK v1 to AWS SDK v2

## Background

The ingest SDK uses AWS SDK v1 (`com.amazonaws:1.12.655`) for S3 operations, GCS
access (via S3-compatible API), and HTTP proxy configuration. AWS SDK v1 is
deprecated and will reach end-of-support in 2025.

The JDBC driver (`snowflake-jdbc`) has **already completed** this migration to
AWS SDK v2 (`software.amazon.awssdk:2.37.5`). We can reference their
implementation and learn from their bug fixes.

---

## Scope

**19 files** import `com.amazonaws.*`, all in `fileTransferAgent/`:

| Category | Files | v2 Equivalent |
|---|---|---|
| S3 client (non-Iceberg) | `SnowflakeS3Client` | `S3AsyncClient` + `S3TransferManager` |
| S3 client (Iceberg) | `IcebergS3Client` | `S3AsyncClient` + `S3TransferManager` |
| GCS via AWS SDK | `AwsSdkGCPSigner`, `GCSAccessStrategyAwsSdk` | v2 `Signer` interface |
| Proxy/HTTP config | `S3HttpUtil`, `JdbcHttpUtil` | `ProxyConfiguration` + `ClientOverrideConfiguration` |
| Metadata wrappers | `S3ObjectMetadata`, `IcebergS3ObjectMetadata` | Direct `HeadObjectResponse` |
| Factories | `StorageClientFactory`, `IcebergStorageClientFactory` | Updated builders |
| Iterators | `S3ObjectSummariesIterator`, `StorageObjectSummary*` | `ListObjectsV2Response` |
| HTTP interceptor | `HeaderCustomizerHttpRequestInterceptor` | `ExecutionInterceptor` |
| Route planner | `SnowflakeMutableProxyRoutePlanner` | `ProxyConfiguration` |
| Trust manager | `SFTrustManager` | Minimal changes |

---

## Known Risks (from JDBC's migration experience)

### RISK 1: CipherInputStream + mark/reset — CRITICAL

**Problem:** `CipherInputStream` does not support `mark()/reset()`. AWS SDK v2's
`AsyncRequestBody.fromInputStream()` may retry failed uploads by resetting the
stream. If the stream is a `CipherInputStream`, this causes silent data corruption
(~1% of uploads produce unreadable files).

**Applies to us?** **YES — CRITICAL.** Our `EncryptionProvider.encrypt()` returns
a `CipherInputStream` (line 191), which is passed directly to TransferManager.
Same pattern that caused JDBC's bug.

**How JDBC fixed it (PR #2502):** In `SnowflakeS3Client.upload()`, they wrap
the stream with `BufferedInputStream` before passing to `AsyncRequestBody`:
```java
AsyncRequestBody.fromInputStream(
    new BufferedInputStream(uploadStreamInfo.left),
    contentLength, executorService)
```
`BufferedInputStream` supports mark/reset, so SDK retries work correctly.
File: `snowflake-jdbc/internal/jdbc/cloud/storage/SnowflakeS3Client.java`

**Fix:** Same — wrap `CipherInputStream` in `BufferedInputStream` at upload time.

**Affected files:** `SnowflakeS3Client.upload()` (non-Iceberg 128-bit encryption
path only). `IcebergS3Client` is NOT affected (no client-side encryption).

### RISK 2: Multipart threshold change — MEDIUM

**Problem:** AWS SDK v1 TransferManager default multipart threshold was 16MB.
SDK v2's `S3AsyncClient` with `multipartEnabled(true)` uses 8MB. Files 8-16MB
switch from single-part to multipart, adding extra API round trips.

**Applies to us?** **YES.** Blobs can be up to 1 GB and the 8-16MB range is
hit during normal operation.

**How JDBC fixed it (PR #2526):** Explicitly set 16MB threshold on the
`S3AsyncClient` builder:
```java
.multipartConfiguration(
    MultipartConfiguration.builder()
        .thresholdInBytes(16L * 1024 * 1024)
        .build())
```
File: `snowflake-jdbc/internal/jdbc/cloud/storage/SnowflakeS3Client.java`

**Fix:** Same — set 16MB threshold on all `S3AsyncClient` builders.

### RISK 3: S3Exception.awsErrorDetails() null — MEDIUM

**Problem:** `S3Exception.awsErrorDetails()` can return `null` in v2. Code that
calls `.errorCode()` on it without a null check will NPE.

**Applies to us?** **YES.** Our error handlers in `SnowflakeS3Client` and
`IcebergS3Client` check for `ExpiredToken` error code.

**How JDBC fixed it (PR #2550):** Added null check in `S3ErrorHandler.java`:
```java
if (e.awsErrorDetails() != null
    && EXPIRED_AWS_TOKEN_ERROR_CODE.equalsIgnoreCase(
        e.awsErrorDetails().errorCode())) {
```
Also used `constant.equalsIgnoreCase(variable)` pattern for null safety.
File: `snowflake-jdbc/internal/jdbc/cloud/storage/S3ErrorHandler.java`

**Fix:** Same — null-check `awsErrorDetails()` in all error handlers.

### RISK 4: Custom signer API change — HIGH

**Problem:** v1 uses `SignerFactory.registerSigner()` + `AWS4Signer` extension.
v2 uses `SdkAdvancedClientOption.SIGNER` + `software.amazon.awssdk.core.signer.Signer`.

**Applies to us?** **YES.** `GCSAccessStrategyAwsSdk` registers a custom
`AwsSdkGCPSigner` that extends v1's `AWS4Signer`.

**How JDBC did it:** Rewrote `AwsSdkGCPSigner` to implement v2's `Signer`
interface. The v2 signer:
1. Strips the AWS `Authorization` header
2. Adds `Authorization: Bearer <token>` for GCS
3. Maps `x-amz-*` → `x-goog-*` headers
4. Injected via `SdkAdvancedClientOption.SIGNER` on the client builder
5. Uses `AnonymousCredentialsProvider` (signing handled by custom signer)
File: `snowflake-jdbc/internal/jdbc/cloud/storage/AwsSdkGCPSigner.java`

**Fix:** Replicate JDBC's v2 `AwsSdkGCPSigner` implementation.

### RISK 5: aws-crt shading — LOW

**Problem:** `aws-crt` native library cannot be shaded. Must be excluded.

**Applies to us?** **YES.** We shade dependencies.

**How JDBC did it:** Excludes `aws-crt` from `s3`, `s3-transfer-manager`, and
`http-auth-aws` dependencies in `parent-pom.xml`. Also has a build comment:
"aws-crt cannot be shaded".
File: `snowflake-jdbc/parent-pom.xml`

**Fix:** Same — exclude `aws-crt` from all v2 deps that pull it transitively.

---

## Migration Steps

### Step 1: Update dependencies

Replace v1 deps with v2 in `pom.xml`:

```xml
<!-- Remove -->
<dependency>com.amazonaws:aws-java-sdk-core:1.12.655</dependency>
<dependency>com.amazonaws:aws-java-sdk-kms:1.12.655</dependency>
<dependency>com.amazonaws:aws-java-sdk-s3:1.12.655</dependency>

<!-- Add -->
<dependency>software.amazon.awssdk:s3</dependency>
<dependency>software.amazon.awssdk:s3-transfer-manager</dependency>
<dependency>software.amazon.awssdk:netty-nio-client</dependency>
<dependency>software.amazon.awssdk:auth</dependency>
<dependency>software.amazon.awssdk:regions</dependency>
<!-- BOM for version management -->
<dependency>software.amazon.awssdk:bom:2.37.5 (type=pom, scope=import)</dependency>
```

Exclude `aws-crt` from all v2 deps. Update shade rules:
`com.amazonaws` → `software.amazon.awssdk`.

### Step 2: Migrate IcebergS3Client (lowest risk)

Start here because it has NO client-side encryption (avoids Risk 1).

**Changes:**
- `AmazonS3ClientBuilder` → `S3AsyncClient.builder()`
- `BasicAWSCredentials`/`BasicSessionCredentials` → `AwsBasicCredentials`/`AwsSessionCredentials`
- `AWSStaticCredentialsProvider` → `StaticCredentialsProvider`
- `ClientConfiguration` → `NettyNioAsyncHttpClient.builder()` with `ProxyConfiguration`
- `TransferManager` → `S3TransferManager`
- `ObjectMetadata` → `PutObjectRequest.builder()` metadata
- `SSEAwsKeyManagementParams` → `ServerSideEncryption.AWS_KMS` + `ssekmsKeyId()`
- Set multipart threshold to 16MB (Risk 2)
- Null-check `awsErrorDetails()` in error handler (Risk 3)
- `Region`/`RegionUtils` → `software.amazon.awssdk.regions.Region`

### Step 3: Migrate SnowflakeS3Client (high risk — encryption)

Same as Step 2, plus:
- Wrap `CipherInputStream` in `BufferedInputStream` for uploads (Risk 1)
- `AmazonS3EncryptionClient` → removed (encryption is manual via EncryptionProvider)
- `CryptoConfiguration`/`EncryptionMaterials` → removed (not needed, encryption is JCE-based)
- Keep `EncryptionProvider.encrypt()` returning `CipherInputStream` but wrap at upload time

### Step 4: Migrate GCS-via-AWS-SDK

**Changes:**
- `AwsSdkGCPSigner` (extends `AWS4Signer`) → implement v2 `Signer` interface
  - Strip AWS Authorization header, add `Bearer` token
  - Map `x-amz-*` → `x-goog-*` headers
  - Inject via `SdkAdvancedClientOption.SIGNER`
- `GCSAccessStrategyAwsSdk`:
  - Use `S3AsyncClient` with GCS endpoint (`storage.googleapis.com`)
  - `AnonymousCredentialsProvider` (signing handled by custom signer)
  - `forcePathStyle(false)` for virtual-hosted style

### Step 5: Migrate support classes

- `S3HttpUtil` → use `ProxyConfiguration` from v2
- `S3ObjectMetadata`/`IcebergS3ObjectMetadata` → wrap v2 response types
- `HeaderCustomizerHttpRequestInterceptor` → implement `ExecutionInterceptor` (v2)
- `S3ObjectSummariesIterator` → use `ListObjectsV2Response`
- `StorageClientFactory`/`IcebergStorageClientFactory` → update builders
- `SnowflakeMutableProxyRoutePlanner` → `ProxyConfiguration` (v2 handles this)
- `StorageObjectSummary`/`StorageObjectSummaryCollection` → adapt to v2 types
- `StorageProviderException` → catch `SdkException` (v2 base exception)

### Step 6: Update shade plugin

```xml
<!-- Remove -->
<pattern>com.amazonaws</pattern>
<!-- Add -->
<pattern>software.amazon.awssdk</pattern>
<shadedPattern>${shadeBase}.software.amazon.awssdk</shadedPattern>
```

### Step 7: Update tests

Update all test files that reference v1 types. Verify:
- Encryption round-trip (EncryptionProviderTest, GcmEncryptionProviderTest)
- S3 client tests
- GCS signer test (AwsSdkGCPSignerTest)
- Integration tests on all 3 clouds

---

## PR Strategy

| PR | Content | Risk Level |
|---|---|---|
| PR 1 | Add v2 deps alongside v1, update shade rules | Low |
| PR 2 | Migrate `IcebergS3Client` + `IcebergStorageClientFactory` | Medium |
| PR 3 | Migrate `SnowflakeS3Client` + `StorageClientFactory` (encryption path) | **High** |
| PR 4 | Migrate `AwsSdkGCPSigner` + `GCSAccessStrategyAwsSdk` | Medium |
| PR 5 | Migrate support classes (metadata, iterators, interceptor, proxy) | Low |
| PR 6 | Remove v1 deps, clean up shade rules | Low |

---

## JDBC Reference Files

Key JDBC files to reference during migration (at `snowflakedb/snowflake-jdbc` main branch):

| JDBC File | Ingest Equivalent |
|---|---|
| `internal/jdbc/cloud/storage/SnowflakeS3Client.java` | `SnowflakeS3Client.java` |
| `internal/jdbc/cloud/storage/GCSAccessStrategyAwsSdk.java` | `GCSAccessStrategyAwsSdk.java` |
| `internal/jdbc/cloud/storage/AwsSdkGCPSigner.java` | `AwsSdkGCPSigner.java` |
| `internal/jdbc/cloud/storage/EncryptionProvider.java` | `EncryptionProvider.java` |
| `internal/jdbc/cloud/storage/S3ErrorHandler.java` | Error handling in S3 clients |
| `parent-pom.xml` (deps section) | `pom.xml` |

---

## Verification

- [ ] `mvn compiler:compile` passes
- [ ] All unit tests pass
- [ ] Integration tests pass on S3, Azure, GCS
- [ ] Encryption round-trip verified (upload encrypted → download → decrypt → matches original)
- [ ] Multipart uploads > 16MB work correctly
- [ ] Uploads < 16MB use single-part (not multipart)
- [ ] GCS uploads via S3-compatible API work
- [ ] SSE-KMS uploads (Iceberg path) work
- [ ] Proxy configuration works
- [ ] Shaded jar contains no `com.amazonaws` classes
- [ ] e2e-jar-test passes (shaded + unshaded)

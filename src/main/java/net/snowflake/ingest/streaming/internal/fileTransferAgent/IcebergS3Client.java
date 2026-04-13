package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static net.snowflake.ingest.streaming.internal.fileTransferAgent.ErrorCode.CLOUD_STORAGE_CREDENTIALS_EXPIRED;
import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.createDefaultExecutorService;
import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.getRootCause;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import net.snowflake.ingest.streaming.internal.VolumeEncryptionMode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFPair;
import net.snowflake.ingest.utils.SFSessionProperty;
import net.snowflake.ingest.utils.Stopwatch;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;

class IcebergS3Client implements IcebergStorageClient {
  private static final Logging logger = new Logging(IcebergS3Client.class);
  private static final String S3_STREAMING_INGEST_CLIENT_NAME = "ingestclientname";
  private static final String S3_STREAMING_INGEST_CLIENT_KEY = "ingestclientkey";
  // expired AWS token error code
  private static final String EXPIRED_AWS_TOKEN_ERROR_CODE = "ExpiredToken";

  // Multipart upload threshold: 16 MB
  private static final long MULTIPART_THRESHOLD_BYTES = 16L * 1024 * 1024;

  private boolean isUseS3RegionalUrl = false;
  private ClientConfiguration clientConfig = null;
  private ProxyConfiguration proxyConfig = null;
  private String stageRegion = null;
  private Properties proxyProperties = null;
  private String stageEndPoint = null; // FIPS endpoint, if needed
  private boolean isClientSideEncrypted = true;
  private S3AsyncClient amazonClient = null;
  private VolumeEncryptionMode volumeEncryptionMode = null;
  private String encryptionKmsKeyId = null;

  public IcebergS3Client(
      Map<?, ?> stageCredentials,
      ClientConfiguration clientConfig,
      Properties proxyProperties,
      String stageRegion,
      String stageEndPoint,
      boolean isClientSideEncrypted,
      boolean useS3RegionalUrl,
      VolumeEncryptionMode volumeEncryptionMode,
      String encryptionKmsKeyId)
      throws SnowflakeSQLException {
    this.isUseS3RegionalUrl = useS3RegionalUrl;
    this.volumeEncryptionMode = volumeEncryptionMode;
    this.encryptionKmsKeyId = encryptionKmsKeyId;
    setupSnowflakeS3Client(
        stageCredentials,
        clientConfig,
        proxyProperties,
        stageRegion,
        stageEndPoint,
        isClientSideEncrypted);
  }

  private void setupSnowflakeS3Client(
      Map<?, ?> stageCredentials,
      ClientConfiguration clientConfig,
      Properties proxyProperties,
      String stageRegion,
      String stageEndPoint,
      boolean isClientSideEncrypted)
      throws SnowflakeSQLException {
    // Save the client creation parameters so that we can reuse them,
    // to reset the AWS client. We won't save the awsCredentials since
    // we will be refreshing that, every time we reset the AWS client
    this.clientConfig = clientConfig;
    this.stageRegion = stageRegion;
    this.proxyProperties = proxyProperties;
    this.stageEndPoint = stageEndPoint; // FIPS endpoint, if needed
    this.isClientSideEncrypted = isClientSideEncrypted;

    // Retrieve S3 stage credentials
    String awsID = (String) stageCredentials.get("AWS_KEY_ID");
    String awsKey = (String) stageCredentials.get("AWS_SECRET_KEY");
    String awsToken = (String) stageCredentials.get("AWS_TOKEN");

    // initialize aws credentials
    AwsCredentials awsCredentials =
        (awsToken != null)
            ? AwsSessionCredentials.create(awsID, awsKey, awsToken)
            : AwsBasicCredentials.create(awsID, awsKey);

    this.proxyConfig = buildProxyConfiguration(proxyProperties);

    NettyNioAsyncHttpClient.Builder httpClientBuilder =
        NettyNioAsyncHttpClient.builder()
            .maxConcurrency(clientConfig.getMaxConnections())
            .connectionAcquisitionTimeout(Duration.ofSeconds(60))
            .connectionTimeout(Duration.ofMillis(clientConfig.getConnectionTimeout()))
            .readTimeout(Duration.ofMillis(clientConfig.getSocketTimeout()))
            .writeTimeout(Duration.ofMillis(clientConfig.getSocketTimeout()));
    if (this.proxyConfig != null) {
      httpClientBuilder.proxyConfiguration(this.proxyConfig);
    }

    S3AsyncClientBuilder s3Builder =
        S3AsyncClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
            .httpClientBuilder(httpClientBuilder)
            .forcePathStyle(false)
            .multipartEnabled(true)
            .multipartConfiguration(
                software.amazon.awssdk.services.s3.multipart.MultipartConfiguration.builder()
                    .thresholdInBytes(MULTIPART_THRESHOLD_BYTES)
                    .build());

    Region region = Region.of(stageRegion);
    if (this.stageEndPoint != null
        && !this.stageEndPoint.isEmpty()
        && !"null".equals(this.stageEndPoint)) {
      s3Builder.endpointOverride(URI.create(this.stageEndPoint));
      s3Builder.region(region);
    } else {
      if (stageRegion != null) {
        if (this.isUseS3RegionalUrl) {
          String domainSuffixForRegionalUrl = getDomainSuffixForRegionalUrl(stageRegion);
          s3Builder.endpointOverride(
              URI.create("https://s3." + stageRegion + "." + domainSuffixForRegionalUrl));
        }
        s3Builder.region(region);
      }
    }

    amazonClient = s3Builder.build();
  }

  /* Adds digest metadata to the StorageObjectMetadata object */
  @Override
  public void addDigestMetadata(StorageObjectMetadata meta, String digest) {
    meta.addUserMetadata("sfc-digest", digest);
  }

  /*
   * Adds streaming ingest metadata to the StorageObjectMetadata object, used for streaming ingest
   * per client billing calculation
   */
  @Override
  public void addStreamingIngestMetadata(
      StorageObjectMetadata meta, String clientName, String clientKey) {
    meta.addUserMetadata(S3_STREAMING_INGEST_CLIENT_NAME, clientName);
    meta.addUserMetadata(S3_STREAMING_INGEST_CLIENT_KEY, clientKey);
  }

  // Returns the Max number of retry attempts
  @Override
  public int getMaxRetries() {
    return 25;
  }

  // Returns the max exponent for multiplying backoff with the power of 2, the value
  // of 4 will give us 16secs as the max number of time to sleep before retry
  @Override
  public int getRetryBackoffMaxExponent() {
    return 4;
  }

  // Returns the min number of milliseconds to sleep before retry
  @Override
  public int getRetryBackoffMin() {
    return 1000;
  }

  /**
   * Upload a file (-stream) to S3.
   *
   * @param parallelism number of threads do parallel uploading
   * @param uploadFromStream true if upload source is stream
   * @param remoteStorageLocation s3 bucket name
   * @param srcFile source file if not uploading from a stream
   * @param destFileName file name on s3 after upload
   * @param inputStream stream used for uploading if fileBackedOutputStream is null
   * @param fileBackedOutputStream stream used for uploading if not null
   * @param meta object meta data
   * @param stageRegion region name where the stage persists
   * @param presignedUrl Not used in S3
   * @throws SnowflakeSQLException if upload failed even after retry
   */
  @Override
  public String upload(
      int parallelism,
      boolean uploadFromStream,
      String remoteStorageLocation,
      File srcFile,
      String destFileName,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutputStream,
      StorageObjectMetadata meta,
      String stageRegion,
      String presignedUrl)
      throws SnowflakeSQLException {
    logger.logInfo(
        StorageHelper.getStartUploadLog(
            "S3", uploadFromStream, inputStream, fileBackedOutputStream, srcFile, destFileName));

    final long originalContentLength = meta.getContentLength();
    final List<FileInputStream> toClose = new ArrayList<>();

    IcebergS3ObjectMetadata s3Meta;
    if (meta instanceof IcebergS3ObjectMetadata) {
      s3Meta = (IcebergS3ObjectMetadata) meta;
    } else {
      throw new IllegalArgumentException("Unexpected metadata object type");
    }

    SFPair<InputStream, Boolean> uploadStreamInfo =
        createUploadStream(
            srcFile,
            uploadFromStream,
            inputStream,
            fileBackedOutputStream,
            s3Meta,
            originalContentLength,
            toClose);

    S3TransferManager tx = null;
    int retryCount = 0;
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    do {
      try {
        logger.logDebug(
            "Creating executor service for transfer" + "manager with {} threads", parallelism);

        ExecutorService executor =
            createDefaultExecutorService("s3-transfer-manager-uploader-", parallelism);

        // upload files to s3
        tx = S3TransferManager.builder().s3Client(amazonClient).executor(executor).build();

        // Build the PutObjectRequest from metadata (includes CRC32 checksum)
        PutObjectRequest.Builder putBuilder =
            s3Meta.getS3PutObjectRequest().toBuilder()
                .bucket(remoteStorageLocation)
                .key(destFileName);

        if (this.volumeEncryptionMode != null) {
          this.volumeEncryptionMode.validateKmsKeyId(this.encryptionKmsKeyId);
        }
        if (VolumeEncryptionMode.AWS_SSE_KMS.equals(this.volumeEncryptionMode)) {
          putBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
          putBuilder.ssekmsKeyId(this.encryptionKmsKeyId);
        } else if (s3Meta.getSSEAlgorithm() != null) {
          // Apply SSE-S3 (AES256) if set in metadata
          putBuilder.serverSideEncryption(ServerSideEncryption.AES256);
        }

        PutObjectRequest putRequest = putBuilder.build();

        // Wrap the stream with BufferedInputStream to work around CipherInputStream bug
        InputStream uploadStream = uploadStreamInfo.left;
        AsyncRequestBody requestBody;
        if (uploadStreamInfo.right) {
          requestBody =
              AsyncRequestBody.fromInputStream(
                  new BufferedInputStream(uploadStream), originalContentLength, executor);
        } else {
          requestBody = AsyncRequestBody.fromFile(srcFile.toPath());
        }

        Upload myUpload =
            tx.upload(
                UploadRequest.builder()
                    .putObjectRequest(putRequest)
                    .requestBody(requestBody)
                    .build());
        CompletedUpload result = myUpload.completionFuture().join();
        stopwatch.stop();
        long uploadMillis = stopwatch.elapsedMillis();

        // get out
        for (FileInputStream is : toClose) {
          IOUtils.closeQuietly(is);
        }

        if (uploadFromStream) {
          logger.logInfo(
              "Uploaded data from input stream to S3 location: {}. It took {} ms with {} retries",
              destFileName,
              uploadMillis,
              retryCount);
        } else {
          logger.logInfo(
              "Uploaded file {} to S3 location: {}. It took {} ms with {} retries",
              srcFile.getAbsolutePath(),
              destFileName,
              uploadMillis,
              retryCount);
        }
        return result.response().eTag();
      } catch (Exception ex) {

        handleS3Exception(ex, ++retryCount, "upload", this);
        if (uploadFromStream && fileBackedOutputStream == null) {
          throw new SnowflakeSQLException(
              ex,
              SqlState.SYSTEM_ERROR,
              ErrorCode.IO_ERROR.getMessageCode(),
              "Encountered exception during upload: "
                  + ex.getMessage()
                  + "\nCannot retry upload from stream.");
        }
        uploadStreamInfo =
            createUploadStream(
                srcFile,
                uploadFromStream,
                inputStream,
                fileBackedOutputStream,
                s3Meta,
                originalContentLength,
                toClose);
      } finally {
        if (tx != null) {
          tx.close();
        }
      }
    } while (retryCount <= getMaxRetries());

    for (FileInputStream is : toClose) {
      IOUtils.closeQuietly(is);
    }

    throw new SnowflakeSQLLoggedException(
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        SqlState.INTERNAL_ERROR,
        "Unexpected: upload unsuccessful without exception!");
  }

  static String getDomainSuffixForRegionalUrl(String regionName) {
    return regionName.toLowerCase().startsWith("cn-") ? "amazonaws.com.cn" : "amazonaws.com";
  }

  /**
   * Checks the status code of the exception to see if it's a 400 or 404
   *
   * @param ex exception
   * @return true if it's a 400 or 404 status code
   */
  public boolean isClientException400Or404(Throwable ex) {
    if (ex instanceof SdkServiceException) {
      SdkServiceException ssEx = (SdkServiceException) ex;
      return ssEx.statusCode() == HttpStatus.SC_NOT_FOUND
          || ssEx.statusCode() == HttpStatus.SC_BAD_REQUEST;
    }
    return false;
  }

  private SFPair<InputStream, Boolean> createUploadStream(
      File srcFile,
      boolean uploadFromStream,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutputStream,
      IcebergS3ObjectMetadata meta,
      long originalContentLength,
      List<FileInputStream> toClose)
      throws SnowflakeSQLException {
    logger.logDebug(
        "createUploadStream({}, {}, {}, {}, {}, {}, {})",
        this,
        srcFile,
        uploadFromStream,
        inputStream,
        fileBackedOutputStream,
        meta,
        toClose);
    final InputStream result;
    FileInputStream srcFileStream = null;
    try {
      if (!isClientSideEncrypted) {
        // since we're not client-side encrypting, make sure we're server-side encrypting
        if (this.volumeEncryptionMode == null
            || VolumeEncryptionMode.NONE.equals(this.volumeEncryptionMode)
            || VolumeEncryptionMode.AWS_SSE_S3.equals(this.volumeEncryptionMode)) {
          // Default to SSE-S3 encryption (AES256)
          meta.setSSEAlgorithm("AES256");
        } else if (VolumeEncryptionMode.AWS_SSE_KMS.equals(this.volumeEncryptionMode)) {
          meta.setSSEAlgorithm("aws:kms");
        } else {
          throw new IllegalArgumentException(
              "Unexpected volume encryption mode: "
                  + this.volumeEncryptionMode
                  + ". Expected "
                  + VolumeEncryptionMode.AWS_SSE_S3
                  + " or "
                  + VolumeEncryptionMode.AWS_SSE_KMS);
        }
      }

      result =
          uploadFromStream
              ? (fileBackedOutputStream != null
                  ? fileBackedOutputStream.asByteSource().openStream()
                  : inputStream)
              : (srcFileStream = new FileInputStream(srcFile));
      toClose.add(srcFileStream);

    } catch (FileNotFoundException ex) {
      logger.logError("Failed to open input file", ex);
      throw new SnowflakeSQLLoggedException(
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          "Failed to open input file",
          ex.getMessage());
    } catch (IOException ex) {
      logger.logError("Failed to open input stream", ex);
      throw new SnowflakeSQLLoggedException(
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          "Failed to open input stream",
          ex.getMessage());
    }

    return SFPair.of(result, uploadFromStream);
  }

  private static void handleS3Exception(
      Exception ex, int retryCount, String operation, IcebergS3Client s3Client)
      throws SnowflakeSQLException {
    // no need to retry if it is invalid key exception
    if (ex.getCause() instanceof InvalidKeyException) {
      // Most likely cause is that the unlimited strength policy files are not installed
      // Log the error and throw a message that explains the cause
      StorageClientUtil.throwJCEMissingError(operation, ex);
    }

    // If there is no space left in the download location, java.io.IOException is thrown.
    // Don't retry.
    if (getRootCause(ex) instanceof IOException) {
      StorageClientUtil.throwNoSpaceLeftError(null, operation, ex);
    }

    // Don't retry if max retries has been reached or the error code is 404/400
    // Check ex.getCause() because CompletableFuture.join() wraps in CompletionException
    Throwable cause = ex.getCause();
    if (cause instanceof SdkException) {
      logger.logDebug("SdkException: " + ex.getMessage());
      if (retryCount > s3Client.getMaxRetries() || s3Client.isClientException400Or404(cause)) {
        String extendedRequestId = "none";

        if (cause instanceof S3Exception) {
          S3Exception s3ex = (S3Exception) cause;
          extendedRequestId = s3ex.extendedRequestId() != null ? s3ex.extendedRequestId() : "none";
        }

        if (cause instanceof SdkServiceException) {
          SdkServiceException ex1 = (SdkServiceException) cause;
          // The AWS credentials might have expired when server returns error 400 and
          // does not return the ExpiredToken error code.
          // If session is null we cannot renew the token so throw the exception
          String errorCode = "Unknown";
          String errorType = "Unknown";
          if (cause instanceof S3Exception) {
            S3Exception s3ex = (S3Exception) cause;
            if (s3ex.awsErrorDetails() != null) {
              errorCode = s3ex.awsErrorDetails().errorCode();
              errorType = s3ex.awsErrorDetails().errorMessage();
            }
          }
          throw new SnowflakeSQLLoggedException(
              SqlState.SYSTEM_ERROR,
              ErrorCode.S3_OPERATION_ERROR.getMessageCode(),
              ex1,
              operation,
              errorType,
              errorCode,
              ex1.getMessage(),
              ex1.requestId() != null ? ex1.requestId() : "none",
              extendedRequestId);
        } else {
          throw new SnowflakeSQLLoggedException(
              SqlState.SYSTEM_ERROR,
              ErrorCode.AWS_CLIENT_ERROR.getMessageCode(),
              ex,
              operation,
              ex.getMessage());
        }
      } else {
        logger.logDebug(
            "Encountered exception ({}) during {}, retry count: {}",
            ex.getMessage(),
            operation,
            retryCount);
        logger.logDebug("Stack trace: ", ex);

        // exponential backoff up to a limit
        int backoffInMillis = s3Client.getRetryBackoffMin();

        if (retryCount > 1) {
          backoffInMillis <<= (Math.min(retryCount - 1, s3Client.getRetryBackoffMaxExponent()));
        }

        try {
          logger.logDebug("Sleep for {} milliseconds before retry", backoffInMillis);

          Thread.sleep(backoffInMillis);
        } catch (InterruptedException ex1) {
          // ignore
        }

        // If the exception indicates that the AWS token has expired,
        // we need to refresh our S3 client with the new token
        if (cause instanceof S3Exception) {
          S3Exception s3ex = (S3Exception) cause;
          if (s3ex.awsErrorDetails() != null
              && EXPIRED_AWS_TOKEN_ERROR_CODE.equalsIgnoreCase(
                  s3ex.awsErrorDetails().errorCode())) {
            // If session is null we cannot renew the token so throw the ExpiredToken exception
            throw new SnowflakeSQLException(
                s3ex.awsErrorDetails().errorCode(),
                CLOUD_STORAGE_CREDENTIALS_EXPIRED,
                "S3 credentials have expired");
          }
        }
      }
    } else {
      if (ex instanceof InterruptedException
          || getRootCause(ex) instanceof SocketTimeoutException
          || ex instanceof CompletionException) {
        if (retryCount > s3Client.getMaxRetries()) {
          throw new SnowflakeSQLLoggedException(
              SqlState.SYSTEM_ERROR,
              ErrorCode.IO_ERROR.getMessageCode(),
              ex,
              "Encountered exception during " + operation + ": " + ex.getMessage());
        } else {
          logger.logDebug(
              "Encountered exception ({}) during {}, retry count: {}",
              ex.getMessage(),
              operation,
              retryCount);
        }
      } else {
        throw new SnowflakeSQLLoggedException(
            SqlState.SYSTEM_ERROR,
            ErrorCode.IO_ERROR.getMessageCode(),
            ex,
            "Encountered exception during " + operation + ": " + ex.getMessage());
      }
    }
  }

  /**
   * Builds a ProxyConfiguration from proxy properties, or returns null if no proxy is configured.
   *
   * @param proxyProperties Proxy Properties
   * @return ProxyConfiguration or null
   * @throws SnowflakeSQLException Thrown on configuration parsing failures
   */
  private static ProxyConfiguration buildProxyConfiguration(Properties proxyProperties)
      throws SnowflakeSQLException {
    if (proxyProperties != null
        && proxyProperties.size() > 0
        && proxyProperties.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()) != null) {
      final boolean useProxy =
          Boolean.parseBoolean(
              proxyProperties.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()));
      if (useProxy) {
        String proxyHost =
            proxyProperties.getProperty(SFSessionProperty.PROXY_HOST.getPropertyKey());

        int proxyPort;
        try {
          proxyPort =
              Integer.parseInt(
                  proxyProperties.getProperty(SFSessionProperty.PROXY_PORT.getPropertyKey()));
        } catch (NullPointerException | NumberFormatException var11) {
          throw new SnowflakeSQLException(
              ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
        }

        String proxyUser =
            proxyProperties.getProperty(SFSessionProperty.PROXY_USER.getPropertyKey());
        String proxyPassword =
            proxyProperties.getProperty(SFSessionProperty.PROXY_PASSWORD.getPropertyKey());
        String nonProxyHosts =
            proxyProperties.getProperty(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey());
        String proxyProtocol =
            proxyProperties.getProperty(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey());
        String scheme =
            isNotEmpty(proxyProtocol) && proxyProtocol.equalsIgnoreCase("https") ? "https" : "http";

        ProxyConfiguration.Builder proxyBuilder =
            ProxyConfiguration.builder()
                .scheme(scheme)
                .host(proxyHost)
                .port(proxyPort)
                .useEnvironmentVariableValues(false)
                .useSystemPropertyValues(false);

        if (isNotEmpty(proxyUser) && isNotEmpty(proxyPassword)) {
          proxyBuilder.username(proxyUser).password(proxyPassword);
        }

        if (isNotEmpty(nonProxyHosts)) {
          Set<String> nonProxyHostSet = new HashSet<>();
          for (String host : nonProxyHosts.split("\\|")) {
            if (!host.isEmpty()) {
              nonProxyHostSet.add(host);
            }
          }
          proxyBuilder.nonProxyHosts(nonProxyHostSet);
        }

        String logMessage =
            String.format(
                "Set sessionless S3 proxy. Host: %s, port: %d, protocol: %s, non-proxy hosts: %s",
                proxyHost, proxyPort, scheme, nonProxyHosts);
        if (isNotEmpty(proxyUser) && isNotEmpty(proxyPassword)) {
          logMessage = String.format("%s, user: %s with password provided", logMessage, proxyUser);
        }
        logger.logDebug(logMessage);

        return proxyBuilder.build();
      } else {
        logger.logDebug("Omitting sessionless S3 proxy setup as proxy is disabled");
      }
    } else {
      logger.logDebug("Omitting sessionless S3 proxy setup");
    }
    return null;
  }

  private static boolean isNotEmpty(final String string) {
    return string != null && !string.isEmpty();
  }

  public static class ClientConfiguration {
    private final int maxConnections;
    private final int maxErrorRetry;
    private final int connectionTimeout;
    private final int socketTimeout;

    public ClientConfiguration(
        int maxConnections, int maxErrorRetry, int connectionTimeout, int socketTimeout) {
      this.maxConnections = maxConnections;
      this.maxErrorRetry = maxErrorRetry;
      this.connectionTimeout = connectionTimeout;
      this.socketTimeout = socketTimeout;
    }

    public int getMaxConnections() {
      return maxConnections;
    }

    public int getMaxErrorRetry() {
      return maxErrorRetry;
    }

    public int getConnectionTimeout() {
      return connectionTimeout;
    }

    public int getSocketTimeout() {
      return socketTimeout;
    }
  }
}

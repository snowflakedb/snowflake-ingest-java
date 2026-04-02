/* Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/cloud/storage/GCSAccessStrategyAwsSdk.java
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static net.snowflake.ingest.streaming.internal.fileTransferAgent.ErrorCode.CLOUD_STORAGE_CREDENTIALS_EXPIRED;
import static net.snowflake.ingest.streaming.internal.fileTransferAgent.SnowflakeS3Client.EXPIRED_AWS_TOKEN_ERROR_CODE;
import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.createDefaultExecutorService;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.SignerFactory;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import java.io.File;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;
import net.snowflake.ingest.utils.SFPair;
import org.apache.http.HttpStatus;

class GCSAccessStrategyAwsSdk implements GCSAccessStrategy {
  private static final SFLogger logger = SFLoggerFactory.getLogger(GCSAccessStrategyAwsSdk.class);
  private final AmazonS3 amazonClient;

  GCSAccessStrategyAwsSdk(StageInfo stage)
      throws SnowflakeSQLException, net.snowflake.client.jdbc.SnowflakeSQLException {
    String accessToken = (String) stage.getCredentials().get("GCS_ACCESS_TOKEN");

    Optional<String> oEndpoint = stage.gcsCustomEndpoint();
    String endpoint = "storage.googleapis.com";
    if (oEndpoint.isPresent()) {
      endpoint = oEndpoint.get();
    }
    if (endpoint.startsWith("https://")) {
      endpoint = endpoint.replaceFirst("https://", "");
    }
    if (stage.getStorageAccount() != null && endpoint.startsWith(stage.getStorageAccount())) {
      endpoint = endpoint.replaceFirst(stage.getStorageAccount() + ".", "");
    }

    AmazonS3ClientBuilder amazonS3Builder =
        AmazonS3Client.builder()
            .withPathStyleAccessEnabled(false)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(endpoint, "auto"));

    ClientConfiguration clientConfig = new ClientConfiguration();

    SignerFactory.registerSigner(
        "net.snowflake.client.jdbc.cloud.storage.AwsSdkGCPSigner",
        net.snowflake.client.jdbc.cloud.storage.AwsSdkGCPSigner.class);
    clientConfig.setSignerOverride("net.snowflake.client.jdbc.cloud.storage.AwsSdkGCPSigner");

    clientConfig
        .getApacheHttpClientConfig()
        .setSslSocketFactory(SnowflakeS3Client.getSSLConnectionSocketFactory());
    S3HttpUtil.setSessionlessProxyForS3(stage.getProxyProperties(), clientConfig);

    if (accessToken != null) {
      amazonS3Builder.withCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessToken, "")));
    } else {
      logger.debug("no credentials provided, configuring bucket client without credentials");
      amazonS3Builder.withCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials("", "")));
    }

    amazonClient = amazonS3Builder.withClientConfiguration(clientConfig).build();
  }

  @Override
  public StorageObjectSummaryCollection listObjects(String remoteStorageLocation, String prefix) {
    ObjectListing objListing = amazonClient.listObjects(remoteStorageLocation, prefix);

    return new StorageObjectSummaryCollection(objListing.getObjectSummaries());
  }

  @Override
  public StorageObjectMetadata getObjectMetadata(String remoteStorageLocation, String prefix) {
    ObjectMetadata meta = amazonClient.getObjectMetadata(remoteStorageLocation, prefix);

    Map<String, String> userMetadata =
        meta.getRawMetadata().entrySet().stream()
            .filter(entry -> entry.getKey().startsWith("x-goog-meta-"))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().replaceFirst("x-goog-meta-", ""),
                    e -> e.getValue().toString()));

    meta.setUserMetadata(userMetadata);
    return new S3ObjectMetadata(meta);
  }

  @Override
  public Map<String, String> download(
      int parallelism, String remoteStorageLocation, String stageFilePath, File localFile)
      throws InterruptedException {

    logger.debug(
        "Staring download of file from S3 stage path: {} to {}",
        stageFilePath,
        localFile.getAbsolutePath());

    TransferManager tx = null;
    logger.debug("Creating executor service for transfer manager with {} threads", parallelism);
    try {

      // download files from s3
      tx =
          TransferManagerBuilder.standard()
              .withS3Client(amazonClient)
              .withDisableParallelDownloads(true)
              .withExecutorFactory(
                  () ->
                      createDefaultExecutorService("s3-transfer-manager-downloader-", parallelism))
              .build();

      Download myDownload = tx.download(remoteStorageLocation, stageFilePath, localFile);

      // Pull object metadata from S3
      StorageObjectMetadata meta = this.getObjectMetadata(remoteStorageLocation, stageFilePath);

      Map<String, String> metaMap =
          StorageClientUtil.createCaseInsensitiveMap(meta.getUserMetadata());
      myDownload.waitForCompletion();
      return metaMap;
    } finally {
      if (tx != null) {
        tx.shutdownNow(false);
      }
    }
  }

  @Override
  public SFPair<InputStream, Map<String, String>> downloadToStream(
      String remoteStorageLocation, String stageFilePath, boolean isEncrypting) {
    S3Object file = amazonClient.getObject(remoteStorageLocation, stageFilePath);
    ObjectMetadata meta = amazonClient.getObjectMetadata(remoteStorageLocation, stageFilePath);
    InputStream stream = file.getObjectContent();

    Map<String, String> metaMap =
        StorageClientUtil.createCaseInsensitiveMap(meta.getUserMetadata());

    return SFPair.of(stream, metaMap);
  }

  @Override
  public void uploadWithDownScopedToken(
      int parallelism,
      String remoteStorageLocation,
      String destFileName,
      String contentEncoding,
      Map<String, String> metadata,
      long contentLength,
      InputStream content,
      String queryId)
      throws InterruptedException {
    // we need to assemble an ObjectMetadata object here, as we are not using S3ObjectMatadata for
    // GCS
    ObjectMetadata s3Meta = new ObjectMetadata();
    if (contentEncoding != null) {
      s3Meta.setContentEncoding(contentEncoding);
    }
    s3Meta.setContentLength(contentLength);
    s3Meta.setUserMetadata(metadata);

    TransferManager tx = null;
    logger.debug("Creating executor service for transfer manager with {} threads", parallelism);
    try {
      // upload files to s3
      tx =
          TransferManagerBuilder.standard()
              .withS3Client(amazonClient)
              .withExecutorFactory(
                  () -> createDefaultExecutorService("s3-transfer-manager-uploader-", parallelism))
              .build();

      final Upload myUpload;

      myUpload = tx.upload(remoteStorageLocation, destFileName, content, s3Meta);
      myUpload.waitForCompletion();

      logger.info("Uploaded data from input stream to S3 location: {}.", destFileName);

    } finally {
      if (tx != null) {
        tx.shutdownNow(false);
      }
    }
  }

  private static boolean isClientException400Or404(Exception ex) {
    if (ex instanceof AmazonServiceException) {
      AmazonServiceException asEx = (AmazonServiceException) (ex);
      return asEx.getStatusCode() == HttpStatus.SC_NOT_FOUND
          || asEx.getStatusCode() == HttpStatus.SC_BAD_REQUEST;
    }
    return false;
  }

  @Override
  public boolean handleStorageException(
      Exception ex,
      int retryCount,
      String operation,
      String command,
      String queryId,
      SnowflakeGCSClient gcsClient)
      throws SnowflakeSQLException, net.snowflake.client.jdbc.SnowflakeSQLException {
    if (ex instanceof AmazonClientException) {
      logger.debug("GCSAccessStrategyAwsSdk: " + ex.getMessage());

      if (retryCount > gcsClient.getMaxRetries() || isClientException400Or404(ex)) {
        String extendedRequestId = "none";

        if (ex instanceof AmazonS3Exception) {
          AmazonS3Exception ex1 = (AmazonS3Exception) ex;
          extendedRequestId = ex1.getExtendedRequestId();
        }

        if (ex instanceof AmazonServiceException) {
          AmazonServiceException ex1 = (AmazonServiceException) ex;

          throw new SnowflakeSQLLoggedException(
              queryId,
              null,
              SqlState.SYSTEM_ERROR,
              ErrorCode.S3_OPERATION_ERROR.getMessageCode(),
              ex1,
              operation,
              ex1.getErrorType().toString(),
              ex1.getErrorCode(),
              ex1.getMessage(),
              ex1.getRequestId(),
              extendedRequestId);

        } else {
          throw new SnowflakeSQLLoggedException(
              queryId,
              null,
              SqlState.SYSTEM_ERROR,
              ErrorCode.AWS_CLIENT_ERROR.getMessageCode(),
              ex,
              operation,
              ex.getMessage());
        }
      } else {
        logger.debug(
            "Encountered exception ({}) during {}, retry count: {}",
            ex.getMessage(),
            operation,
            retryCount);
        logger.debug("Stack trace: ", ex);

        // exponential backoff up to a limit
        int backoffInMillis = gcsClient.getRetryBackoffMin();

        if (retryCount > 1) {
          backoffInMillis <<= (Math.min(retryCount - 1, gcsClient.getRetryBackoffMaxExponent()));
        }

        try {
          logger.debug("Sleep for {} milliseconds before retry", backoffInMillis);

          Thread.sleep(backoffInMillis);
        } catch (InterruptedException ex1) {
          // ignore
        }

        // If the exception indicates that the AWS token has expired,
        // we need to refresh our S3 client with the new token
        if (ex instanceof AmazonS3Exception) {
          AmazonS3Exception s3ex = (AmazonS3Exception) ex;
          if (s3ex.getErrorCode().equalsIgnoreCase(EXPIRED_AWS_TOKEN_ERROR_CODE)) {
            throw new SnowflakeSQLException(
                queryId,
                s3ex.getErrorCode(),
                CLOUD_STORAGE_CREDENTIALS_EXPIRED,
                "S3 credentials have expired");
          }
        }
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void shutdown() {
    if (this.amazonClient != null) {
      this.amazonClient.shutdown();
    }
  }
}

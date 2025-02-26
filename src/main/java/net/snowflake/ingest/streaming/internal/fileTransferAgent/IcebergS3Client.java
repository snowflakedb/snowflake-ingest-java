package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static net.snowflake.client.core.Constants.CLOUD_STORAGE_CREDENTIALS_EXPIRED;
import static net.snowflake.client.jdbc.SnowflakeUtil.createDefaultExecutorService;
import static net.snowflake.client.jdbc.SnowflakeUtil.getRootCause;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFSSLConnectionSocketFactory;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.FileBackedOutputStream;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.cloud.storage.S3HttpUtil;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.client.jdbc.internal.amazonaws.AmazonClientException;
import net.snowflake.client.jdbc.internal.amazonaws.AmazonServiceException;
import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration;
import net.snowflake.client.jdbc.internal.amazonaws.auth.AWSCredentials;
import net.snowflake.client.jdbc.internal.amazonaws.auth.AWSStaticCredentialsProvider;
import net.snowflake.client.jdbc.internal.amazonaws.auth.BasicAWSCredentials;
import net.snowflake.client.jdbc.internal.amazonaws.auth.BasicSessionCredentials;
import net.snowflake.client.jdbc.internal.amazonaws.client.builder.AwsClientBuilder;
import net.snowflake.client.jdbc.internal.amazonaws.client.builder.ExecutorFactory;
import net.snowflake.client.jdbc.internal.amazonaws.regions.Region;
import net.snowflake.client.jdbc.internal.amazonaws.regions.RegionUtils;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3Builder;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3Client;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.AmazonS3Exception;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.ObjectMetadata;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.PutObjectRequest;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.transfer.TransferManager;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.transfer.TransferManagerBuilder;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.transfer.Upload;
import net.snowflake.client.jdbc.internal.apache.http.HttpStatus;
import net.snowflake.client.jdbc.internal.apache.http.conn.ssl.SSLConnectionSocketFactory;
import net.snowflake.client.jdbc.internal.apache.http.conn.ssl.SSLInitializationException;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState;
import net.snowflake.client.util.SFPair;
import net.snowflake.client.util.Stopwatch;
import net.snowflake.ingest.utils.Logging;
import org.apache.commons.io.IOUtils;

class IcebergS3Client implements IcebergStorageClient {
  private static final Logging logger = new Logging(IcebergS3Client.class);
  private static final String S3_STREAMING_INGEST_CLIENT_NAME = "ingestclientname";
  private static final String S3_STREAMING_INGEST_CLIENT_KEY = "ingestclientkey";
  // expired AWS token error code
  private static final String EXPIRED_AWS_TOKEN_ERROR_CODE = "ExpiredToken";

  private boolean isUseS3RegionalUrl = false;
  private ClientConfiguration clientConfig = null;
  private String stageRegion = null;
  private Properties proxyProperties = null;
  private String stageEndPoint = null; // FIPS endpoint, if needed
  private boolean isClientSideEncrypted = true;
  private AmazonS3 amazonClient = null;

  // socket factory used by s3 client's http client.
  private static SSLConnectionSocketFactory s3ConnectionSocketFactory = null;

  private static SSLConnectionSocketFactory getSSLConnectionSocketFactory() {
    if (s3ConnectionSocketFactory == null) {
      synchronized (IcebergS3Client.class) {
        if (s3ConnectionSocketFactory == null) {
          try {
            // trust manager is set to null, which will use default ones
            // instead of SFTrustManager (which enables ocsp checking)
            s3ConnectionSocketFactory =
                new SFSSLConnectionSocketFactory(null, HttpUtil.isSocksProxyDisabled());
          } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new SSLInitializationException(e.getMessage(), e);
          }
        }
      }
    }

    return s3ConnectionSocketFactory;
  }

  public IcebergS3Client(
      Map<?, ?> stageCredentials,
      ClientConfiguration clientConfig,
      Properties proxyProperties,
      String stageRegion,
      String stageEndPoint,
      boolean isClientSideEncrypted,
      boolean useS3RegionalUrl)
      throws SnowflakeSQLException {
    this.isUseS3RegionalUrl = useS3RegionalUrl;
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
    AWSCredentials awsCredentials =
        (awsToken != null)
            ? new BasicSessionCredentials(awsID, awsKey, awsToken)
            : new BasicAWSCredentials(awsID, awsKey);

    clientConfig.withSignerOverride("AWSS3V4SignerType");
    clientConfig.getApacheHttpClientConfig().setSslSocketFactory(getSSLConnectionSocketFactory());
    S3HttpUtil.setSessionlessProxyForS3(proxyProperties, clientConfig);
    AmazonS3Builder<?, ?> amazonS3Builder =
        AmazonS3Client.builder()
            .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
            .withClientConfiguration(clientConfig);

    Region region = RegionUtils.getRegion(stageRegion);
    if (this.stageEndPoint != null && this.stageEndPoint != "" && this.stageEndPoint != "null") {
      amazonS3Builder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(this.stageEndPoint, region.getName()));
    } else {
      if (region != null) {
        if (this.isUseS3RegionalUrl) {
          String domainSuffixForRegionalUrl = getDomainSuffixForRegionalUrl(region.getName());
          amazonS3Builder.withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(
                  "s3." + region.getName() + "." + domainSuffixForRegionalUrl, region.getName()));
        } else {
          amazonS3Builder.withRegion(region.getName());
        }
      }
    }
    // Explicitly force to use virtual address style
    amazonS3Builder.withPathStyleAccessEnabled(false);
    amazonClient = (AmazonS3) amazonS3Builder.build();
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
    SFPair<InputStream, Boolean> uploadStreamInfo =
        createUploadStream(
            srcFile,
            uploadFromStream,
            inputStream,
            fileBackedOutputStream,
            ((IcebergS3ObjectMetadata) meta).getS3ObjectMetadata(),
            originalContentLength,
            toClose);

    ObjectMetadata s3Meta;
    if (meta instanceof IcebergS3ObjectMetadata) {
      s3Meta = ((IcebergS3ObjectMetadata) meta).getS3ObjectMetadata();
    } else {
      throw new IllegalArgumentException("Unexpected metadata object type");
    }

    TransferManager tx = null;
    int retryCount = 0;
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    do {
      try {
        logger.logDebug(
            "Creating executor service for transfer" + "manager with {} threads", parallelism);

        // upload files to s3
        tx =
            TransferManagerBuilder.standard()
                .withS3Client(amazonClient)
                .withExecutorFactory(
                    new ExecutorFactory() {
                      @Override
                      public ExecutorService newExecutor() {
                        return createDefaultExecutorService(
                            "s3-transfer-manager-uploader-", parallelism);
                      }
                    })
                .build();

        final Upload myUpload;

        if (!this.isClientSideEncrypted) {
          // since we're not client-side encrypting, make sure we're server-side encrypting with
          // SSE-S3
          s3Meta.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }

        if (uploadStreamInfo.right) {
          myUpload = tx.upload(remoteStorageLocation, destFileName, uploadStreamInfo.left, s3Meta);
        } else {
          PutObjectRequest putRequest =
              new PutObjectRequest(remoteStorageLocation, destFileName, srcFile);
          putRequest.setMetadata(s3Meta);

          myUpload = tx.upload(putRequest);
        }

        myUpload.waitForCompletion();
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
        // -------------------------NEW LOGIC HERE START -----------------------------------------
        return myUpload.waitForUploadResult().getETag();
        // -------------------------NEW LOGIC HERE END -----------------------------------------
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
          tx.shutdownNow(false);
        }
      }
    } while (retryCount <= getMaxRetries());

    for (FileInputStream is : toClose) {
      IOUtils.closeQuietly(is);
    }

    throw new SnowflakeSQLLoggedException(
        null,
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
  public boolean isClientException400Or404(Exception ex) {
    if (ex instanceof AmazonServiceException) {
      AmazonServiceException asEx = (AmazonServiceException) (ex);
      return asEx.getStatusCode() == HttpStatus.SC_NOT_FOUND
          || asEx.getStatusCode() == HttpStatus.SC_BAD_REQUEST;
    }
    return false;
  }

  private SFPair<InputStream, Boolean> createUploadStream(
      File srcFile,
      boolean uploadFromStream,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutputStream,
      ObjectMetadata meta,
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
        // since we're not client-side encrypting, make sure we're server-side encrypting with
        // SSE-S3
        meta.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
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
          null /* session */,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          "Failed to open input file",
          ex.getMessage());
    } catch (IOException ex) {
      logger.logError("Failed to open input stream", ex);
      throw new SnowflakeSQLLoggedException(
          null /* session */,
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
      SnowflakeFileTransferAgent.throwJCEMissingError(operation, ex, null /* queryId */);
    }

    // If there is no space left in the download location, java.io.IOException is thrown.
    // Don't retry.
    if (getRootCause(ex) instanceof IOException) {
      SnowflakeFileTransferAgent.throwNoSpaceLeftError(
          null /* session */, operation, ex, null /* queryId */);
    }

    // Don't retry if max retries has been reached or the error code is 404/400
    if (ex instanceof AmazonClientException) {
      logger.logDebug("AmazonClientException: " + ex.getMessage());
      if (retryCount > s3Client.getMaxRetries() || s3Client.isClientException400Or404(ex)) {
        String extendedRequestId = "none";

        if (ex instanceof AmazonS3Exception) {
          AmazonS3Exception ex1 = (AmazonS3Exception) ex;
          extendedRequestId = ex1.getExtendedRequestId();
        }

        if (ex instanceof AmazonServiceException) {
          AmazonServiceException ex1 = (AmazonServiceException) ex;
          // The AWS credentials might have expired when server returns error 400 and
          // does not return the ExpiredToken error code.
          // If session is null we cannot renew the token so throw the exception
          throw new SnowflakeSQLLoggedException(
              null /* session */,
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
              null /* session */,
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
        if (ex instanceof AmazonS3Exception) {
          AmazonS3Exception s3ex = (AmazonS3Exception) ex;
          if (s3ex.getErrorCode().equalsIgnoreCase(EXPIRED_AWS_TOKEN_ERROR_CODE)) {
            // If session is null we cannot renew the token so throw the ExpiredToken exception
            throw new SnowflakeSQLException(
                s3ex.getErrorCode(),
                CLOUD_STORAGE_CREDENTIALS_EXPIRED,
                "S3 credentials have expired");
          }
        }
      }
    } else {
      if (ex instanceof InterruptedException
          || getRootCause(ex) instanceof SocketTimeoutException) {
        if (retryCount > s3Client.getMaxRetries()) {
          throw new SnowflakeSQLLoggedException(
              null /* session */,
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
            null /* session */,
            SqlState.SYSTEM_ERROR,
            ErrorCode.IO_ERROR.getMessageCode(),
            ex,
            "Encountered exception during " + operation + ": " + ex.getMessage());
      }
    }
  }
}

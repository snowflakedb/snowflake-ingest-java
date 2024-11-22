package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static net.snowflake.client.jdbc.SnowflakeUtil.getRootCause;
import static net.snowflake.client.jdbc.SnowflakeUtil.isBlank;

import com.google.common.base.Strings;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.FileBackedOutputStream;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.client.jdbc.internal.google.api.gax.rpc.FixedHeaderProvider;
import net.snowflake.client.jdbc.internal.google.cloud.storage.Blob;
import net.snowflake.client.jdbc.internal.google.cloud.storage.BlobId;
import net.snowflake.client.jdbc.internal.google.cloud.storage.BlobInfo;
import net.snowflake.client.jdbc.internal.google.cloud.storage.Storage;
import net.snowflake.client.jdbc.internal.google.cloud.storage.StorageException;
import net.snowflake.client.jdbc.internal.google.cloud.storage.StorageOptions;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState;
import net.snowflake.client.util.SFPair;
import net.snowflake.client.util.Stopwatch;
import net.snowflake.ingest.utils.Logging;
import org.apache.commons.io.IOUtils;

class IcebergGCSClient implements IcebergStorageClient {
  private static final String GCS_STREAMING_INGEST_CLIENT_NAME = "ingestclientname";
  private static final String GCS_STREAMING_INGEST_CLIENT_KEY = "ingestclientkey";

  private static final Logging logger = new Logging(IcebergGCSClient.class);
  private StageInfo stageInfo;
  private Storage gcsClient = null;

  private IcebergGCSClient() {}

  /*
   * Factory method for a SnowflakeGCSClient object
   * @param stage   The stage information that the client will operate on
   * @param encMat  The encryption material
   *                required to decrypt/encrypt content in stage
   */
  public static IcebergGCSClient createSnowflakeGCSClient(StageInfo stage)
      throws SnowflakeSQLException {
    IcebergGCSClient sfGcsClient = new IcebergGCSClient();
    sfGcsClient.setupGCSClient(stage);

    return sfGcsClient;
  }

  /** Adds digest metadata to the StorageObjectMetadata object */
  @Override
  public void addDigestMetadata(StorageObjectMetadata meta, String digest) {
    if (!isBlank(digest)) {
      meta.addUserMetadata("sfc-digest", digest);
    }
  }

  /**
   * Adds streaming ingest metadata to the StorageObjectMetadata object, used for streaming ingest
   * per client billing calculation
   */
  @Override
  public void addStreamingIngestMetadata(
      StorageObjectMetadata meta, String clientName, String clientKey) {
    meta.addUserMetadata(GCS_STREAMING_INGEST_CLIENT_NAME, clientName);
    meta.addUserMetadata(GCS_STREAMING_INGEST_CLIENT_KEY, clientKey);
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

  /*
   * Initializes the GCS client
   * This method is used during the object construction, but also to
   * reset/recreate the encapsulated CloudBlobClient object with new
   * credentials (after token expiration)
   * @param stage   The stage information that the client will operate on
   * @param encMat  The encryption material
   *                required to decrypt/encrypt content in stage
   * @throws IllegalArgumentException when invalid credentials are used
   */
  private void setupGCSClient(StageInfo stage)
      throws IllegalArgumentException, SnowflakeSQLException {
    // Save the client creation parameters so that we can reuse them,
    // to reset the GCS client.
    this.stageInfo = stage;

    logger.logDebug("Setting up the GCS client ", false);

    try {
      String accessToken = (String) stage.getCredentials().get("GCS_ACCESS_TOKEN");
      if (accessToken != null) {
        // We are authenticated with an oauth access token.
        StorageOptions.Builder builder = StorageOptions.newBuilder();

        // Using GoogleCredential with access token will cause IllegalStateException when the token
        // is expired and trying to refresh, which cause error cannot be caught. Instead, set a
        // header so we can caught the error code.
        this.gcsClient =
            builder
                .setHeaderProvider(
                    FixedHeaderProvider.create("Authorization", "Bearer " + accessToken))
                .build()
                .getService();
      } else {
        // Use anonymous authentication.
        this.gcsClient = StorageOptions.getUnauthenticatedInstance().getService();
      }
    } catch (Exception ex) {
      throw new IllegalArgumentException("invalid_gcs_credentials");
    }
  }

  @Override
  public String upload(
      int parallel,
      boolean uploadFromStream,
      String location,
      File srcFile,
      String destFileName,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutStr,
      StorageObjectMetadata meta,
      String region,
      String presignedUrl)
      throws SnowflakeSQLException {
    throw new SnowflakeSQLLoggedException(
        null,
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        SqlState.INTERNAL_ERROR,
        /*session = */ "IcebergGCSClient.upload" + " only works with pre-signed URL.");
  }

  /**
   * Upload a file (-stream) to remote storage with Pre-signed URL without JDBC session.
   *
   * @param networkTimeoutInMilli Network timeout for the upload
   * @param ocspModeAndProxyKey OCSP mode and proxy settings for the upload.
   * @param parallelism number of threads do parallel uploading
   * @param uploadFromStream true if upload source is stream
   * @param remoteStorageLocation s3 bucket name
   * @param srcFile source file if not uploading from a stream
   * @param destFileName file name on remote storage after upload
   * @param inputStream stream used for uploading if fileBackedOutputStream is null
   * @param fileBackedOutputStream stream used for uploading if not null
   * @param meta object meta data
   * @param stageRegion region name where the stage persists
   * @param presignedUrl presigned URL for upload. Used by GCP.
   * @throws SnowflakeSQLException if upload failed
   */
  @Override
  public String uploadWithPresignedUrlWithoutConnection(
      int networkTimeoutInMilli,
      HttpClientSettingsKey ocspModeAndProxyKey,
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
            "GCS", uploadFromStream, inputStream, fileBackedOutputStream, srcFile, destFileName));
    final List<FileInputStream> toClose = new ArrayList<>();
    long originalContentLength = meta.getContentLength();

    SFPair<InputStream, Boolean> uploadStreamInfo =
        createUploadStream(
            srcFile,
            uploadFromStream,
            inputStream,
            meta,
            originalContentLength,
            fileBackedOutputStream,
            toClose);

    if (!(meta instanceof IcebergCommonObjectMetadata)) {
      throw new IllegalArgumentException("Unexpected metadata object type");
    }
    Stopwatch stopwatch = new Stopwatch();
    Blob uploadedBlob;
    stopwatch.start();
    if (Strings.isNullOrEmpty(presignedUrl) || "null".equalsIgnoreCase(presignedUrl)) {
      logger.logDebug("Starting upload with downscoped token");
      uploadedBlob =
          uploadWithDownScopedToken(
              remoteStorageLocation,
              destFileName,
              meta.getContentEncoding(),
              meta.getUserMetadata(),
              uploadStreamInfo.left);
      logger.logDebug("Upload successful with downscoped token");
    } else {
      throw new IllegalArgumentException("Unexpected non-null presignedUrl");
    }
    stopwatch.stop();

    if (uploadFromStream) {
      logger.logInfo(
          "Uploaded data from input stream to GCS location: {}. It took {} ms",
          remoteStorageLocation,
          stopwatch.elapsedMillis());
    } else {
      logger.logInfo(
          "Uploaded file {} to GCS location: {}. It took {} ms",
          srcFile.getAbsolutePath(),
          remoteStorageLocation,
          stopwatch.elapsedMillis());
    }

    // close any open streams in the "toClose" list and return
    for (FileInputStream is : toClose) {
      IOUtils.closeQuietly(is);
    }

    // For GCS, return null instead of returning uploadedBlob.getEtag(), as the service does expect
    // to see blob MD5
    // and not the ETag.
    return null;
  }

  /**
   * Upload file with down scoped token.
   *
   * @param remoteStorageLocation storage container name
   * @param destFileName file name on remote storage after upload
   * @param contentEncoding Object's content encoding. We do special things for "gzip"
   * @param metadata Custom metadata to be uploaded with the object
   * @param content File content
   * @return
   */
  private Blob uploadWithDownScopedToken(
      String remoteStorageLocation,
      String destFileName,
      String contentEncoding,
      Map<String, String> metadata,
      InputStream content)
      throws SnowflakeSQLException {
    logger.logDebug("Uploading file {} to bucket {}", destFileName, remoteStorageLocation);
    BlobId blobId = BlobId.of(remoteStorageLocation, destFileName);
    BlobInfo blobInfo =
        BlobInfo.newBuilder(blobId)
            .setContentEncoding(contentEncoding)
            .setMetadata(metadata)
            .build();

    try {
      return gcsClient.create(blobInfo, content);
    } catch (Exception e) {
      handleStorageException(e, 0, "upload");
      throw e;
    }
  }

  private SFPair<InputStream, Boolean> createUploadStream(
      File srcFile,
      boolean uploadFromStream,
      InputStream inputStream,
      StorageObjectMetadata meta,
      long originalContentLength,
      FileBackedOutputStream fileBackedOutputStream,
      List<FileInputStream> toClose)
      throws SnowflakeSQLException {
    logger.logDebug(
        "createUploadStream({}, {}, {}, {}, {}, {})",
        this,
        srcFile,
        uploadFromStream,
        inputStream,
        fileBackedOutputStream,
        toClose);

    final InputStream stream;
    FileInputStream srcFileStream = null;
    try {
      if (uploadFromStream) {
        if (fileBackedOutputStream != null) {
          stream = fileBackedOutputStream.asByteSource().openStream();
        } else {
          stream = inputStream;
        }
      } else {
        srcFileStream = new FileInputStream(srcFile);
        toClose.add(srcFileStream);
        stream = srcFileStream;
      }
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

    return SFPair.of(stream, uploadFromStream);
  }

  private void handleStorageException(Exception ex, int retryCount, String operation)
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

    if (ex instanceof StorageException) {
      // NOTE: this code path only handle Access token based operation,
      // presigned URL is not covered. Presigned Url do not raise
      // StorageException

      StorageException se = (StorageException) ex;
      // If we have exceeded the max number of retries, propagate the error
      if (retryCount > getMaxRetries()) {
        throw new SnowflakeSQLLoggedException(
            null /* session */,
            SqlState.SYSTEM_ERROR,
            ErrorCode.GCP_SERVICE_ERROR.getMessageCode(),
            se,
            operation,
            se.getCode(),
            se.getMessage(),
            se.getReason());
      } else {
        logger.logDebug(
            "Encountered exception ({}) during {}, retry count: {}",
            ex.getMessage(),
            operation,
            retryCount);
        logger.logDebug("Stack trace: ", ex);

        // exponential backoff up to a limit
        int backoffInMillis = getRetryBackoffMin();

        if (retryCount > 1) {
          backoffInMillis <<= (Math.min(retryCount - 1, getRetryBackoffMaxExponent()));
        }

        try {
          logger.logDebug("Sleep for {} milliseconds before retry", backoffInMillis);

          Thread.sleep(backoffInMillis);
        } catch (InterruptedException ex1) {
          // ignore
        }
      }
    } else if (ex instanceof InterruptedException
        || getRootCause(ex) instanceof SocketTimeoutException) {
      if (retryCount > getMaxRetries()) {
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

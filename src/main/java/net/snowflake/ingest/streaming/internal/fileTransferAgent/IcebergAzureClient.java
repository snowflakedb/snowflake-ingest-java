package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static net.snowflake.client.core.Constants.CLOUD_STORAGE_CREDENTIALS_EXPIRED;
import static net.snowflake.client.core.HttpUtil.setSessionlessProxyForAzure;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.FileBackedOutputStream;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.OperationContext;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.StorageCredentials;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.StorageCredentialsAnonymous;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.StorageException;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.StorageExtendedErrorInformation;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.BlobRequestOptions;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.CloudBlobClient;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.CloudBlobContainer;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.CloudBlockBlob;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState;
import net.snowflake.client.util.SFPair;
import net.snowflake.client.util.Stopwatch;
import net.snowflake.ingest.utils.Logging;
import org.apache.commons.io.IOUtils;

class IcebergAzureClient implements IcebergStorageClient {
  private static final String AZ_STREAMING_INGEST_CLIENT_NAME = "ingestclientname";
  private static final String AZ_STREAMING_INGEST_CLIENT_KEY = "ingestclientkey";
  private static final Logging logger = new Logging(IcebergAzureClient.class);
  private StageInfo stageInfo;
  private CloudBlobClient azStorageClient;
  private OperationContext opContext;

  public static IcebergAzureClient createSnowflakeAzureClient(StageInfo stage)
      throws SnowflakeSQLException {
    IcebergAzureClient azureClient = new IcebergAzureClient();
    azureClient.setupAzureClient(stage);

    return azureClient;
  }

  /** Adds digest metadata to the StorageObjectMetadata object */
  @Override
  public void addDigestMetadata(StorageObjectMetadata meta, String digest) {
    if (!SnowflakeUtil.isBlank(digest)) {
      // Azure doesn't allow hyphens in the name of a metadata field.
      meta.addUserMetadata("sfcdigest", digest);
    }
  }

  /**
   * Adds streaming ingest metadata to the StorageObjectMetadata object, used for streaming ingest
   * per client billing calculation
   */
  @Override
  public void addStreamingIngestMetadata(
      StorageObjectMetadata meta, String clientName, String clientKey) {
    meta.addUserMetadata(AZ_STREAMING_INGEST_CLIENT_NAME, clientName);
    meta.addUserMetadata(AZ_STREAMING_INGEST_CLIENT_KEY, clientKey);
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
   * Initializes the Azure client
   * This method is used during the object construction, but also to
   * reset/recreate the encapsulated CloudBlobClient object with new
   * credentials (after SAS token expiration)
   * @param stage   The stage information that the client will operate on
   * @param encMat  The encryption material
   *                required to decrypt/encrypt content in stage
   * @throws IllegalArgumentException when invalid credentials are used
   */
  private void setupAzureClient(StageInfo stage)
      throws IllegalArgumentException, SnowflakeSQLException {
    // Save the client creation parameters so that we can reuse them,
    // to reset the Azure client.
    this.stageInfo = stage;

    logger.logDebug("Setting up the Azure client ", false);

    try {
      URI storageEndpoint =
          buildAzureStorageEndpointURI(stage.getEndPoint(), stage.getStorageAccount());

      StorageCredentials azCreds;
      String sasToken = (String) stage.getCredentials().get("AZURE_SAS_TOKEN");
      if (sasToken != null) {
        // We are authenticated with a shared access token.
        azCreds = new StorageCredentialsSharedAccessSignature(sasToken);
      } else {
        // Use anonymous authentication.
        azCreds = StorageCredentialsAnonymous.ANONYMOUS;
      }

      this.azStorageClient = new CloudBlobClient(storageEndpoint, azCreds);
      opContext = new OperationContext();
      setSessionlessProxyForAzure(stage.getProxyProperties(), opContext);
    } catch (URISyntaxException ex) {
      throw new IllegalArgumentException("invalid_azure_credentials");
    }
  }

  /*
   * Builds a URI to an Azure Storage account endpoint
   *
   *  @param storageEndPoint   the storage endpoint name
   *  @param storageAccount    the storage account name
   */
  private static URI buildAzureStorageEndpointURI(String storageEndPoint, String storageAccount)
      throws URISyntaxException {
    URI storageEndpoint =
        new URI("https", storageAccount + "." + storageEndPoint + "/", null, null);

    return storageEndpoint;
  }

  /**
   * Upload a file/stream to remote storage
   *
   * @param parallelism number of threads for parallel uploading
   * @param uploadFromStream true if upload source is stream
   * @param remoteStorageLocation storage container name
   * @param srcFile source file if not uploading from a stream
   * @param destFileName file name on remote storage after upload
   * @param inputStream stream used for uploading if fileBackedOutputStream is null
   * @param fileBackedOutputStream stream used for uploading if not null
   * @param meta object meta data
   * @param stageRegion region name where the stage persists
   * @param presignedUrl Unused in Azure
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
            "Azure", uploadFromStream, inputStream, fileBackedOutputStream, srcFile, destFileName));
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

    int retryCount = 0;
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    do {
      try {
        InputStream fileInputStream = uploadStreamInfo.left;
        CloudBlobContainer container = azStorageClient.getContainerReference(remoteStorageLocation);
        CloudBlockBlob blob = container.getBlockBlobReference(destFileName);

        // Set the user-defined/Snowflake metadata and upload the BLOB
        blob.setMetadata(new HashMap<>(meta.getUserMetadata()));

        BlobRequestOptions transferOptions = new BlobRequestOptions();
        transferOptions.setConcurrentRequestCount(parallelism);

        blob.upload(
            fileInputStream, // input stream to upload from
            -1, // -1 indicates an unknown stream length
            null,
            transferOptions,
            opContext);
        stopwatch.stop();

        if (uploadFromStream) {
          logger.logInfo(
              "Uploaded data from input stream to Azure location: {}. It took {} ms with {}"
                  + " retries",
              remoteStorageLocation,
              stopwatch.elapsedMillis(),
              retryCount);
        } else {
          logger.logInfo(
              "Uploaded file {} to Azure location: {}. It took {} ms with {} retries",
              srcFile.getAbsolutePath(),
              remoteStorageLocation,
              stopwatch.elapsedMillis(),
              retryCount);
        }

        blob.uploadMetadata(null, transferOptions, opContext);

        // close any open streams in the "toClose" list and return
        for (FileInputStream is : toClose) {
          IOUtils.closeQuietly(is);
        }

        return blob.getProperties().getEtag();
      } catch (Exception ex) {
        handleAzureException(ex, ++retryCount, "upload", this);

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
                meta,
                originalContentLength,
                fileBackedOutputStream,
                toClose);
      }

    } while (retryCount <= getMaxRetries());

    for (FileInputStream is : toClose) {
      IOUtils.closeQuietly(is);
    }

    throw new SnowflakeSQLException(
        SqlState.INTERNAL_ERROR,
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        "Unexpected: upload unsuccessful without exception!");
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

  /**
   * Handles exceptions thrown by Azure Storage It will retry transient errors as defined by the
   * Azure Client retry policy It will re-create the client if the SAS token has expired, and re-try
   *
   * @param ex the exception to handle
   * @param retryCount current number of retries, incremented by the caller before each call
   * @param operation string that indicates the function/operation that was taking place, when the
   *     exception was raised, for example "upload"
   * @param azClient the current Snowflake Azure client object
   * @throws SnowflakeSQLException exceptions not handled
   */
  private static void handleAzureException(
      Exception ex, int retryCount, String operation, IcebergAzureClient azClient)
      throws SnowflakeSQLException {

    // no need to retry if it is invalid key exception
    if (ex.getCause() instanceof InvalidKeyException) {
      // Most likely cause is that the unlimited strength policy files are not installed
      // Log the error and throw a message that explains the cause
      SnowflakeFileTransferAgent.throwJCEMissingError(operation, ex, null /* queryId */);
    }

    // If there is no space left in the download location, java.io.IOException is thrown.
    // Don't retry.
    if (SnowflakeUtil.getRootCause(ex) instanceof IOException) {
      SnowflakeFileTransferAgent.throwNoSpaceLeftError(
          null /* session */, operation, ex, null /* queryId */);
    }

    if (ex instanceof StorageException) {
      StorageException se = (StorageException) ex;

      if (((StorageException) ex).getHttpStatusCode() == 403) {
        // A 403 indicates that the SAS token has expired,
        // we need to refresh the Azure client with the new token
        // If session is null we cannot renew the token so throw the ExpiredToken exception
        throw new SnowflakeSQLException(
            se.getErrorCode(),
            CLOUD_STORAGE_CREDENTIALS_EXPIRED,
            "Azure credentials may have expired");
      }
      // If we have exceeded the max number of retries, propagate the error
      // no need for back off and retry if the file does not exist
      if (retryCount > azClient.getMaxRetries()
          || ((StorageException) ex).getHttpStatusCode() == 404) {
        throw new SnowflakeSQLLoggedException(
            null /* session */,
            SqlState.SYSTEM_ERROR,
            ErrorCode.AZURE_SERVICE_ERROR.getMessageCode(),
            se,
            operation,
            se.getErrorCode(),
            se.getHttpStatusCode(),
            se.getMessage(),
            FormatStorageExtendedErrorInformation(se.getExtendedErrorInformation()));
      } else {
        logger.logDebug(
            "Encountered exception ({}) during {}, retry count: {}",
            ex.getMessage(),
            operation,
            retryCount);
        logger.logDebug("Stack trace: ", ex);

        // exponential backoff up to a limit
        int backoffInMillis = azClient.getRetryBackoffMin();

        if (retryCount > 1) {
          backoffInMillis <<= (Math.min(retryCount - 1, azClient.getRetryBackoffMaxExponent()));
        }

        try {
          logger.logDebug("Sleep for {} milliseconds before retry", backoffInMillis);

          Thread.sleep(backoffInMillis);
        } catch (InterruptedException ex1) {
          // ignore
        }
      }
    } else {
      if (ex instanceof InterruptedException
          || SnowflakeUtil.getRootCause(ex) instanceof SocketTimeoutException) {
        if (retryCount > azClient.getMaxRetries()) {
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

  /**
   * Format the StorageExtendedErrorInformation to a String.
   *
   * @param info the StorageExtendedErrorInformation object
   * @return
   */
  static String FormatStorageExtendedErrorInformation(StorageExtendedErrorInformation info) {
    if (info == null) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("StorageExceptionExtendedErrorInformation: {ErrorCode= ");
    sb.append(info.getErrorCode());
    sb.append(", ErrorMessage= ");
    sb.append(info.getErrorMessage());

    HashMap<String, String[]> details = info.getAdditionalDetails();
    if (details != null) {
      sb.append(", AdditionalDetails= { ");
      for (Map.Entry<String, String[]> detail : details.entrySet()) {
        sb.append(detail.getKey());
        sb.append("= ");

        for (String value : detail.getValue()) {
          sb.append(value);
        }
        sb.append(",");
      }
      sb.setCharAt(sb.length() - 1, '}'); // overwrite the last comma
    }
    sb.append("}");
    return sb.toString();
  }
}

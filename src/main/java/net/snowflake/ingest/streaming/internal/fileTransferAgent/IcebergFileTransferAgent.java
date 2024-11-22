package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Properties;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.jdbc.FileBackedOutputStream;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.ingest.utils.Logging;
import org.apache.commons.io.IOUtils;

public class IcebergFileTransferAgent {
  private static final Logging logger = new Logging(IcebergFileTransferAgent.class);

  // We will allow buffering of upto 128M data before spilling to disk during
  // compression and digest computation
  static final int MAX_BUFFER_SIZE = 1 << 27;

  static final IcebergStorageClientFactory storageFactory =
      IcebergStorageClientFactory.getFactory();

  static class InputStreamWithMetadata {
    long size;
    String digest;

    // FileBackedOutputStream that should be destroyed when
    // the input stream has been consumed entirely
    FileBackedOutputStream fileBackedOutputStream;

    InputStreamWithMetadata(
        long size, String digest, FileBackedOutputStream fileBackedOutputStream) {
      this.size = size;
      this.digest = digest;
      this.fileBackedOutputStream = fileBackedOutputStream;
    }
  }

  /** Remote object location: "bucket" for S3, "container" for Azure BLOB */
  static class remoteLocation {
    String location;
    String path;

    public remoteLocation(String remoteStorageLocation, String remotePath) {
      location = remoteStorageLocation;
      path = remotePath;
    }
  }

  /**
   * Static API function to upload data without JDBC session.
   *
   * <p>NOTE: This function is developed based on getUploadFileCallable().
   *
   * @throws Exception if error occurs while data upload.
   */
  public static String uploadWithoutConnection(
      SnowflakeFileTransferMetadataV1 metadata,
      InputStream uploadStream,
      Properties proxyProperties,
      String streamingIngestClientName,
      String streamingIngestClientKey,
      String fullFilePath)
      throws Exception {
    OCSPMode ocspMode = OCSPMode.FAIL_OPEN;
    int networkTimeoutInMilli = 0;

    // Create HttpClient key
    HttpClientSettingsKey key =
        SnowflakeUtil.convertProxyPropertiesToHttpClientKey(ocspMode, proxyProperties);

    StageInfo stageInfo = metadata.getStageInfo();
    stageInfo.setProxyProperties(proxyProperties);
    String destFileName = metadata.getPresignedUrlFileName();

    logger.logDebug("Begin upload data for {}", destFileName);

    long uploadSize;
    File fileToUpload = null;
    String digest = null;

    // Temp file that needs to be cleaned up when upload was successful
    FileBackedOutputStream fileBackedOutputStream = null;

    // SNOW-16082: we should capture exception if we fail to compress or calculate digest.
    try {
      // If it's not local_fs, we store our digest in the metadata
      // In local_fs, we don't need digest, and if we turn it on,
      // we will consume whole uploadStream, which local_fs uses.
      IcebergFileTransferAgent.InputStreamWithMetadata result = computeDigest(uploadStream, true);
      digest = result.digest;
      fileBackedOutputStream = result.fileBackedOutputStream;
      uploadSize = result.size;

      if (result.fileBackedOutputStream.getFile() != null) {
        fileToUpload = result.fileBackedOutputStream.getFile();
      }

      logger.logDebug(
          "Started copying file to {}:{} destName: {} size={}",
          stageInfo.getStageType().name(),
          stageInfo.getLocation(),
          destFileName,
          uploadSize);

      IcebergStorageClient initialClient = storageFactory.createClient(stageInfo, 1);

      switch (stageInfo.getStageType()) {
        case S3:
        case AZURE:
          return pushFileToRemoteStore(
              metadata.getStageInfo(),
              destFileName,
              uploadStream,
              fileBackedOutputStream,
              uploadSize,
              digest,
              initialClient,
              1 /* parallel */,
              fileToUpload /* srcFile */,
              (fileToUpload == null) /* uploadFromStream */,
              streamingIngestClientName,
              streamingIngestClientKey);

        case GCS:
          destFileName = fullFilePath;

          return pushFileToRemoteStoreWithPresignedUrl(
              metadata.getStageInfo(),
              destFileName,
              uploadStream,
              fileBackedOutputStream,
              uploadSize,
              digest,
              initialClient,
              networkTimeoutInMilli,
              key,
              1 /* parallel */,
              metadata.getPresignedUrl(),
              streamingIngestClientName,
              streamingIngestClientKey);

        default:
          throw new IllegalArgumentException(
              "Unknown stageInfo.getStageType()=" + stageInfo.getStageType().name());
      }
    } catch (Exception ex) {
      logger.logError("Exception encountered during file upload in uploadWithoutConnection", ex);
      throw ex;
    } finally {
      if (fileBackedOutputStream != null) {
        try {
          fileBackedOutputStream.reset();
        } catch (IOException ex) {
          logger.logDebug("Failed to clean up temp file: {}", ex);
        }
      }
    }
  }

  private static String pushFileToRemoteStore(
      StageInfo stage,
      String destFileName,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutStr,
      long uploadSize,
      String digest,
      IcebergStorageClient initialClient,
      int parallel,
      File srcFile,
      boolean uploadFromStream,
      String streamingIngestClientName,
      String streamingIngestClientKey)
      throws SQLException, IOException {
    IcebergFileTransferAgent.remoteLocation remoteLocation =
        extractLocationAndPath(stage.getLocation());

    String origDestFileName = destFileName;
    if (remoteLocation.path != null && !remoteLocation.path.isEmpty()) {
      destFileName =
          remoteLocation.path + (!remoteLocation.path.endsWith("/") ? "/" : "") + destFileName;
    }

    logger.logDebug(
        "Upload object. Location: {}, key: {}, srcFile: {}",
        remoteLocation.location,
        destFileName,
        srcFile);

    StorageObjectMetadata meta = storageFactory.createStorageMetadataObj(stage.getStageType());
    meta.setContentLength(uploadSize);
    if (digest != null) {
      initialClient.addDigestMetadata(meta, digest);
    }

    if (streamingIngestClientName != null && streamingIngestClientKey != null) {
      initialClient.addStreamingIngestMetadata(
          meta, streamingIngestClientName, streamingIngestClientKey);
    }

    try {
      String presignedUrl = "";
      return initialClient.upload(
          parallel,
          uploadFromStream,
          remoteLocation.location,
          srcFile,
          destFileName,
          inputStream,
          fileBackedOutStr,
          meta,
          stage.getRegion(),
          presignedUrl);
    } finally {
      if (uploadFromStream && inputStream != null) {
        inputStream.close();
      }
    }
  }

  /**
   * Push a file (or stream) to remote store with pre-signed URL without JDBC session.
   *
   * <p>NOTE: This function is developed based on pushFileToRemoteStore(). The main difference is
   * that the caller needs to provide pre-signed URL and the upload doesn't need JDBC session.
   */
  private static String pushFileToRemoteStoreWithPresignedUrl(
      StageInfo stage,
      String destFileName,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutStr,
      long uploadSize,
      String digest,
      IcebergStorageClient initialClient,
      int networkTimeoutInMilli,
      HttpClientSettingsKey ocspModeAndProxyKey,
      int parallel,
      String presignedUrl,
      String streamingIngestClientName,
      String streamingIngestClientKey)
      throws SQLException, IOException {
    IcebergFileTransferAgent.remoteLocation remoteLocation =
        extractLocationAndPath(stage.getLocation());

    if (remoteLocation.path != null && !remoteLocation.path.isEmpty()) {
      destFileName =
          remoteLocation.path + (!remoteLocation.path.endsWith("/") ? "/" : "") + destFileName;
    }

    logger.logDebug("Upload object. Location: {}, key: {}", remoteLocation.location, destFileName);

    StorageObjectMetadata meta = storageFactory.createStorageMetadataObj(stage.getStageType());
    meta.setContentLength(uploadSize);
    if (digest != null) {
      initialClient.addDigestMetadata(meta, digest);
    }

    if (streamingIngestClientName != null && streamingIngestClientKey != null) {
      initialClient.addStreamingIngestMetadata(
          meta, streamingIngestClientName, streamingIngestClientKey);
    }

    try {
      return initialClient.uploadWithPresignedUrlWithoutConnection(
          networkTimeoutInMilli,
          ocspModeAndProxyKey,
          parallel,
          true /* uploadFromStream */,
          remoteLocation.location,
          null /* srcFile */,
          destFileName,
          inputStream,
          fileBackedOutStr,
          meta,
          stage.getRegion(),
          presignedUrl);
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }

  private static IcebergFileTransferAgent.InputStreamWithMetadata computeDigest(
      InputStream is, boolean resetStream) throws NoSuchAlgorithmException, IOException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    if (resetStream) {
      FileBackedOutputStream tempStream = new FileBackedOutputStream(MAX_BUFFER_SIZE, true);

      CountingOutputStream countingOutputStream = new CountingOutputStream(tempStream);

      DigestOutputStream digestStream = new DigestOutputStream(countingOutputStream, md);

      IOUtils.copy(is, digestStream);

      return new IcebergFileTransferAgent.InputStreamWithMetadata(
          countingOutputStream.getCount(),
          Base64.getEncoder().encodeToString(digestStream.getMessageDigest().digest()),
          tempStream);
    } else {
      CountingOutputStream countingOutputStream =
          new CountingOutputStream(ByteStreams.nullOutputStream());

      DigestOutputStream digestStream = new DigestOutputStream(countingOutputStream, md);
      IOUtils.copy(is, digestStream);
      return new IcebergFileTransferAgent.InputStreamWithMetadata(
          countingOutputStream.getCount(),
          Base64.getEncoder().encodeToString(digestStream.getMessageDigest().digest()),
          null);
    }
  }

  /**
   * A small helper for extracting location name and path from full location path
   *
   * @param stageLocationPath stage location
   * @return remoteLocation object
   */
  public static IcebergFileTransferAgent.remoteLocation extractLocationAndPath(
      String stageLocationPath) {
    String location = stageLocationPath;
    String path = "";

    // split stage location as location name and path
    if (stageLocationPath.contains("/")) {
      location = stageLocationPath.substring(0, stageLocationPath.indexOf("/"));
      path = stageLocationPath.substring(stageLocationPath.indexOf("/") + 1);
    }

    return new IcebergFileTransferAgent.remoteLocation(location, path);
  }
}

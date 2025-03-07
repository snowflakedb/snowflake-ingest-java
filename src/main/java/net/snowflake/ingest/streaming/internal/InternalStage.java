/*
 * Copyright (c) 2024-2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.HttpUtil.generateProxyPropertiesForJDBC;
import static net.snowflake.ingest.utils.Utils.getStackTrace;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeFileTransferConfig;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;
import net.snowflake.client.jdbc.internal.google.cloud.storage.StorageException;
import net.snowflake.client.jdbc.internal.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.IcebergFileTransferAgent;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;

/** Handles uploading files to the Snowflake Streaming Ingest Storage */
@VisibleForTesting
public class InternalStage implements IStorage {
  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * Object mapper for parsing the client/configure response to Jackson version the same as
   * jdbc.internal.fasterxml.jackson. We need two different versions of ObjectMapper because {@link
   * SnowflakeFileTransferAgent#getFileTransferMetadatas(net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode)}
   * expects a different version of json object than {@link StreamingIngestResponse}. TODO:
   * SNOW-1493470 Align Jackson version
   */
  private static final net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
      parseConfigureResponseMapper =
          new net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper();

  private static final long REFRESH_THRESHOLD_IN_MS =
      TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

  // Stage credential refresh interval, currently the token will expire in 1hr for GCS and 2hr for
  // AWS/Azure, so set it a bit smaller than 1hr
  private static final Duration refreshDuration = Duration.ofMinutes(58);
  private static Instant prevRefresh = Instant.EPOCH;

  private static final Logging logger = new Logging(InternalStage.class);

  private final IStorageManager owningManager;
  private final String clientName;
  private final String clientPrefix;
  private final TableRef tableRef;
  private final int maxUploadRetries;

  // Proxy parameters that we set while calling the Snowflake JDBC to upload the streams
  private final Properties proxyProperties;
  private final boolean useIcebergFileTransferAgent;

  private FileLocationInfo fileLocationInfo;
  private SnowflakeFileTransferMetadataWithAge fileTransferMetadataWithAge;

  /**
   * Default constructor
   *
   * @param owningManager the storage manager owning this storage
   * @param clientName The client name
   * @param clientPrefix client prefix
   * @param tableRef
   * @param useIcebergFileTransferAgent Whether to use the local copy of the JDBC file transfer
   *     agent so that the etag of the uploaded file can be registered to snowflake
   * @param fileLocationInfo The file location information from open channel response
   * @param maxUploadRetries The maximum number of retries to attempt
   */
  InternalStage(
      IStorageManager owningManager,
      String clientName,
      String clientPrefix,
      TableRef tableRef,
      boolean useIcebergFileTransferAgent,
      FileLocationInfo fileLocationInfo,
      int maxUploadRetries)
      throws SnowflakeSQLException, IOException {
    this(
        owningManager,
        clientName,
        clientPrefix,
        tableRef,
        useIcebergFileTransferAgent,
        (SnowflakeFileTransferMetadataWithAge) null,
        maxUploadRetries);
    Utils.assertStringNotNullOrEmpty("client prefix", clientPrefix);
    setFileLocationInfo(fileLocationInfo);
  }

  /**
   * Constructor for TESTING that takes SnowflakeFileTransferMetadataWithAge as input
   *
   * @param owningManager the storage manager owning this storage
   * @param clientName the client name
   * @param clientPrefix
   * @param tableRef
   * @param useIcebergFileTransferAgent
   * @param testMetadata SnowflakeFileTransferMetadataWithAge to test with
   * @param maxUploadRetries the maximum number of retries to attempt
   */
  InternalStage(
      IStorageManager owningManager,
      String clientName,
      String clientPrefix,
      TableRef tableRef,
      boolean useIcebergFileTransferAgent,
      SnowflakeFileTransferMetadataWithAge testMetadata,
      int maxUploadRetries)
      throws SnowflakeSQLException, IOException {
    this.owningManager = owningManager;
    this.clientName = clientName;
    this.clientPrefix = clientPrefix;
    this.tableRef = tableRef;
    this.maxUploadRetries = maxUploadRetries;
    this.useIcebergFileTransferAgent = useIcebergFileTransferAgent;
    this.proxyProperties = generateProxyPropertiesForJDBC();
    this.fileTransferMetadataWithAge = testMetadata;
  }

  private Optional<IcebergPostUploadMetadata> putRemote(BlobPath blobPath, byte[] data) {
    int retryCount = 0;

    while (true) {
      try {
        // Proactively refresh the credential if it's going to expire, to avoid the token expiration
        // error from JDBC which confuses customer. Also refresh on the first retry.
        if (Instant.now().isAfter(prevRefresh.plus(refreshDuration)) || retryCount == 1) {
          refreshSnowflakeMetadata(false /* force */);
          if (this.useIcebergFileTransferAgent) {
            BlobPath refreshedPath =
                this.owningManager.generateBlobPath(this.tableRef.fullyQualifiedName);
            logger.logInfo(
                "Refreshed Iceberg file upload path, client={}, tableRef={}, originalPath={},"
                    + " refreshedFilepath={}",
                clientName,
                tableRef,
                blobPath.uploadPath,
                refreshedPath.uploadPath);
            blobPath = refreshedPath;
          }
        }

        /*
         * Since we can have multiple calls to putRemote in parallel and because the metadata
         * includes the file path we use a copy for the upload to prevent us from using the wrong
         * file path.
         */
        SnowflakeFileTransferMetadataV1 fileTransferMetadataCopy =
            this.fileTransferMetadataWithAge.fileTransferMetadata.isForOneFile()
                ? this.fetchSignedURL(blobPath.uploadPath)
                : copyFileTransferMetadata(
                    this.fileTransferMetadataWithAge.fileTransferMetadata, blobPath.uploadPath);

        InputStream inStream = new ByteArrayInputStream(data);

        if (this.useIcebergFileTransferAgent) {
          return Optional.of(
              new IcebergPostUploadMetadata(
                  IcebergFileTransferAgent.uploadWithoutConnection(
                      fileTransferMetadataCopy,
                      inStream,
                      proxyProperties,
                      clientName,
                      clientPrefix,
                      blobPath.uploadPath),
                  blobPath));
        } else {
          SnowflakeFileTransferAgent.uploadWithoutConnection(
              SnowflakeFileTransferConfig.Builder.newInstance()
                  .setSnowflakeFileTransferMetadata(fileTransferMetadataCopy)
                  .setSilentException(true)
                  .setUploadStream(inStream)
                  .setRequireCompress(false)
                  .setOcspMode(OCSPMode.FAIL_OPEN)
                  .setStreamingIngestClientKey(this.clientPrefix)
                  .setStreamingIngestClientName(this.clientName)
                  .setProxyProperties(this.proxyProperties)
                  .setDestFileName(blobPath.uploadPath)
                  .build());
          return Optional.empty();
        }
      } catch (SQLException | IOException | StorageException e) {
        retryCount++;
        if (retryCount > maxUploadRetries) {
          logger.logError(
              "Failed to upload to stage, retry attempts exhausted ({}), client={}, message={}",
              maxUploadRetries,
              clientName,
              e.getMessage());
          throw new SFException(e, ErrorCode.IO_ERROR);
        }
        StreamingIngestUtils.sleepForRetry(retryCount);
        logger.logInfo(
            "Retrying upload, attempt {}/{} msg: {}, stackTrace:{}",
            retryCount,
            maxUploadRetries,
            e.getMessage(),
            getStackTrace(e));
      } catch (Exception e) {
        logger.logError(
            "Failed to upload to stage, unexpected exception, msg: {}, stackTrace:{}",
            e.getMessage(),
            e.getMessage());
        throw new SFException(e, ErrorCode.INTERNAL_ERROR);
      }
    }
  }

  /**
   * Gets new stage credentials and other metadata from Snowflake. Synchronized to prevent multiple
   * calls to putRemote from trying to refresh at the same time
   *
   * @param force if true will ignore REFRESH_THRESHOLD and force metadata refresh
   * @return refreshed metadata
   * @throws SnowflakeSQLException
   * @throws IOException
   */
  synchronized SnowflakeFileTransferMetadataWithAge refreshSnowflakeMetadata(boolean force)
      throws SnowflakeSQLException, IOException {
    if (!force
        && fileTransferMetadataWithAge != null
        && fileTransferMetadataWithAge.timestamp.isPresent()
        && fileTransferMetadataWithAge.timestamp.get()
            > System.currentTimeMillis() - REFRESH_THRESHOLD_IN_MS) {
      return fileTransferMetadataWithAge;
    }

    logger.logInfo(
        "Refresh Snowflake metadata, client={} force={} tableRef={}", clientName, force, tableRef);

    FileLocationInfo location =
        this.owningManager.getRefreshedLocation(this.tableRef, Optional.empty());
    setFileLocationInfo(location);
    return this.fileTransferMetadataWithAge;
  }

  private synchronized void setFileLocationInfo(FileLocationInfo fileLocationInfo)
      throws SnowflakeSQLException, IOException {
    this.fileTransferMetadataWithAge = createFileTransferMetadataWithAge(fileLocationInfo);
    this.fileLocationInfo = fileLocationInfo;
  }

  static SnowflakeFileTransferMetadataWithAge createFileTransferMetadataWithAge(
      FileLocationInfo fileLocationInfo)
      throws JsonProcessingException,
          net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException,
          SnowflakeSQLException {
    final SnowflakeFileTransferMetadataWithAge fileTransferMetadataWithAge;

    if (fileLocationInfo
        .getLocationType()
        .replaceAll(
            "^[\"]|[\"]$", "") // Replace the first and last character if they're double quotes
        .equals(StageInfo.StageType.LOCAL_FS.name())) {
      fileTransferMetadataWithAge =
          new SnowflakeFileTransferMetadataWithAge(
              fileLocationInfo
                  .getLocation()
                  .replaceAll(
                      "^[\"]|[\"]$",
                      ""), // Replace the first and last character if they're double quotes
              Optional.of(System.currentTimeMillis()));
    } else {
      fileTransferMetadataWithAge =
          new SnowflakeFileTransferMetadataWithAge(
              (SnowflakeFileTransferMetadataV1)
                  SnowflakeFileTransferAgent.getFileTransferMetadatas(
                          parseFileLocationInfo(fileLocationInfo))
                      .get(0),
              Optional.of(System.currentTimeMillis()));
    }

    /*
    this is not used thus commented out, but technically we are not copying over some info from fileLocationInfo
    to fileTransferMetadata.

    String presignedUrl = fileLocationInfo.getPresignedUrl();
    if (presignedUrl != null) {
      fileTransferMetadataWithAge.fileTransferMetadata.setPresignedUrl(presignedUrl);
      String[] parts = presignedUrl.split("/");
      fileTransferMetadataWithAge.fileTransferMetadata.setPresignedUrlFileName(parts[parts.length - 1]);
    }
    */

    prevRefresh = Instant.now();
    return fileTransferMetadataWithAge;
  }

  /**
   * GCS requires a signed url per file. We need to fetch this from the server for each put
   *
   * @throws SnowflakeSQLException
   * @throws IOException
   */
  SnowflakeFileTransferMetadataV1 fetchSignedURL(String fileName)
      throws SnowflakeSQLException, IOException {

    FileLocationInfo location =
        this.owningManager.getRefreshedLocation(this.tableRef, Optional.of(fileName));

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(parseFileLocationInfo(location))
                .get(0);
    // Transfer agent trims path for fileName
    metadata.setPresignedUrlFileName(fileName);
    return metadata;
  }

  static net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode
      parseFileLocationInfo(FileLocationInfo fileLocationInfo)
          throws JsonProcessingException,
              net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException {
    JsonNode fileLocationInfoNode = mapper.valueToTree(fileLocationInfo);

    // Currently there are a few mismatches between the client/configure response and what
    // SnowflakeFileTransferAgent expects

    ObjectNode node = mapper.createObjectNode();
    node.putObject("data");
    ObjectNode dataNode = (ObjectNode) node.get("data");
    dataNode.set("stageInfo", fileLocationInfoNode);

    // JDBC expects this field which maps to presignedFileUrlName.  We will set this later
    dataNode.putArray("src_locations").add("placeholder");

    // use String as intermediate object to avoid Jackson version mismatch
    // TODO: SNOW-1493470 Align Jackson version
    String responseString = mapper.writeValueAsString(node);
    return parseConfigureResponseMapper.readTree(responseString);
  }

  /** Upload file to internal stage */
  public Optional<IcebergPostUploadMetadata> put(BlobPath blobPath, byte[] blob) {
    if (this.isLocalFS()) {
      putLocal(this.fileTransferMetadataWithAge.localLocation, blobPath.fileRegistrationPath, blob);
      return Optional.empty();
    } else {
      return putRemote(blobPath, blob);
    }
  }

  boolean isLocalFS() {
    return this.fileTransferMetadataWithAge.isLocalFS;
  }

  /**
   * Upload file to local internal stage with previously cached credentials.
   *
   * @param fullFilePath
   * @param data
   */
  @VisibleForTesting
  static void putLocal(String stageLocation, String fullFilePath, byte[] data) {
    if (fullFilePath == null || fullFilePath.isEmpty() || fullFilePath.endsWith("/")) {
      throw new SFException(ErrorCode.BLOB_UPLOAD_FAILURE);
    }

    InputStream input = new ByteArrayInputStream(data);
    try {
      File destFile = Paths.get(stageLocation, fullFilePath).toFile();
      FileUtils.copyInputStreamToFile(input, destFile);
    } catch (Exception ex) {
      throw new SFException(ex, ErrorCode.BLOB_UPLOAD_FAILURE);
    }
  }

  FileLocationInfo getFileLocationInfo() {
    return this.fileLocationInfo;
  }

  private SnowflakeFileTransferMetadataV1 copyFileTransferMetadata(
      SnowflakeFileTransferMetadataV1 fileTransferMetadata, String uploadPath)
      throws SnowflakeSQLException, IOException {
    // TODO: SNOW-1969309 investigate if this code is thread-safe
    RemoteStoreFileEncryptionMaterial encryptionMaterial =
        fileTransferMetadata.getEncryptionMaterial();

    return new SnowflakeFileTransferMetadataV1(
        fileTransferMetadata.getPresignedUrl(),
        uploadPath,
        encryptionMaterial != null ? encryptionMaterial.getQueryStageMasterKey() : null,
        encryptionMaterial != null ? encryptionMaterial.getQueryId() : null,
        encryptionMaterial != null ? encryptionMaterial.getSmkId() : null,
        fileTransferMetadata.getCommandType(),
        fileTransferMetadata.getStageInfo());
  }

  @VisibleForTesting
  public void setEmptyIcebergFileTransferMetadataWithAge() {
    this.fileTransferMetadataWithAge.timestamp = Optional.empty();
    this.fileTransferMetadataWithAge.fileTransferMetadata.setPresignedUrlFileName("");
    this.fileTransferMetadataWithAge.fileTransferMetadata.getStageInfo().setPresignedUrl("");

    Map<?, ?> stageCredentials =
        this.fileTransferMetadataWithAge.fileTransferMetadata.getStageInfo().getCredentials();
    HashMap<String, String> invalidStageCredentials = new HashMap<>();
    for (Object key : stageCredentials.keySet()) {
      invalidStageCredentials.put((String) key, "");
    }
    this.fileTransferMetadataWithAge
        .fileTransferMetadata
        .getStageInfo()
        .setCredentials(invalidStageCredentials);
  }
}

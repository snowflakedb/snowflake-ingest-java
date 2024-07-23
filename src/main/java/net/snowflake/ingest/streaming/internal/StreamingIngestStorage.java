/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
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
import java.time.Duration;
import java.time.Instant;
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
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;

/** Handles uploading files to the Snowflake Streaming Ingest Storage */
class StreamingIngestStorage<T, TLocation> {
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

  private static final Logging logger = new Logging(StreamingIngestStorage.class);

  /**
   * Wrapper class containing SnowflakeFileTransferMetadata and the timestamp at which the metadata
   * was refreshed
   */
  static class SnowflakeFileTransferMetadataWithAge {
    SnowflakeFileTransferMetadataV1 fileTransferMetadata;
    private final boolean isLocalFS;
    private final String localLocation;

    /* Do not always know the age of the metadata, so we use the empty
    state to record unknown age.
     */
    Optional<Long> timestamp;

    SnowflakeFileTransferMetadataWithAge(
        SnowflakeFileTransferMetadataV1 fileTransferMetadata, Optional<Long> timestamp) {
      this.isLocalFS = false;
      this.fileTransferMetadata = fileTransferMetadata;
      this.timestamp = timestamp;
      this.localLocation = null;
    }

    SnowflakeFileTransferMetadataWithAge(String localLocation, Optional<Long> timestamp) {
      this.isLocalFS = true;
      this.localLocation = localLocation;
      this.timestamp = timestamp;
    }
  }

  private SnowflakeFileTransferMetadataWithAge fileTransferMetadataWithAge;
  private final IStorageManager<T, TLocation> owningManager;
  private final TLocation location;
  private final String clientName;

  private final int maxUploadRetries;

  // Proxy parameters that we set while calling the Snowflake JDBC to upload the streams
  private final Properties proxyProperties;

  /**
   * Default constructor
   *
   * @param owningManager the storage manager owning this storage
   * @param clientName The client name
   * @param fileLocationInfo The file location information from open channel response
   * @param location A reference to the target location
   * @param maxUploadRetries The maximum number of retries to attempt
   */
  StreamingIngestStorage(
      IStorageManager<T, TLocation> owningManager,
      String clientName,
      FileLocationInfo fileLocationInfo,
      TLocation location,
      int maxUploadRetries)
      throws SnowflakeSQLException, IOException {
    this(
        owningManager,
        clientName,
        (SnowflakeFileTransferMetadataWithAge) null,
        location,
        maxUploadRetries);
    createFileTransferMetadataWithAge(fileLocationInfo);
  }

  /**
   * Constructor for TESTING that takes SnowflakeFileTransferMetadataWithAge as input
   *
   * @param owningManager the storage manager owning this storage
   * @param clientName the client name
   * @param testMetadata SnowflakeFileTransferMetadataWithAge to test with
   * @param location A reference to the target location
   * @param maxUploadRetries the maximum number of retries to attempt
   */
  StreamingIngestStorage(
      IStorageManager<T, TLocation> owningManager,
      String clientName,
      SnowflakeFileTransferMetadataWithAge testMetadata,
      TLocation location,
      int maxUploadRetries)
      throws SnowflakeSQLException, IOException {
    this.owningManager = owningManager;
    this.clientName = clientName;
    this.maxUploadRetries = maxUploadRetries;
    this.proxyProperties = generateProxyPropertiesForJDBC();
    this.location = location;
    this.fileTransferMetadataWithAge = testMetadata;
  }

  /**
   * Upload file to internal stage with previously cached credentials. Will refetch and cache
   * credentials if they've expired.
   *
   * @param fullFilePath Full file name to be uploaded
   * @param data Data string to be uploaded
   */
  void putRemote(String fullFilePath, byte[] data) throws SnowflakeSQLException, IOException {
    this.putRemote(fullFilePath, data, 0);
  }

  private void putRemote(String fullFilePath, byte[] data, int retryCount)
      throws SnowflakeSQLException, IOException {
    SnowflakeFileTransferMetadataV1 fileTransferMetadataCopy;
    if (this.fileTransferMetadataWithAge.fileTransferMetadata.isForOneFile()) {
      fileTransferMetadataCopy = this.fetchSignedURL(fullFilePath);
    } else {
      // Set file path to be uploaded
      SnowflakeFileTransferMetadataV1 fileTransferMetadata =
          fileTransferMetadataWithAge.fileTransferMetadata;

      /*
      Since we can have multiple calls to putRemote in parallel and because the metadata includes the file path
      we use a copy for the upload to prevent us from using the wrong file path.
       */
      fileTransferMetadataCopy =
          new SnowflakeFileTransferMetadataV1(
              fileTransferMetadata.getPresignedUrl(),
              fullFilePath,
              fileTransferMetadata.getEncryptionMaterial() != null
                  ? fileTransferMetadata.getEncryptionMaterial().getQueryStageMasterKey()
                  : null,
              fileTransferMetadata.getEncryptionMaterial() != null
                  ? fileTransferMetadata.getEncryptionMaterial().getQueryId()
                  : null,
              fileTransferMetadata.getEncryptionMaterial() != null
                  ? fileTransferMetadata.getEncryptionMaterial().getSmkId()
                  : null,
              fileTransferMetadata.getCommandType(),
              fileTransferMetadata.getStageInfo());
    }
    InputStream inStream = new ByteArrayInputStream(data);

    try {
      // Proactively refresh the credential if it's going to expire, to avoid the token expiration
      // error from JDBC which confuses customer
      if (Instant.now().isAfter(prevRefresh.plus(refreshDuration))) {
        refreshSnowflakeMetadata();
      }

      SnowflakeFileTransferAgent.uploadWithoutConnection(
          SnowflakeFileTransferConfig.Builder.newInstance()
              .setSnowflakeFileTransferMetadata(fileTransferMetadataCopy)
              .setUploadStream(inStream)
              .setRequireCompress(false)
              .setOcspMode(OCSPMode.FAIL_OPEN)
              .setStreamingIngestClientKey(this.owningManager.getClientPrefix())
              .setStreamingIngestClientName(this.clientName)
              .setProxyProperties(this.proxyProperties)
              .setDestFileName(fullFilePath)
              .build());
    } catch (Exception e) {
      if (retryCount == 0) {
        // for the first exception, we always perform a metadata refresh.
        this.refreshSnowflakeMetadata();
      }
      if (retryCount >= maxUploadRetries) {
        logger.logError(
            "Failed to upload to stage, retry attempts exhausted ({}), client={}, message={}",
            maxUploadRetries,
            clientName,
            e.getMessage());
        throw new SFException(e, ErrorCode.IO_ERROR);
      }
      retryCount++;
      StreamingIngestUtils.sleepForRetry(retryCount);
      logger.logInfo(
          "Retrying upload, attempt {}/{} msg: {}, stackTrace:{}",
          retryCount,
          maxUploadRetries,
          e.getMessage(),
          getStackTrace(e));
      this.putRemote(fullFilePath, data, retryCount);
    }
  }

  SnowflakeFileTransferMetadataWithAge refreshSnowflakeMetadata()
      throws SnowflakeSQLException, IOException {
    logger.logInfo("Refresh Snowflake metadata, client={}", clientName);
    return refreshSnowflakeMetadata(false);
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

    FileLocationInfo location =
        this.owningManager.getRefreshedLocation(this.location, Optional.empty());
    return createFileTransferMetadataWithAge(location);
  }

  private SnowflakeFileTransferMetadataWithAge createFileTransferMetadataWithAge(
      FileLocationInfo fileLocationInfo)
      throws JsonProcessingException,
          net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException,
          SnowflakeSQLException {
    Utils.assertStringNotNullOrEmpty("client prefix", this.owningManager.getClientPrefix());

    if (fileLocationInfo
        .getLocationType()
        .replaceAll(
            "^[\"]|[\"]$", "") // Replace the first and last character if they're double quotes
        .equals(StageInfo.StageType.LOCAL_FS.name())) {
      this.fileTransferMetadataWithAge =
          new SnowflakeFileTransferMetadataWithAge(
              fileLocationInfo
                  .getLocation()
                  .replaceAll(
                      "^[\"]|[\"]$",
                      ""), // Replace the first and last character if they're double quotes
              Optional.of(System.currentTimeMillis()));
    } else {
      this.fileTransferMetadataWithAge =
          new SnowflakeFileTransferMetadataWithAge(
              (SnowflakeFileTransferMetadataV1)
                  SnowflakeFileTransferAgent.getFileTransferMetadatas(
                          parseFileLocationInfo(fileLocationInfo))
                      .get(0),
              Optional.of(System.currentTimeMillis()));
    }

    prevRefresh = Instant.now();
    return this.fileTransferMetadataWithAge;
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
        this.owningManager.getRefreshedLocation(this.location, Optional.of(fileName));

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(parseFileLocationInfo(location))
                .get(0);
    // Transfer agent trims path for fileName
    metadata.setPresignedUrlFileName(fileName);
    return metadata;
  }

  private net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode
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

  /**
   * Upload file to internal stage
   *
   * @param filePath
   * @param blob
   */
  void put(String filePath, byte[] blob) {
    if (this.isLocalFS()) {
      putLocal(filePath, blob);
    } else {
      try {
        putRemote(filePath, blob);
      } catch (SnowflakeSQLException | IOException e) {
        throw new SFException(e, ErrorCode.BLOB_UPLOAD_FAILURE);
      }
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
  void putLocal(String fullFilePath, byte[] data) {
    if (fullFilePath == null || fullFilePath.isEmpty() || fullFilePath.endsWith("/")) {
      throw new SFException(ErrorCode.BLOB_UPLOAD_FAILURE);
    }

    InputStream input = new ByteArrayInputStream(data);
    try {
      String stageLocation = this.fileTransferMetadataWithAge.localLocation;
      File destFile = Paths.get(stageLocation, fullFilePath).toFile();
      FileUtils.copyInputStreamToFile(input, destFile);
    } catch (Exception ex) {
      throw new SFException(ex, ErrorCode.BLOB_UPLOAD_FAILURE);
    }
  }
}

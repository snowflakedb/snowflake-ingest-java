/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CLIENT_CONFIGURE;
import static net.snowflake.ingest.streaming.internal.StreamingIngestUtils.executeWithRetries;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;
import static net.snowflake.ingest.utils.HttpUtil.generateProxyPropertiesForJDBC;
import static net.snowflake.ingest.utils.Utils.getStackTrace;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeFileTransferConfig;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;

/** Handles uploading files to the Snowflake Streaming Ingest Storage */
class UploadService {
  protected static final Logging logger = new Logging(StreamingIngestStage.class);

  protected static final long REFRESH_THRESHOLD_IN_MS =
      TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

  /**
   * Wrapper class containing SnowflakeFileTransferMetadata and the timestamp at which the metadata
   * was refreshed
   */
  static class SnowflakeFileTransferMetadataWithAge {
    SnowflakeFileTransferMetadataV1 fileTransferMetadata;
    protected final boolean isLocalFS;
    protected final String localLocation;

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

  protected static class MapStatusGetter<T> implements Function<T, Long> {
    public MapStatusGetter() {}

    public Long apply(T input) {
      try {
        return ((Integer) ((Map<String, Object>) input).get("status_code")).longValue();
      } catch (Exception e) {
        throw new SFException(ErrorCode.INTERNAL_ERROR, "failed to get status_code from response");
      }
    }
  }

  protected static final MapStatusGetter statusGetter = new MapStatusGetter();

  protected static final ObjectMapper mapper = new ObjectMapper();

  protected SnowflakeFileTransferMetadataWithAge fileTransferMetadataWithAge;
  protected final CloseableHttpClient httpClient;
  protected final RequestBuilder requestBuilder;
  protected final String role;
  protected final String clientName;
  protected String clientPrefix;
  protected final String configureEndpoint;

  private final int maxUploadRetries;

  // Proxy parameters that we set while calling the Snowflake JDBC to upload the streams
  private final Properties proxyProperties;

  /**
   * Constructor of abstract cloud storage use to upload file to external volume
   *
   * @param role Snowflake role used by the Client
   * @param httpClient http client reference
   * @param requestBuilder request builder to build the HTTP request
   * @param clientName the client name
   * @param configureEndpoint endpoint for configuration call
   * @param maxUploadRetries maximum time of upload retries
   */
  UploadService(
      String role,
      CloseableHttpClient httpClient,
      RequestBuilder requestBuilder,
      String clientName,
      String configureEndpoint,
      int maxUploadRetries) {
    this.httpClient = httpClient;
    this.role = role;
    this.requestBuilder = requestBuilder;
    this.clientName = clientName;
    this.configureEndpoint = configureEndpoint;
    this.proxyProperties = generateProxyPropertiesForJDBC();
    this.maxUploadRetries = maxUploadRetries;
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
      SnowflakeFileTransferAgent.uploadWithoutConnection(
          SnowflakeFileTransferConfig.Builder.newInstance()
              .setSnowflakeFileTransferMetadata(fileTransferMetadataCopy)
              .setUploadStream(inStream)
              .setRequireCompress(false)
              .setOcspMode(OCSPMode.FAIL_OPEN)
              .setStreamingIngestClientKey(this.clientPrefix)
              .setStreamingIngestClientName(this.clientName)
              .setProxyProperties(this.proxyProperties)
              .setDestFileName(fullFilePath)
              .build());
    } catch (Exception e) {
      if (retryCount == 0) {
        // for the first exception, we always perform a metadata refresh.
        logger.logInfo(
            "Stage metadata need to be refreshed due to upload error: {} on first retry attempt",
            e.getMessage());
        this.refreshCloudStorageMetadata();
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

  SnowflakeFileTransferMetadataWithAge refreshCloudStorageMetadata()
      throws SnowflakeSQLException, IOException {
    logger.logInfo("Refresh Snowflake metadata, client={}", clientName);
    return refreshCloudStorageMetadata(false);
  }

  /**
   * Creates a client-specific prefix that will be also part of the files registered by this client.
   * The prefix will include a server-side generated string and the GlobalID of the deployment the
   * client is registering blobs to. The latter (deploymentId) is needed in order to guarantee that
   * blob filenames are unique across deployments even with replication enabled.
   *
   * @param response the client/configure response from the server
   * @return the client prefix.
   */
  protected String createClientPrefix(final JsonNode response) {
    final String prefix = response.get("prefix").textValue();
    final String deploymentId =
        response.has("deployment_id") ? "_" + response.get("deployment_id").longValue() : "";
    return prefix + deploymentId;
  }

  boolean isLocalFS() {
    return this.fileTransferMetadataWithAge.isLocalFS;
  }

  /** Get the server generated unique prefix for this client */
  String getClientPrefix() {
    return this.clientPrefix;
  }

  /**
   * GCS requires a signed url per file. We need to fetch this from the server for each put
   *
   * @throws SnowflakeSQLException
   * @throws IOException
   */
  SnowflakeFileTransferMetadataV1 fetchSignedURL(String fileName)
      throws SnowflakeSQLException, IOException {

    Map<Object, Object> payload = getConfigurePayload();
    payload.put("file_name", fileName);
    Map<String, Object> response = this.makeConfigureCall(payload);

    JsonNode responseNode = this.parseConfigureResponse(response);

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(responseNode).get(0);
    // Transfer agent trims path for fileName
    metadata.setPresignedUrlFileName(fileName);
    return metadata;
  }

  protected JsonNode parseConfigureResponse(Map<String, Object> response) {
    JsonNode responseNode = mapper.valueToTree(response);

    // Currently there are a few mismatches between the client/configure response and what
    // SnowflakeFileTransferAgent expects
    ObjectNode mutable = (ObjectNode) responseNode;
    mutable.putObject("data");
    ObjectNode dataNode = (ObjectNode) mutable.get("data");
    dataNode.set("stageInfo", responseNode.get("stage_location"));

    // JDBC expects this field which maps to presignedFileUrlName.  We will set this later
    dataNode.putArray("src_locations").add("placeholder");
    return responseNode;
  }

  /**
   * Gets new storage credentials and other metadata from Snowflake. Synchronized to prevent
   * multiple calls to putRemote from trying to refresh at the same time
   *
   * @param force if true will ignore REFRESH_THRESHOLD and force metadata refresh
   * @return refreshed metadata
   * @throws SnowflakeSQLException
   * @throws IOException
   */
  synchronized SnowflakeFileTransferMetadataWithAge refreshCloudStorageMetadata(boolean force)
      throws SnowflakeSQLException, IOException {
    if (!force
        && fileTransferMetadataWithAge != null
        && fileTransferMetadataWithAge.timestamp.isPresent()
        && fileTransferMetadataWithAge.timestamp.get()
            > System.currentTimeMillis() - REFRESH_THRESHOLD_IN_MS) {
      return fileTransferMetadataWithAge;
    }

    Map<Object, Object> payload = getConfigurePayload();
    Map<String, Object> response = this.makeConfigureCall(payload);

    JsonNode responseNode = this.parseConfigureResponse(response);
    // Do not change the prefix everytime we have to refresh credentials
    if (Utils.isNullOrEmpty(this.clientPrefix)) {
      this.clientPrefix = createClientPrefix(responseNode);
    }
    Utils.assertStringNotNullOrEmpty("client prefix", this.clientPrefix);

    if (responseNode
        .get("data")
        .get("stageInfo")
        .get("locationType")
        .toString()
        .replaceAll(
            "^[\"]|[\"]$", "") // Replace the first and last character if they're double quotes
        .equals(StageInfo.StageType.LOCAL_FS.name())) {
      this.fileTransferMetadataWithAge =
          new SnowflakeFileTransferMetadataWithAge(
              responseNode
                  .get("data")
                  .get("stageInfo")
                  .get("location")
                  .toString()
                  .replaceAll(
                      "^[\"]|[\"]$",
                      ""), // Replace the first and last character if they're double quotes
              Optional.of(System.currentTimeMillis()));
    } else {
      this.fileTransferMetadataWithAge =
          new SnowflakeFileTransferMetadataWithAge(
              (SnowflakeFileTransferMetadataV1)
                  SnowflakeFileTransferAgent.getFileTransferMetadatas(responseNode).get(0),
              Optional.of(System.currentTimeMillis()));
    }
    return this.fileTransferMetadataWithAge;
  }

  private Map<String, Object> makeConfigureCall(Map<Object, Object> payload) throws IOException {
    try {
      Map<String, Object> response =
          executeWithRetries(
              Map.class,
              this.configureEndpoint,
              mapper.writeValueAsString(payload),
              "client configure",
              STREAMING_CLIENT_CONFIGURE,
              httpClient,
              requestBuilder,
              statusGetter);

      // Check for Snowflake specific response code
      if (!response.get("status_code").equals((int) RESPONSE_SUCCESS)) {
        throw new SFException(
            ErrorCode.CLIENT_CONFIGURE_FAILURE, response.get("message").toString());
      }
      return response;
    } catch (IngestResponseException e) {
      throw new SFException(e, ErrorCode.CLIENT_CONFIGURE_FAILURE, e.getMessage());
    }
  }

  protected abstract Map<Object, Object> getConfigurePayload();

  /**
   * Upload file
   *
   * @param filePath
   * @param blob
   */
  abstract void put(String filePath, byte[] blob);
}

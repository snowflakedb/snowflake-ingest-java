/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CLIENT_CONFIGURE;
import static net.snowflake.ingest.utils.Constants.CLIENT_CONFIGURE_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeFileTransferConfig;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.DeserializationFeature;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.ServiceResponseHandler;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.http.client.HttpClient;

/** Handles uploading files to the Snowflake Streaming Ingest Stage */
class StreamingIngestStage {
  private static final Logging logger = new Logging(StreamingIngestStage.class);

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final long REFRESH_THRESHOLD_IN_MS =
      TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
  static final int MAX_RETRY_COUNT = 1;
  private String clientPrefix;

  /**
   * Wrapper class containing SnowflakeFileTransferMetadata and the timestamp at which the metadata
   * was refreshed
   */
  static class SnowflakeFileTransferMetadataWithAge {
    SnowflakeFileTransferMetadataV1 fileTransferMetadata;
    private final boolean isLocalFS;
    private final String localLocation;

    /* Do do not always know the age of the metadata, so we use the empty
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
  private final HttpClient httpClient;
  private final RequestBuilder requestBuilder;
  private final String role;
  private final String clientName;

  StreamingIngestStage(
      boolean isTestMode,
      String role,
      HttpClient httpClient,
      RequestBuilder requestBuilder,
      String clientName)
      throws SnowflakeSQLException, IOException {
    this.httpClient = httpClient;
    this.role = role;
    this.requestBuilder = requestBuilder;
    this.clientName = clientName;
    /*
    All integer numbers will be deserialized as longs.
    If this is false Jackson will deserialize to int or long based on size
    */
    this.mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);

    if (!isTestMode) {
      refreshSnowflakeMetadata();
    }
  }

  /**
   * Constructor for TESTING that takes SnowflakeFileTransferMetadataWithAge as input
   *
   * @param isTestMode must be true
   * @param role Snowflake role used by the Client
   * @param httpClient http client reference
   * @param requestBuilder request builder to build the HTTP request
   * @param clientName the client name
   * @param testMetadata SnowflakeFileTransferMetadataWithAge to test with
   */
  StreamingIngestStage(
      boolean isTestMode,
      String role,
      HttpClient httpClient,
      RequestBuilder requestBuilder,
      String clientName,
      SnowflakeFileTransferMetadataWithAge testMetadata) {
    if (!isTestMode) {
      throw new SFException(ErrorCode.INTERNAL_ERROR);
    }
    this.httpClient = httpClient;
    this.role = role;
    this.requestBuilder = requestBuilder;
    this.clientName = clientName;
    this.fileTransferMetadataWithAge = testMetadata;
    this.mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
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
    if (this.fileTransferMetadataWithAge.fileTransferMetadata.getStageInfo().getStageType()
        == StageInfo.StageType.GCS) {
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
              .build());
    } catch (NullPointerException npe) {
      // TODO SNOW-350701 Update JDBC driver to throw a reliable token expired error
      if (retryCount >= MAX_RETRY_COUNT) {
        throw npe;
      }
      this.refreshSnowflakeMetadata();
      this.putRemote(fullFilePath, data, ++retryCount);
    } catch (Exception e) {
      throw new SFException(e, ErrorCode.IO_ERROR);
    }
  }

  SnowflakeFileTransferMetadataWithAge refreshSnowflakeMetadata()
      throws SnowflakeSQLException, IOException {
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

    Map<Object, Object> payload = new HashMap<>();
    payload.put("role", this.role);
    Map<String, Object> response = this.makeClientConfigureCall(payload);

    JsonNode responseNode = this.parseClientConfigureResponse(response);
    // Do not change the prefix everytime we have to refresh credentials
    if (Utils.isNullOrEmpty(this.clientPrefix)) {
      this.clientPrefix = responseNode.get("prefix").textValue();
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

  /**
   * GCS requires a signed url per file. We need to fetch this from the server for each put
   *
   * @throws SnowflakeSQLException
   * @throws IOException
   */
  SnowflakeFileTransferMetadataV1 fetchSignedURL(String fileName)
      throws SnowflakeSQLException, IOException {

    Map<Object, Object> payload = new HashMap<>();
    payload.put("role", this.role);
    payload.put("file_name", fileName);
    Map<String, Object> response = this.makeClientConfigureCall(payload);

    JsonNode responseNode = this.parseClientConfigureResponse(response);

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(responseNode).get(0);
    // Transfer agent trims path for fileName
    metadata.setPresignedUrlFileName(fileName);
    return metadata;
  }

  private JsonNode parseClientConfigureResponse(Map<String, Object> response) {
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

  private Map<String, Object> makeClientConfigureCall(Map<Object, Object> payload)
      throws IOException {
    try {
      Map<String, Object> response =
          ServiceResponseHandler.unmarshallStreamingIngestResponse(
              httpClient.execute(
                  requestBuilder.generateStreamingIngestPostRequest(
                      payload, CLIENT_CONFIGURE_ENDPOINT, "client configure")),
              Map.class,
              STREAMING_CLIENT_CONFIGURE);

      // Check for Snowflake specific response code
      if (!response.get("status_code").equals((int) RESPONSE_SUCCESS)) {
        throw new SFException(
            ErrorCode.CLIENT_CONFIGURE_FAILURE, response.get("message").toString());
      }
      return response;
    } catch (IngestResponseException e) {
      throw new SFException(e, ErrorCode.CLIENT_CONFIGURE_FAILURE);
    }
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
      Paths.get(stageLocation, fullFilePath);
      File destFile = Paths.get(stageLocation, fullFilePath).toFile();
      FileUtils.copyInputStreamToFile(input, destFile);
    } catch (Exception ex) {
      throw new SFException(ex, ErrorCode.BLOB_UPLOAD_FAILURE);
    }
  }

  /** Get the server generated unique prefix for this client */
  String getClientPrefix() {
    return this.clientPrefix;
  }
}

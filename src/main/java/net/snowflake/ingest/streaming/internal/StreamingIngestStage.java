/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.Constants.CLIENT_CONFIGURE_ENDPOINT;

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.*;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.client.utils.URIBuilder;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** Handles uploading files to the Snowflake Streaming Ingest Stage */
public class StreamingIngestStage {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final long REFRESH_THRESHOLD_IN_MS =
      TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
  static final int MAX_RETRY_COUNT = 1;

  /**
   * Wrapper class containing SnowflakeFileTransferMetadata and the timestamp at which the metadata
   * was refreshed
   */
  static class SnowflakeFileTransferMetadataWithAge {
    SnowflakeFileTransferMetadataV1 fileTransferMetadata;

    /* Do do not always know the age of the metadata, so we use the empty
    state to record unknown age.
     */
    Optional<Long> timestamp;

    SnowflakeFileTransferMetadataWithAge(
        SnowflakeFileTransferMetadataV1 fileTransferMetadata, Optional<Long> timestamp) {
      this.fileTransferMetadata = fileTransferMetadata;
      this.timestamp = timestamp;
    }
  }

  private SnowflakeFileTransferMetadataWithAge fileTransferMetadataWithAge;
  private final SnowflakeURL snowflakeURL;
  private final SnowflakeConnectionV1 conn;
  private final boolean isTestMode;
  private static final String putTemplate = "PUT " + Constants.STAGE_LOCATION + " @";
  private final long expirationTime = Constants.CREDENTIAL_EXPIRE_IN_SEC;

  public StreamingIngestStage(Connection conn, SnowflakeURL snowflakeURL, boolean isTestMode)
      throws SnowflakeSQLException, IOException {
    this.conn = (SnowflakeConnectionV1) conn;
    this.snowflakeURL = snowflakeURL;
    this.isTestMode = isTestMode;

    if (!isTestMode) {
      refreshSnowflakeMetadata();
      checkConnection();
      createInternalStage();
    }
  }

  /**
   * Upload file to internal stage with previously cached credentials. Will refetch and cache
   * credentials if they've expired.
   *
   * @param fullFilePath Full file name to be uploaded
   * @param data Data string to be uploaded
   */
  public void putRemote(String fullFilePath, byte[] data)
      throws SnowflakeSQLException, IOException {
    this.putRemote(fullFilePath, data, 0);
  }

  private void putRemote(String fullFilePath, byte[] data, int retryCount)
      throws SnowflakeSQLException, IOException {
    // Set filename to be uploaded
    SnowflakeFileTransferMetadataV1 fileTransferMetadata =
        fileTransferMetadataWithAge.fileTransferMetadata;

    /*
    Since we can have multiple calls to putRemote in parallel and because the metadata includes the file path
    we use a copy for the upload to prevent us from using the wrong file path.
     */
    SnowflakeFileTransferMetadataV1 fileTransferMetadataCopy =
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

    InputStream inStream = new ByteArrayInputStream(data);

    try {
      SnowflakeFileTransferAgent.uploadWithoutConnection(
          SnowflakeFileTransferConfig.Builder.newInstance()
              .setSnowflakeFileTransferMetadata(fileTransferMetadataCopy)
              .setUploadStream(inStream)
              .setRequireCompress(false)
              .setOcspMode(OCSPMode.FAIL_OPEN)
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

    // TODO Move to JWT/Oauth
    // TODO update configure url when we have new endpoint
    URI uri;
    try {
      uri =
          new URIBuilder()
              .setScheme(snowflakeURL.getScheme())
              .setHost(snowflakeURL.getUrlWithoutPort())
              .setPort(snowflakeURL.getPort())
              .setPath(CLIENT_CONFIGURE_ENDPOINT)
              .build();
    } catch (URISyntaxException e) {
      // TODO throw proper exception
      //      throw SnowflakeErrors.ERROR_6007.getException(e);
      throw new RuntimeException(e);
    }

    HttpPost postRequest = new HttpPost(uri);
    postRequest.addHeader("Accept", "application/json");

    StringEntity input = new StringEntity("{}", StandardCharsets.UTF_8);
    input.setContentType("application/json");
    postRequest.setEntity(input);

    String response = HttpUtil.executeGeneralRequest(postRequest, 60, null);
    JsonNode responseNode = mapper.readTree(response);

    // Currently have a few mismatches between the client/configure response and what
    // SnowflakeFileTransferAgent expects
    ObjectNode mutable = (ObjectNode) responseNode;
    mutable.putObject("data");
    ObjectNode dataNode = (ObjectNode) mutable.get("data");
    dataNode.set("stageInfo", responseNode.get("stage_location"));

    // JDBC expects this field which maps to presignedFileUrlName.  We override presignedFileUrlName
    // on each upload.
    dataNode.putArray("src_locations").add("placeholder");

    // Temporarily disabled until the new JDBC release is available
    /*    this.fileTransferMetadataWithAge =
    new SnowflakeFileTransferMetadataWithAge(
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(responseNode).get(0),
        Optional.of(System.currentTimeMillis()));*/
    return this.fileTransferMetadataWithAge;
  }

  /**
   * ONLY FOR TESTING. Sets the age of the file transfer metadata
   *
   * @param timestamp new age in milliseconds
   */
  void setFileTransferMetadataAge(long timestamp) {
    this.fileTransferMetadataWithAge.timestamp = Optional.of(timestamp);
  }

  /**
   * Upload file to internal stage
   *
   * @param fileName
   * @param blob
   */
  public void put(String fileName, byte[] blob) {
    if (getStageType() == StageInfo.StageType.LOCAL_FS) {
      putLocal(fileName, blob);
    } else {
      putRemoteWithCache(fileName, blob);
    }
  }

  /**
   * Get the backend stage type, S3, Azure or GCS. Involves one GS call.
   *
   * @return stage type
   */
  @VisibleForTesting
  StageInfo.StageType getStageType() {
    checkConnection();
    try {
      String command = putTemplate + Constants.STAGE_NAME;
      SnowflakeFileTransferAgent agent =
          new SnowflakeFileTransferAgent(
              command, this.conn.getSfSession(), new SFStatement(this.conn.getSfSession()));
      return agent.getStageInfo().getStageType();
    } catch (SnowflakeSQLException e) {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR);
    }
  }

  /**
   * Upload file to remote internal stage with previously cached credentials. Refresh credential
   * every 30 minutes
   *
   * @param fullFilePath Full file name to be uploaded
   * @param data Data string to be uploaded
   */
  @VisibleForTesting
  void putRemoteWithCache(String fullFilePath, byte[] data) {
    String command = putTemplate + Constants.STAGE_NAME;
    try {
      if (!isCredentialValid()) {
        SnowflakeFileTransferAgent agent =
            new SnowflakeFileTransferAgent(
                command, this.conn.getSfSession(), new SFStatement(this.conn.getSfSession()));
        // If the backend is not GCP, we cache the credential. Otherwise throw error.
        // transfer metadata list must only have one element
        SnowflakeFileTransferMetadataV1 fileTransferMetadata =
            (SnowflakeFileTransferMetadataV1) agent.getFileTransferMetadatas().get(0);
        if (fileTransferMetadata.getStageInfo().getStageType() != StageInfo.StageType.GCS) {
          // Overwrite the credential to be used
          fileTransferMetadataWithAge =
              new SnowflakeFileTransferMetadataWithAge(
                  fileTransferMetadata, Optional.of(System.currentTimeMillis()));
        } else {
          throw new SFException(ErrorCode.INTERNAL_ERROR);
        }
      }

      SnowflakeFileTransferMetadataV1 fileTransferMetadata =
          fileTransferMetadataWithAge.fileTransferMetadata;
      // Set filename to be uploaded
      fileTransferMetadata.setPresignedUrlFileName(fullFilePath);

      InputStream inStream = new ByteArrayInputStream(data);

      SnowflakeFileTransferAgent.uploadWithoutConnection(
          SnowflakeFileTransferConfig.Builder.newInstance()
              .setSnowflakeFileTransferMetadata(fileTransferMetadata)
              .setUploadStream(inStream)
              .setRequireCompress(false)
              .setOcspMode(OCSPMode.FAIL_OPEN)
              .build());
    } catch (Exception e) {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR);
    }
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
      throw new SFException(ErrorCode.INTERNAL_ERROR);
    }

    InputStream input = new ByteArrayInputStream(data);
    try {
      this.conn.uploadStream(Constants.STAGE_NAME, "", input, fullFilePath, false);
    } catch (Exception e) {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR);
    }
  }

  /** Make sure connection is not closed */
  private void checkConnection() {
    try {
      if (this.conn == null || this.conn.isClosed()) {
        throw new SFException(ErrorCode.INTERNAL_ERROR);
      }
    } catch (SQLException e) {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR);
    }
  }

  /**
   * Check whether the credential has expired or not
   *
   * @return a boolean indicates whether the credential is valid or not
   */
  @VisibleForTesting
  boolean isCredentialValid() {
    // Key is cached and not expired
    return fileTransferMetadataWithAge != null
        && fileTransferMetadataWithAge.timestamp.isPresent()
        && System.currentTimeMillis() - fileTransferMetadataWithAge.timestamp.get()
            < this.expirationTime;
  }

  private void createInternalStage() {
    String query = "create database if not exists identifier(?);";
    try {
      PreparedStatement stmt = this.conn.prepareStatement(query);
      stmt.setString(1, Constants.INTERNAL_STAGE_DB_NAME);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR);
    }

    query = "use database identifier(?);";
    try {
      PreparedStatement stmt = this.conn.prepareStatement(query);
      stmt.setString(1, Constants.INTERNAL_STAGE_DB_NAME);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR);
    }

    query = "create schema if not exists identifier(?);";
    try {
      PreparedStatement stmt = this.conn.prepareStatement(query);
      stmt.setString(1, Constants.INTERNAL_STAGE_SCHEMA_NAME);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR);
    }

    query = "use schema identifier(?);";
    try {
      PreparedStatement stmt = this.conn.prepareStatement(query);
      stmt.setString(1, Constants.INTERNAL_STAGE_SCHEMA_NAME);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR);
    }

    query = "create stage if not exists identifier(?);";
    try {
      PreparedStatement stmt = this.conn.prepareStatement(query);
      stmt.setString(1, Constants.STAGE_NAME);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR);
    }
  }
}

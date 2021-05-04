/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import net.snowflake.client.jdbc.*;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHeaders;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;

/**
 * Handles uploading files to Snowflake cloud storage.
 */
public class StreamingIngestStage {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final long REFRESH_THRESHOLD_IN_MS = 1000 * 60; // Do not attempt to refresh credentials if the current ones are younger than this.

  /**
   Wrapper class containing SnowflakeFileTransferMetadata and the timestamp
   at which the metadata was refreshed
   */
  private static class SnowflakeFileTransferMetadataWithAge {
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
  private final SnowflakeConnection connection;

  public StreamingIngestStage(
      SnowflakeConnection connection, SnowflakeFileTransferMetadataV1 fileTransferMetadata) {
    this.connection = connection;
    this.fileTransferMetadataWithAge = new SnowflakeFileTransferMetadataWithAge(fileTransferMetadata, Optional.empty());
  }

  public StreamingIngestStage(SnowflakeConnection connection)
      throws SnowflakeSQLException, IOException {
    this.connection = connection;
    this.fileTransferMetadataWithAge = this.refreshSnowflakeMetadata();
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
    // Set filename to be uploaded
    SnowflakeFileTransferMetadataV1 fileTransferMetadata = fileTransferMetadataWithAge.fileTransferMetadata;

    /*
    Since we can have multiple calls to putRemote in parallel and because the metadata includes the file path
    we use a copy for the upload to prevent us from using the wrong file path.
     */
    SnowflakeFileTransferMetadataV1 fileTransferMetadataCopy = new SnowflakeFileTransferMetadataV1(
            fileTransferMetadata.getPresignedUrl(),
            fullFilePath,
            fileTransferMetadata.getEncryptionMaterial() != null ? fileTransferMetadata.getEncryptionMaterial().getQueryStageMasterKey() : null,
            fileTransferMetadata.getEncryptionMaterial() != null ? fileTransferMetadata.getEncryptionMaterial().getQueryId() : null,
            fileTransferMetadata.getEncryptionMaterial() != null ? fileTransferMetadata.getEncryptionMaterial().getSmkId() : null,
            fileTransferMetadata.getCommandType(),
            fileTransferMetadata.getStageInfo()
    );

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
      // TODO Update JDBC driver to throw a reliable token expired error
      if (npe.getStackTrace()[0].getClassName() == "net.snowflake.client.core.SFStatement"
          && npe.getStackTrace()[0].getLineNumber() == 332) {
        this.fileTransferMetadataWithAge = this.refreshSnowflakeMetadata();

        // TODO revisit possible infinite recursion here if the metadata expiration happens too quickly
        this.putRemote(fullFilePath, data);
        return;
      }
      throw npe;
    } catch (Exception e) {
      throw new SnowflakeSQLException(e, ErrorCode.IO_ERROR);
    }
  }

  SnowflakeFileTransferMetadataWithAge refreshSnowflakeMetadata() throws SnowflakeSQLException, IOException {
    return refreshSnowflakeMetadata(false);
  }

  /**
   * Gets new stage credentials and other metadata from Snowflake.  Synchronized to prevent
   * multiple calls to putRemote from trying to refresh at the same time
   *
   * @param force if true will ignore REFRESH_THRESHOLD and force metadata refresh
   * @return refreshed metadata
   * @throws SnowflakeSQLException
   * @throws IOException
   */
  synchronized SnowflakeFileTransferMetadataWithAge refreshSnowflakeMetadata(boolean force)
      throws SnowflakeSQLException, IOException {
    if (!force
            && fileTransferMetadataWithAge.timestamp.isPresent()
            && fileTransferMetadataWithAge.timestamp.get() > System.currentTimeMillis() - REFRESH_THRESHOLD_IN_MS
    ) {
      return fileTransferMetadataWithAge;
    }

    // TODO Move to JWT/Oauth
    String sessionToken = ((SnowflakeConnectionV1) connection).getSfSession().getSessionToken();
    SnowflakeConnectString connectString =
        ((SnowflakeConnectionV1) connection).getSfSession().getSnowflakeConnectionString();
    // TODO update configure url when we have new endpoint
    String configureUrl =
        String.format(
            "%s://%s:%s/v1/streaming/client/configure",
            connectString.getScheme(), connectString.getHost(), connectString.getPort());

    HttpPost postRequest = new HttpPost(configureUrl);
    postRequest.addHeader("Accept", "application/json");
    postRequest.setHeader(HttpHeaders.AUTHORIZATION, "Basic");

    postRequest.setHeader(
        "Authorization", "Snowflake" + " " + "token" + "=\"" + sessionToken + "\"");

    StringEntity input = new StringEntity("{}", StandardCharsets.UTF_8);
    input.setContentType("application/json");
    postRequest.setEntity(input);

    String response = HttpUtil.executeGeneralRequest(postRequest, 100, null);
    JsonNode responseNode = mapper.readTree(response);

    // Currently have a few mismatches between the client/configure response and what
    // SnowflakeFileTransferAgent expects
    ObjectNode mutable = (ObjectNode) responseNode;
    mutable.putObject("data");
    ObjectNode dataNode = (ObjectNode) mutable.get("data");
    dataNode.set("stageInfo", responseNode.get("stage_location"));

    // JDBC expects this field which maps to presignedFileUrlName.  We override presignedFileUrlName on each upload.
    dataNode.putArray("src_locations").add("placeholder/");

    return new SnowflakeFileTransferMetadataWithAge(SnowflakeFileTransferAgent.getFileTransferMetadatas(responseNode).get(0),
            Optional.of(System.currentTimeMillis())
    );
  }
}

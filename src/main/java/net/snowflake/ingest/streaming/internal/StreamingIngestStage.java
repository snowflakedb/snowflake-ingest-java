/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.CLIENT_CONFIGURE_ENDPOINT;

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** Handles uploading files to the Snowflake Streaming Ingest Stage */
class StreamingIngestStage extends AbstractCloudStorage {

  /**
   * General constructor
   *
   * @param isTestMode whether it's testing mode
   * @param role Snowflake role used by the Client
   * @param httpClient http client reference
   * @param requestBuilder request builder to build the HTTP request
   * @param clientName the client name
   * @param maxUploadRetries maximum time of upload retries
   */
  StreamingIngestStage(
      boolean isTestMode,
      String role,
      CloseableHttpClient httpClient,
      RequestBuilder requestBuilder,
      String clientName,
      int maxUploadRetries)
      throws SnowflakeSQLException, IOException {
    super(
        role, httpClient, requestBuilder, clientName, CLIENT_CONFIGURE_ENDPOINT, maxUploadRetries);
    if (!isTestMode) {
      refreshCloudStorageMetadata();
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
      CloseableHttpClient httpClient,
      RequestBuilder requestBuilder,
      String clientName,
      SnowflakeFileTransferMetadataWithAge testMetadata,
      int maxRetryCount)
      throws SnowflakeSQLException, IOException {
    this(isTestMode, role, httpClient, requestBuilder, clientName, maxRetryCount);
    if (!isTestMode) {
      throw new SFException(ErrorCode.INTERNAL_ERROR);
    }
    this.fileTransferMetadataWithAge = testMetadata;
  }

  @Override
  protected Map<Object, Object> getConfigurePayload() {
    Map<Object, Object> payload = new HashMap<>();
    payload.put("role", this.role);
    return payload;
  }

  /**
   * Upload file to internal stage
   *
   * @param filePath
   * @param blob
   */
  @Override
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

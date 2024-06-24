/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.StreamingIngestUtils.executeWithRetries;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.ServiceResponseHandler;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;

/** A class that is used to configure a Snowflake Ingest Storage */
class ConfigureCallHandler {

  // Object mapper for creating payload, ignore null fields
  private static final ObjectMapper mapper =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  private final CloseableHttpClient httpClient;
  private final RequestBuilder requestBuilder;
  private final ServiceResponseHandler.ApiName apiName;
  private final String configureEndpoint;
  private final String role;
  private final String database;
  private final String schema;
  private final String table;

  /**
   * Builder method to create a {@link ConfigureCallHandler}
   *
   * @param httpClient the HTTP client
   * @param requestBuilder the request builder
   * @param apiName the API name, used for logging
   * @param configureEndpoint the configure endpoint
   * @return a {@link ConfigureCallHandlerBuilder} object
   */
  static ConfigureCallHandlerBuilder builder(
      CloseableHttpClient httpClient,
      RequestBuilder requestBuilder,
      ServiceResponseHandler.ApiName apiName,
      String configureEndpoint) {
    return new ConfigureCallHandlerBuilder(httpClient, requestBuilder, apiName, configureEndpoint);
  }

  /** Builder class to build a {@link ConfigureCallHandler} */
  static class ConfigureCallHandlerBuilder {
    private final CloseableHttpClient httpClient;
    private final RequestBuilder requestBuilder;
    private final ServiceResponseHandler.ApiName ApiName;
    private final String configureEndpoint;
    private String role;
    private String database;
    private String schema;
    private String table;
    private boolean isTestMode;

    /**
     * Constructor for ConfigureCallHandlerBuilder
     *
     * @param httpClient the HTTP client
     * @param requestBuilder the request builder
     * @param apiName the API name, used for logging
     * @param configureEndpoint the configure endpoint
     */
    ConfigureCallHandlerBuilder(
        CloseableHttpClient httpClient,
        RequestBuilder requestBuilder,
        ServiceResponseHandler.ApiName apiName,
        String configureEndpoint) {
      this.httpClient = httpClient;
      this.requestBuilder = requestBuilder;
      this.ApiName = apiName;
      this.configureEndpoint = configureEndpoint;
    }

    public ConfigureCallHandlerBuilder setRole(String role) {
      this.role = role;
      return this;
    }

    public ConfigureCallHandlerBuilder setDatabase(String database) {
      this.database = database;
      return this;
    }

    public ConfigureCallHandlerBuilder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public ConfigureCallHandlerBuilder setTable(String table) {
      this.table = table;
      return this;
    }

    public ConfigureCallHandlerBuilder setIsTestMode(boolean isTestMode) {
      this.isTestMode = isTestMode;
      return this;
    }

    public ConfigureCallHandler build() {
      return new ConfigureCallHandler(this);
    }
  }

  ConfigureCallHandler(ConfigureCallHandlerBuilder builder) {
    if (!builder.isTestMode) {
      Utils.assertNotNull("http client", builder.httpClient);
      Utils.assertNotNull("request builder", builder.requestBuilder);
      Utils.assertNotNull("api name", builder.ApiName);
      Utils.assertStringNotNullOrEmpty("configure endpoint", builder.configureEndpoint);
    }

    this.httpClient = builder.httpClient;
    this.requestBuilder = builder.requestBuilder;
    this.apiName = builder.ApiName;
    this.configureEndpoint = builder.configureEndpoint;
    this.role = builder.role;
    this.database = builder.database;
    this.schema = builder.schema;
    this.table = builder.table;
  }

  /**
   * Make a configure call to the Snowflake service
   *
   * @return the configure response
   * @throws IOException
   */
  ConfigureResponse makeConfigureCall() throws IOException {
    return makeConfigureCall(makeConfigurePayload());
  }

  /**
   * Make a configure call to the Snowflake service with a file name, used for GCS
   *
   * @param fileName the file name
   * @return the configure response
   * @throws IOException
   */
  ConfigureResponse makeConfigureCall(String fileName) throws IOException {
    Map<String, String> payload = makeConfigurePayload();
    payload.put("file_name", fileName);
    return makeConfigureCall(payload);
  }

  private ConfigureResponse makeConfigureCall(Map<String, String> payload) throws IOException {
    try {
      ConfigureResponse response =
          executeWithRetries(
              ConfigureResponse.class,
              this.configureEndpoint,
              mapper.writeValueAsString(payload),
              "client configure",
              this.apiName,
              this.httpClient,
              this.requestBuilder);

      // Check for Snowflake specific response code
      if (response.getStatusCode() != RESPONSE_SUCCESS) {
        throw new SFException(ErrorCode.CONFIGURE_FAILURE, response.getMessage());
      }
      return response;
    } catch (IngestResponseException e) {
      throw new SFException(e, ErrorCode.CONFIGURE_FAILURE, e.getMessage());
    }
  }

  private Map<String, String> makeConfigurePayload() {
    Map<String, String> payload = new HashMap<>();
    payload.put("role", this.role);
    payload.put("database", this.database);
    payload.put("schema", this.schema);
    payload.put("table", this.table);
    return payload;
  }
}

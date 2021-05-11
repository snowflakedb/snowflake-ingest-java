/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import static net.snowflake.ingest.streaming.internal.Constants.OPEN_CHANNEL_ENDPOINT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.streaming.internal.Constants;
import net.snowflake.ingest.streaming.internal.SnowflakeURL;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.StreamingUtils;
import org.apache.http.client.utils.URIBuilder;

/** The request that gets sent to Snowflake to open/create a Streaming Ingest channel */
public class OpenChannelRequest {
  private final String channelName;
  private final String dbName;
  private final String schemaName;
  private final String tableName;

  public static OpenChannelRequestBuilder builder(String channelName) {
    return new OpenChannelRequestBuilder(channelName);
  }

  /** Builder class to build a OpenChannelRequest */
  public static class OpenChannelRequestBuilder {
    private String channelName;
    private String dbName;
    private String schemaName;
    private String tableName;

    public OpenChannelRequestBuilder(String channelName) {
      this.channelName = channelName;
    }

    public OpenChannelRequestBuilder setDBName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public OpenChannelRequestBuilder setSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public OpenChannelRequestBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public OpenChannelRequest build() {
      return new OpenChannelRequest(this);
    }
  }

  private OpenChannelRequest(OpenChannelRequestBuilder builder) {
    StreamingUtils.assertStringNotNullOrEmpty("channel name", builder.channelName);
    StreamingUtils.assertStringNotNullOrEmpty("database name", builder.dbName);
    StreamingUtils.assertStringNotNullOrEmpty("schema name", builder.schemaName);
    StreamingUtils.assertStringNotNullOrEmpty("table name", builder.tableName);

    this.channelName = builder.channelName;
    this.dbName = builder.dbName;
    this.schemaName = builder.schemaName;
    this.tableName = builder.tableName;
  }

  public String getDBName() {
    return this.dbName;
  }

  public String getSchemaName() {
    return this.schemaName;
  }

  public String getTableName() {
    return this.tableName;
  }

  public String getChannelName() {
    return this.channelName;
  }

  public String getFullyQualifiedTableName() {
    return String.format("%s.%s.%s", this.dbName, this.schemaName, this.tableName);
  }

  /**
   * Construct the open channel POST request
   *
   * @param accountURL
   * @return the http POST request
   */
  public HttpRequest getHttpRequest(SnowflakeURL accountURL) {
    Map<Object, Object> payload = new HashMap<>();
    payload.put("channel", this.channelName);
    payload.put("table", this.tableName);
    payload.put("database", this.dbName);
    payload.put("schema", this.schemaName);
    payload.put("write_mode", Constants.WriteMode.CLOUD_STORAGE.name());

    HttpRequest request = null;
    try {
      URI uri =
          new URIBuilder()
              .setScheme(accountURL.getScheme())
              .setHost(accountURL.getFullUrl())
              .setPath(OPEN_CHANNEL_ENDPOINT)
              .build();

      // TODO SNOW-349081: add user agent and JWT token to header
      request =
          HttpRequest.newBuilder()
              .uri(uri)
              .setHeader("content-type", "application/json")
              .setHeader("accept", "application/json")
              .POST(
                  HttpRequest.BodyPublishers.ofString(
                      new ObjectMapper().writeValueAsString(payload)))
              .build();
    } catch (JsonProcessingException | URISyntaxException e) {
      throw new SFException(e, ErrorCode.BUILD_REQUEST_FAILURE, "open channel");
    }

    return request;
  }
}

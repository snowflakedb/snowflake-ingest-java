/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.snowflake.ingest.sdk.server;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.Put;
import com.linecorp.armeria.server.annotation.RequestObject;
import com.snowflake.ingest.sdk.server.exception.CustomExceptionHandler;
import com.snowflake.ingest.sdk.server.model.ChannelIdentifier;
import com.snowflake.ingest.sdk.server.request.CreateClientRequest;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that hosts Streaming Ingest SDK APIs as endpoints. This service provides a REST API
 * wrapper around the Snowflake Streaming Ingest SDK, allowing clients to: - Create and manage
 * streaming ingest clients - Open and manage channels - Insert data into Snowflake tables
 */
@ExceptionHandler(CustomExceptionHandler.class)
public class StreamingIngestJavaService {
  private static final Logger logger = LoggerFactory.getLogger(StreamingIngestJavaService.class);

  // Client state management
  private static final Map<String, SnowflakeStreamingIngestClient> clientMap =
      new ConcurrentHashMap<>();
  private static final Map<ChannelIdentifier, SnowflakeStreamingIngestChannel> channelsMap =
      new ConcurrentHashMap<>();

  // Default values
  private static final String DEFAULT_PORT = "443";
  private static final String DEFAULT_SCHEME = "https";
  private static final String DEFAULT_SSL = "on";
  private static final String DEFAULT_ON_ERROR = "CONTINUE";
  private static final String AUTH_TYPE = "JWT";

  // Required client properties
  private static final String[] REQUIRED_PROPS = {
    "user", "url", "account", "private_key", "host", "schema", "database", "warehouse", "role"
  };

  @Get("/health")
  public HttpResponse healthCheck() {
    logger.info("Running health check");
    return HttpResponse.of(HttpStatus.OK);
  }

  @Put("/clients/{clientId}")
  public HttpResponse createClient(
      @Param("clientId") String clientId, @RequestObject CreateClientRequest request)
      throws Exception {
    Map<String, String> parameterOverrides = request.getParameterOverrides();
    Properties props = buildClientProperties(parameterOverrides);
    validateRequiredProperties(props);

    SnowflakeStreamingIngestClient client =
        createSnowflakeClient(clientId, parameterOverrides, props);
    registerClient(clientId, client);

    logger.info("Successfully created streaming ingest client with id: {}", clientId);
    return HttpResponse.ofJson(
        Map.of(
            "status", "success",
            "clientId", clientId,
            "message", "Successfully created streaming ingest client"));
  }

  private Properties buildClientProperties(Map<String, String> overrides) {
    Properties props = new Properties();

    // Required properties
    props.put("user", overrides.get("user"));
    props.put("url", overrides.get("url"));
    props.put("account", overrides.get("account"));
    props.put("private_key", overrides.get("private_key"));
    props.put("host", overrides.get("host"));
    props.put("schema", overrides.get("schema"));
    props.put("database", overrides.get("database"));
    props.put("warehouse", overrides.get("warehouse"));
    props.put("role", overrides.get("role"));

    // Let client specify auth type or use SDK default
    if (overrides.containsKey("authorization_type")) {
      props.put("authorization_type", overrides.get("authorization_type"));
    }

    // Optional properties with defaults
    props.put("port", overrides.getOrDefault("port", DEFAULT_PORT));
    props.put("scheme", overrides.getOrDefault("scheme", DEFAULT_SCHEME));
    props.put("ssl", overrides.getOrDefault("ssl", DEFAULT_SSL));
    props.put("connect_string", overrides.get("connect_string"));

    return props;
  }

  private void validateRequiredProperties(Properties props) {
    String[] requiredProps = {
      "user", "url", "account", "private_key", "host", "schema", "database", "warehouse", "role"
    };
    for (String prop : requiredProps) {
      if (!props.containsKey(prop)
          || props.get(prop) == null
          || props.get(prop).toString().trim().isEmpty()) {
        String msg = "Missing required property: " + prop;
        logger.error(msg);
        throw new IllegalArgumentException(msg);
      }
    }
  }

  private SnowflakeStreamingIngestClient createSnowflakeClient(
      String clientId, Map<String, String> overrides, Properties props) {
    logger.debug("Creating client {} with properties: {}", clientId, props);
    return SnowflakeStreamingIngestClientFactory.builder("client_" + clientId)
        .setProperties(props)
        .build();
  }

  private void registerClient(String clientId, SnowflakeStreamingIngestClient client) {
    clientMap.put(clientId, client);
  }

  @Post("/clients/{clientId}/close")
  public HttpResponse closeClient(@Param("clientId") String clientId) throws Exception {
    SnowflakeStreamingIngestClient client = getClientOrThrow(clientId);
    client.close();

    logger.info("Successfully closed client: {}", clientId);
    return HttpResponse.ofJson(
        Map.of(
            "status", "success",
            "clientId", clientId,
            "message", "Successfully closed client"));
  }

  @Put("/clients/{clientId}/channels/{channelName}")
  public HttpResponse openChannel(
      @Param("clientId") String clientId,
      @Param("channelName") String channelName,
      @RequestObject Map<String, String> request)
      throws Exception {
    SnowflakeStreamingIngestClient client = getClientOrThrow(clientId);
    OpenChannelRequest channelRequest = buildChannelRequest(channelName, request);

    SnowflakeStreamingIngestChannel channel = client.openChannel(channelRequest);
    channelsMap.put(ChannelIdentifier.of(clientId, channelName), channel);

    logger.info("Successfully opened channel {} for client {}", channelName, clientId);
    return HttpResponse.ofJson(
        Map.of(
            "status",
            "success",
            "clientId",
            clientId,
            "channelName",
            channelName,
            "database",
            request.get("database"),
            "schema",
            request.get("schema"),
            "table",
            request.get("table"),
            "message",
            "Successfully opened channel"));
  }

  private OpenChannelRequest buildChannelRequest(String channelName, Map<String, String> request) {
    return OpenChannelRequest.builder(channelName)
        .setDBName(request.get("database"))
        .setSchemaName(request.get("schema"))
        .setTableName(request.get("table"))
        .setOnErrorOption(
            OpenChannelRequest.OnErrorOption.valueOf(
                request.getOrDefault("on_error", DEFAULT_ON_ERROR)))
        .build();
  }

  @Post("/clients/{clientId}/channels/{channelName}/insertRow")
  public HttpResponse insertRow(
      @Param("clientId") String clientId,
      @Param("channelName") String channelName,
      @RequestObject Map<String, Object> row,
      @Param("offsetToken") String offsetToken)
      throws Exception {
    SnowflakeStreamingIngestChannel channel = getChannelOrThrow(clientId, channelName);
    logger.debug(
        "Inserting row to channel {}/{}, row: {}, offsetToken: {}",
        clientId,
        channelName,
        row,
        offsetToken);

    InsertValidationResponse response = channel.insertRow(row, offsetToken);

    if (response.hasErrors()) {
      throw response.getInsertErrors().get(0).getException();
    }

    return HttpResponse.ofJson(
        Map.of(
            "status", "success",
            "message", "Successfully inserted row",
            "clientId", clientId,
            "channelName", channelName,
            "offsetToken", offsetToken));
  }

  @Get("/clients/{clientId}/channels/{channelName}/offset")
  public HttpResponse getLatestCommittedOffsetToken(
      @Param("clientId") String clientId, @Param("channelName") String channelName)
      throws Exception {
    SnowflakeStreamingIngestChannel channel = getChannelOrThrow(clientId, channelName);
    String offsetToken = channel.getLatestCommittedOffsetToken();
    return HttpResponse.ofJson(Map.of("offsetToken", offsetToken));
  }

  @Post("/clients/{clientId}/channels/{channelName}/close")
  public HttpResponse closeChannel(
      @Param("clientId") String clientId, @Param("channelName") String channelName)
      throws Exception {
    SnowflakeStreamingIngestChannel channel = getChannelOrThrow(clientId, channelName);
    channel.close().get();
    channelsMap.remove(ChannelIdentifier.of(clientId, channelName));
    return HttpResponse.ofJson(
        Map.of(
            "status",
            "success",
            "clientId",
            clientId,
            "channelName",
            channelName,
            "message",
            "Successfully closed channel"));
  }

  private SnowflakeStreamingIngestClient getClientOrThrow(String clientId) {
    SnowflakeStreamingIngestClient client = clientMap.get(clientId);
    if (client == null) {
      throw new IllegalArgumentException("Client not found: " + clientId);
    }
    return client;
  }

  private SnowflakeStreamingIngestChannel getChannelOrThrow(String clientId, String channelName) {
    ChannelIdentifier identifier = ChannelIdentifier.of(clientId, channelName);
    SnowflakeStreamingIngestChannel channel = channelsMap.get(identifier);
    if (channel == null) {
      throw new IllegalArgumentException(
          "Channel not found: " + channelName + " for client: " + clientId);
    }
    return channel;
  }
}

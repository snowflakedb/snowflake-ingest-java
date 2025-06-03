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
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.Put;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.RequestObject;
import com.snowflake.ingest.common.exception.AppServerException;
import com.snowflake.ingest.common.request.CreateClientRequest;
import com.snowflake.ingest.sdk.server.exception.CustomExceptionHandler;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Service that hosts Streaming Ingest SDK APIs as endpoints. */
@ExceptionHandler(CustomExceptionHandler.class)
public class StreamingIngestJavaService {
    private static final Logger logger = LoggerFactory.getLogger(StreamingIngestJavaService.class);
    
    // Map to store clientId and client object
    private static final Map<String, SnowflakeStreamingIngestClient> clientMap = new ConcurrentHashMap<>();
    
    // Map to store clientId -> channelName -> channel object
    private static final Map<String, Map<String, SnowflakeStreamingIngestChannel>> channelsMap = new ConcurrentHashMap<>();

    @Get("/health")
    public HttpResponse healthCheck() {
        logger.info("Running health check");
        return HttpResponse.of(HttpStatus.OK);
    }

    @Put("/clients/{clientId}")
    public HttpResponse createClient(
            @Param String clientId, 
            @RequestObject CreateClientRequest request) {
        try {
            Properties props = new Properties();
            Map<String, String> overrides = request.getParameterOverrides();
            
            logger.debug("Creating client with properties: {}", overrides);
            
            // Required properties
            props.put("user", overrides.get("user"));
            props.put("url", overrides.get("url"));
            props.put("account", overrides.get("account"));
            props.put("private_key", overrides.get("private_key"));
            props.put("port", overrides.getOrDefault("port", "443"));
            props.put("host", overrides.get("host"));
            props.put("schema", overrides.get("schema"));
            props.put("scheme", overrides.getOrDefault("scheme", "https"));
            props.put("database", overrides.get("database"));
            props.put("connect_string", overrides.get("connect_string"));
            props.put("ssl", overrides.getOrDefault("ssl", "on"));
            props.put("warehouse", overrides.get("warehouse"));
            props.put("role", overrides.get("role"));
            
            // Add authorization type as JWT
            props.put("authorization_type", "JWT");
            
            // Validate required properties
            String[] requiredProps = {"user", "url", "account", "private_key", "host", "schema", "database", "warehouse", "role"};
            for (String prop : requiredProps) {
                if (!props.containsKey(prop) || props.get(prop) == null || props.get(prop).toString().trim().isEmpty()) {
                    String msg = "Missing required property: " + prop;
                    logger.error(msg);
                    throw new AppServerException(AppServerException.AppErrorCode.INVALID_INPUT, msg);
                }
            }
            
            logger.debug("Building client with properties: {}", props);
            
            try {
                // Create client with proper configuration
                SnowflakeStreamingIngestClientFactory.Builder builder = SnowflakeStreamingIngestClientFactory.builder(clientId)
                    .setProperties(props)
                    .setIsTestMode(Boolean.parseBoolean(overrides.getOrDefault("ROWSET_DEV_VM_TEST_MODE", "false")));
                
                logger.debug("Created builder: {}", builder);
                
                SnowflakeStreamingIngestClient client = builder.build();
                
                if (client == null) {
                    throw new AppServerException(AppServerException.AppErrorCode.UNEXPECTED_ERROR,
                        "Client builder returned null");
                }
                
                logger.debug("Successfully built client: {}", client);
                
                clientMap.put(clientId, client);
                channelsMap.put(clientId, new ConcurrentHashMap<>());

                logger.info("Successfully created streaming ingest client with id: {}", clientId);
                return HttpResponse.of(HttpStatus.OK);
            } catch (Exception e) {
                String msg = e.getMessage();
                if (msg == null && e.getCause() != null) {
                    msg = e.getCause().getMessage();
                    if (msg == null) {
                        msg = e.getCause().toString();
                    }
                }
                if (msg == null) {
                    msg = e.toString();
                }
                logger.error("Error building client: {}", msg, e);
                throw new AppServerException(AppServerException.AppErrorCode.UNEXPECTED_ERROR,
                    "Failed to build client: " + msg);
            }
        } catch (AppServerException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Error creating streaming ingest client", e);
            throw new AppServerException(AppServerException.AppErrorCode.UNEXPECTED_ERROR,
                "Failed to create streaming ingest client: " + e.getMessage());
        }
    }

    @Post("/clients/{clientId}/close")
    public HttpResponse closeClient(@Param String clientId) {
        try {
            SnowflakeStreamingIngestClient client = getClientOrThrow(clientId);
            
            // Close all channels for this client first
            Map<String, SnowflakeStreamingIngestChannel> clientChannels = channelsMap.get(clientId);
            if (clientChannels != null) {
                for (SnowflakeStreamingIngestChannel channel : clientChannels.values()) {
                    try {
                        channel.close().get();
                    } catch (Exception e) {
                        logger.warn("Error closing channel for client {}", clientId, e);
                    }
                }
            }
            
            client.close();
            clientMap.remove(clientId);
            channelsMap.remove(clientId);
            
            logger.info("Successfully closed and removed client: {}", clientId);
            return HttpResponse.of(HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Error closing client", e);
            throw new AppServerException(AppServerException.AppErrorCode.UNEXPECTED_ERROR,
                "Failed to close client: " + e.getMessage());
        }
    }

    @Put("/clients/{clientId}/channels/{channelName}")
    public HttpResponse openChannel(
            @Param String clientId,
            @Param String channelName,
            @RequestObject Map<String, String> request) {
        try {
            SnowflakeStreamingIngestClient client = getClientOrThrow(clientId);
            
            OpenChannelRequest channelRequest = OpenChannelRequest.builder(channelName)
                .setDBName(request.get("dbName"))
                .setSchemaName(request.get("schemaName"))
                .setTableName(request.get("tableName"))
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.valueOf(
                    request.getOrDefault("onErrorOption", "CONTINUE")))
                .build();
            
            SnowflakeStreamingIngestChannel channel = client.openChannel(channelRequest);
            channelsMap.get(clientId).put(channelName, channel);
            
            return HttpResponse.of(HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Error opening channel", e);
            throw new AppServerException(AppServerException.AppErrorCode.UNEXPECTED_ERROR,
                "Failed to open channel: " + e.getMessage());
        }
    }

    @Post("/clients/{clientId}/channels/{channelName}/insertRow")
    public HttpResponse insertRow(
            @Param String clientId,
            @Param String channelName,
            @RequestObject Map<String, Object> row,
            @Param String offsetToken) {
        try {
            SnowflakeStreamingIngestChannel channel = getChannelOrThrow(clientId, channelName);
            
            InsertValidationResponse response = channel.insertRow(row, offsetToken);
            if (response.hasErrors()) {
                throw response.getInsertErrors().get(0).getException();
            }
            
            return HttpResponse.of(HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Error inserting row", e);
            throw new AppServerException(AppServerException.AppErrorCode.UNEXPECTED_ERROR,
                "Failed to insert row: " + e.getMessage());
        }
    }

    @Get("/clients/{clientId}/channels/{channelName}/offset")
    public HttpResponse getLatestCommittedOffsetToken(
            @Param String clientId,
            @Param String channelName) {
        try {
            SnowflakeStreamingIngestChannel channel = getChannelOrThrow(clientId, channelName);
            String offsetToken = channel.getLatestCommittedOffsetToken();
            
            return HttpResponse.ofJson(Map.of("offsetToken", offsetToken));
        } catch (Exception e) {
            logger.error("Error getting offset token", e);
            throw new AppServerException(AppServerException.AppErrorCode.UNEXPECTED_ERROR,
                "Failed to get offset token: " + e.getMessage());
        }
    }

    @Post("/clients/{clientId}/channels/{channelName}/close")
    public HttpResponse closeChannel(
            @Param String clientId,
            @Param String channelName) {
        try {
            SnowflakeStreamingIngestChannel channel = getChannelOrThrow(clientId, channelName);
            channel.close().get();
            channelsMap.get(clientId).remove(channelName);
            
            return HttpResponse.of(HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Error closing channel", e);
            throw new AppServerException(AppServerException.AppErrorCode.UNEXPECTED_ERROR,
                "Failed to close channel: " + e.getMessage());
        }
    }

    private SnowflakeStreamingIngestClient getClientOrThrow(String clientId) {
        SnowflakeStreamingIngestClient client = clientMap.get(clientId);
        if (client == null) {
            throw new AppServerException(AppServerException.AppErrorCode.INVALID_INPUT,
                "Client not found: " + clientId);
        }
        return client;
    }

    private SnowflakeStreamingIngestChannel getChannelOrThrow(String clientId, String channelName) {
        Map<String, SnowflakeStreamingIngestChannel> clientChannels = channelsMap.get(clientId);
        if (clientChannels == null) {
            throw new AppServerException(AppServerException.AppErrorCode.INVALID_INPUT,
                "Client not found: " + clientId);
        }
        
        SnowflakeStreamingIngestChannel channel = clientChannels.get(channelName);
        if (channel == null) {
            throw new AppServerException(AppServerException.AppErrorCode.INVALID_INPUT,
                "Channel not found: " + channelName + " for client: " + clientId);
        }
        return channel;
    }
}

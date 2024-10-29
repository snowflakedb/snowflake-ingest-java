/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.time.ZoneOffset;
import java.util.Collections;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.SnowflakeURL;
import org.junit.Assert;
import org.junit.Test;

/** Validate that we serialize the request into string correctly when sending the http requests. */
public class RequestSerializationTest {
  @Test
  public void testValidateOpenChannelRequestInternal() {
    OpenChannelRequestInternal openChannelRequest =
        new OpenChannelRequestInternal(
            "request_id",
            "test_role",
            "test_db",
            "test_schema",
            "test_table",
            "test_channel",
            Constants.WriteMode.CLOUD_STORAGE,
            true,
            "test_offset_token");
    String serializedRequest =
        StreamingIngestUtils.serializeRequestToString(openChannelRequest, "test");
    String expectedString =
        "{\"request_id\":\"request_id\",\"role\":\"test_role\",\"channel\":\"test_channel\",\"table\":\"test_table\",\"database\":\"test_db\",\"schema\":\"test_schema\",\"write_mode\":\"CLOUD_STORAGE\",\"is_iceberg\":true,\"offset_token\":\"test_offset_token\"}";
    Assert.assertEquals(expectedString, serializedRequest);
  }

  @Test
  public void testValidateGeneratePresignedUrlsRequest() {
    GeneratePresignedUrlsRequest request =
        new GeneratePresignedUrlsRequest(
            new TableRef("test_db", "test_schema", "test_table"), "role", 10, 600, 1031L, true);
    String serializedRequest = StreamingIngestUtils.serializeRequestToString(request, "test");
    String expectedString =
        "{\"database\":\"test_db\",\"schema\":\"test_schema\",\"table\":\"test_table\",\"role\":\"role\",\"count\":10,\"timeout_in_seconds\":600,\"deployment_global_id\":1031,\"is_iceberg\":true}";
    Assert.assertEquals(expectedString, serializedRequest);
  }

  @Test
  public void testValidateDropChannelRequestInternal() {
    DropChannelRequestInternal dropChannelRequest =
        new DropChannelRequestInternal(
            "request_id",
            "test_role",
            "test_db",
            "test_schema",
            "test_table",
            "test_channel",
            false,
            0L);
    String serializedRequest =
        StreamingIngestUtils.serializeRequestToString(dropChannelRequest, "test");
    String expectedString =
        "{\"request_id\":\"request_id\",\"role\":\"test_role\",\"channel\":\"test_channel\",\"table\":\"test_table\",\"database\":\"test_db\",\"schema\":\"test_schema\",\"client_sequencer\":0,\"is_iceberg\":false}";
    Assert.assertEquals(expectedString, serializedRequest);
  }

  @Test
  public void testValidateChannelsStatusRequest() {
    SnowflakeStreamingIngestClientInternal<?> client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client", new SnowflakeURL("snowflake.dev.local:8082"), null, null, true, null, null);
    SnowflakeStreamingIngestChannelInternal<?> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
            "db",
            "schemaName",
            "tableName",
            "0",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            ZoneOffset.UTC,
            null,
            null);

    ChannelsStatusRequest.ChannelStatusRequestDTO dto =
        new ChannelsStatusRequest.ChannelStatusRequestDTO(channel);
    ChannelsStatusRequest request = new ChannelsStatusRequest();
    request.setChannels(Collections.singletonList(dto));
    String serializedRequest = StreamingIngestUtils.serializeRequestToString(request, "test");
    String expectedString =
        "{\"channels\":[{\"database\":\"db\",\"schema\":\"schemaName\",\"table\":\"tableName\",\"channel_name\":\"channel\",\"client_sequencer\":0}],\"role\":null}";
    Assert.assertEquals(expectedString, serializedRequest);
  }

  @Test
  public void testValidateClientConfigureRequest() {
    ClientConfigureRequest clientConfigureRequest = new ClientConfigureRequest("test_role");
    String serializedRequest =
        StreamingIngestUtils.serializeRequestToString(clientConfigureRequest, "test");
    String expectedString = "{\"role\":\"test_role\"}";
    Assert.assertEquals(expectedString, serializedRequest);
  }
}

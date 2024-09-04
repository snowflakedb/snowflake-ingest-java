/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.CHANNEL_CONFIGURE_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.CHANNEL_STATUS_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.CLIENT_CONFIGURE_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.DROP_CHANNEL_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.OPEN_CHANNEL_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.REGISTER_BLOB_ENDPOINT;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.jdbc.internal.apache.commons.io.IOUtils;
import net.snowflake.client.jdbc.internal.apache.http.HttpEntity;
import net.snowflake.client.jdbc.internal.apache.http.HttpStatus;
import net.snowflake.client.jdbc.internal.apache.http.HttpVersion;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpUriRequest;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.message.BasicStatusLine;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.utils.Constants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class SnowflakeServiceClientTest {
  private static final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final RequestBuilder requestBuilder;

  static {
    try {
      requestBuilder = new RequestBuilder("test_host", "test_name", TestUtils.getKeyPair());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final SnowflakeServiceClient snowflakeServiceClient =
      new SnowflakeServiceClient(httpClient, requestBuilder);

  @Before
  public void setUp() throws IOException {
    // set up mock httpclient for testing
    Mockito.doAnswer(
            (Answer<CloseableHttpResponse>)
                invocation -> {
                  HttpUriRequest request = invocation.getArgument(0);
                  switch (request.getURI().getPath()) {
                    case CLIENT_CONFIGURE_ENDPOINT:
                      Map<String, Object> clientConfigresponseMap = new HashMap<>();
                      clientConfigresponseMap.put("prefix", "test_prefix");
                      clientConfigresponseMap.put("status_code", 0L);
                      clientConfigresponseMap.put("message", "OK");
                      clientConfigresponseMap.put("stage_location", getStageLocationMap());
                      clientConfigresponseMap.put("deployment_id", 123L);
                      return buildStreamingIngestResponse(clientConfigresponseMap);
                    case CHANNEL_CONFIGURE_ENDPOINT:
                      Map<String, Object> channelConfigResponseMap = new HashMap<>();
                      channelConfigResponseMap.put("status_code", 0L);
                      channelConfigResponseMap.put("message", "OK");
                      channelConfigResponseMap.put("stage_location", getStageLocationMap());
                      return buildStreamingIngestResponse(channelConfigResponseMap);
                    case OPEN_CHANNEL_ENDPOINT:
                      List<Map<String, Object>> tableColumnsLists = new ArrayList<>();
                      Map<String, Object> tableColumnMap = new HashMap<>();
                      tableColumnMap.put("byteLength", 123L);
                      tableColumnMap.put("length", 0L);
                      tableColumnMap.put("logicalType", "test_logical_type");
                      tableColumnMap.put("name", "test_column");
                      tableColumnMap.put("nullable", true);
                      tableColumnMap.put("precision", 0L);
                      tableColumnMap.put("scale", 0L);
                      tableColumnMap.put("type", "test_type");
                      tableColumnMap.put("ordinal", 0L);
                      tableColumnsLists.add(tableColumnMap);
                      Map<String, Object> openChannelResponseMap = new HashMap<>();
                      openChannelResponseMap.put("status_code", 0L);
                      openChannelResponseMap.put("message", "OK");
                      openChannelResponseMap.put("database", "test_db");
                      openChannelResponseMap.put("schema", "test_schema");
                      openChannelResponseMap.put("table", "test_table");
                      openChannelResponseMap.put("channel", "test_channel");
                      openChannelResponseMap.put("client_sequencer", 123L);
                      openChannelResponseMap.put("row_sequencer", 123L);
                      openChannelResponseMap.put("offset_token", "test_offset_token");
                      openChannelResponseMap.put("table_columns", tableColumnsLists);
                      openChannelResponseMap.put("encryption_key", "test_encryption_key");
                      openChannelResponseMap.put("encryption_key_id", 123L);
                      openChannelResponseMap.put("iceberg_location", getStageLocationMap());
                      return buildStreamingIngestResponse(openChannelResponseMap);
                    case DROP_CHANNEL_ENDPOINT:
                      Map<String, Object> dropChannelResponseMap = new HashMap<>();
                      dropChannelResponseMap.put("status_code", 0L);
                      dropChannelResponseMap.put("message", "OK");
                      dropChannelResponseMap.put("database", "test_db");
                      dropChannelResponseMap.put("schema", "test_schema");
                      dropChannelResponseMap.put("table", "test_table");
                      dropChannelResponseMap.put("channel", "test_channel");
                      return buildStreamingIngestResponse(dropChannelResponseMap);
                    case CHANNEL_STATUS_ENDPOINT:
                      List<Map<String, Object>> channelStatusList = new ArrayList<>();
                      Map<String, Object> channelStatusMap = new HashMap<>();
                      channelStatusMap.put("status_code", 0L);
                      channelStatusMap.put("persisted_row_sequencer", 123L);
                      channelStatusMap.put("persisted_client_sequencer", 123L);
                      channelStatusMap.put("persisted_offset_token", "test_offset_token");
                      Map<String, Object> channelStatusResponseMap = new HashMap<>();
                      channelStatusResponseMap.put("status_code", 0L);
                      channelStatusResponseMap.put("message", "OK");
                      channelStatusResponseMap.put("channels", channelStatusList);
                      return buildStreamingIngestResponse(channelStatusResponseMap);
                    case REGISTER_BLOB_ENDPOINT:
                      List<Map<String, Object>> channelList = new ArrayList<>();
                      Map<String, Object> channelMap = new HashMap<>();
                      channelMap.put("status_code", 0L);
                      channelMap.put("message", "OK");
                      channelMap.put("channel", "test_channel");
                      channelMap.put("client_sequencer", 123L);
                      channelList.add(channelMap);
                      List<Map<String, Object>> chunkList = new ArrayList<>();
                      Map<String, Object> chunkMap = new HashMap<>();
                      chunkMap.put("channels", channelList);
                      chunkMap.put("database", "test_db");
                      chunkMap.put("schema", "test_schema");
                      chunkMap.put("table", "test_table");
                      chunkList.add(chunkMap);
                      List<Map<String, Object>> blobsList = new ArrayList<>();
                      Map<String, Object> blobMap = new HashMap<>();
                      blobMap.put("chunks", chunkList);
                      blobsList.add(blobMap);
                      Map<String, Object> registerBlobResponseMap = new HashMap<>();
                      registerBlobResponseMap.put("status_code", 0L);
                      registerBlobResponseMap.put("message", "OK");
                      registerBlobResponseMap.put("blobs", blobsList);
                      return buildStreamingIngestResponse(registerBlobResponseMap);
                    default:
                      assert false;
                  }
                  return null;
                })
        .when(httpClient)
        .execute(Mockito.any());
  }

  @Test
  public void testClientConfigure() throws IngestResponseException, IOException {
    ClientConfigureRequest clientConfigureRequest = new ClientConfigureRequest("test_role");
    ClientConfigureResponse clientConfigureResponse =
        snowflakeServiceClient.clientConfigure(clientConfigureRequest);
    assert clientConfigureResponse.getStatusCode() == 0L;
    assert clientConfigureResponse.getMessage().equals("OK");
    assert clientConfigureResponse.getPrefix().equals("test_prefix");
    assert clientConfigureResponse.getDeploymentId() == 123L;
  }

  @Test
  public void testChannelConfigure() throws IngestResponseException, IOException {
    ChannelConfigureRequest channelConfigureRequest =
        new ChannelConfigureRequest("test_channel", "test_db", "test_schema", "test_table");
    ChannelConfigureResponse channelConfigureResponse =
        snowflakeServiceClient.channelConfigure(channelConfigureRequest);
    assert channelConfigureResponse.getStatusCode() == 0L;
    assert channelConfigureResponse.getMessage().equals("OK");
  }

  @Test
  public void testOpenChannel() throws IngestResponseException, IOException {
    OpenChannelRequestInternal openChannelRequest =
        new OpenChannelRequestInternal(
            "request_id",
            "test_role",
            "test_db",
            "test_schema",
            "test_table",
            "test_channel",
            Constants.WriteMode.CLOUD_STORAGE,
            "test_offset_token");
    OpenChannelResponse openChannelResponse =
        snowflakeServiceClient.openChannel(openChannelRequest);
    assert openChannelResponse.getStatusCode() == 0L;
    assert openChannelResponse.getMessage().equals("OK");
    assert openChannelResponse.getDBName().equals("test_db");
    assert openChannelResponse.getSchemaName().equals("test_schema");
    assert openChannelResponse.getTableName().equals("test_table");
    assert openChannelResponse.getChannelName().equals("test_channel");
    assert openChannelResponse.getClientSequencer() == 123L;
    assert openChannelResponse.getRowSequencer() == 123L;
    assert openChannelResponse.getOffsetToken().equals("test_offset_token");
    assert openChannelResponse.getTableColumns().size() == 1;
    assert openChannelResponse.getEncryptionKey().equals("test_encryption_key");
    assert openChannelResponse.getEncryptionKeyId() == 123L;
  }

  @Test
  public void testDropChannel() throws IngestResponseException, IOException {
    DropChannelRequestInternal dropChannelRequest =
        new DropChannelRequestInternal(
            "request_id", "test_role", "test_db", "test_schema", "test_table", "test_channel", 0L);
    DropChannelResponse dropChannelResponse =
        snowflakeServiceClient.dropChannel(dropChannelRequest);
    assert dropChannelResponse.getStatusCode() == 0L;
    assert dropChannelResponse.getMessage().equals("OK");
    assert dropChannelResponse.getDBName().equals("test_db");
    assert dropChannelResponse.getSchemaName().equals("test_schema");
    assert dropChannelResponse.getTableName().equals("test_table");
    assert dropChannelResponse.getChannelName().equals("test_channel");
  }

  private static Map<String, Object> getStageLocationMap() {
    Map<String, Object> credsMap = new HashMap<>();
    credsMap.put("AWS_ID", "test_id");
    credsMap.put("AWS_KEY", "test_key");

    Map<String, Object> stageLocationMap = new HashMap<>();
    stageLocationMap.put("locationType", "S3");
    stageLocationMap.put("location", "test_location");
    stageLocationMap.put("path", "test_path");
    stageLocationMap.put("creds", credsMap);
    stageLocationMap.put("region", "test_region");
    stageLocationMap.put("endPoint", "test_endpoint");
    stageLocationMap.put("storageAccount", "test_storage_account");
    stageLocationMap.put("presignedUrl", "test_presigned_url");
    stageLocationMap.put("isClientSideEncrypted", true);
    stageLocationMap.put("useS3RegionalUrl", true);
    stageLocationMap.put("volumeHash", "test_volume_hash");
    return stageLocationMap;
  }

  private static CloseableHttpResponse buildStreamingIngestResponse(Map<String, Object> payload)
      throws IOException {
    CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);

    Mockito.when(response.getStatusLine())
        .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK"));
    Mockito.when(response.getEntity()).thenReturn(httpEntity);
    Mockito.when(httpEntity.getContent())
        .thenReturn(IOUtils.toInputStream(objectMapper.writeValueAsString(payload)));

    return response;
  }
}

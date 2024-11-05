/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.IOException;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.utils.Constants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SnowflakeServiceClientTest {
  @Parameterized.Parameters(name = "enableIcebergStreaming: {0}")
  public static Object[] enableIcebergStreaming() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public boolean enableIcebergStreaming;

  private SnowflakeServiceClient snowflakeServiceClient;

  @Before
  public void setUp() {
    snowflakeServiceClient = MockSnowflakeServiceClient.create(enableIcebergStreaming);
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
  public void testGeneratePresignedUrls() throws IngestResponseException, IOException {
    GeneratePresignedUrlsRequest request =
        new GeneratePresignedUrlsRequest(
            new TableRef("test_db", "test_schema", "test_table"), "role", 10, 600, 1031L, true);
    GeneratePresignedUrlsResponse response = snowflakeServiceClient.generatePresignedUrls(request);
    assert response.getStatusCode() == 0L;
    assert response.getMessage().equals("OK");
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
            enableIcebergStreaming,
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
            "request_id",
            "test_role",
            "test_db",
            "test_schema",
            "test_table",
            "test_channel",
            enableIcebergStreaming,
            0L);
    DropChannelResponse dropChannelResponse =
        snowflakeServiceClient.dropChannel(dropChannelRequest);
    assert dropChannelResponse.getStatusCode() == 0L;
    assert dropChannelResponse.getMessage().equals("OK");
    assert dropChannelResponse.getDBName().equals("test_db");
    assert dropChannelResponse.getSchemaName().equals("test_schema");
    assert dropChannelResponse.getTableName().equals("test_table");
    assert dropChannelResponse.getChannelName().equals("test_channel");
  }
}

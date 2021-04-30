package net.snowflake.ingest.streaming.internal;

import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.*;
import net.snowflake.client.jdbc.internal.amazonaws.util.IOUtils;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.ingest.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  TestUtils.class,
  HttpUtil.class,
  SnowflakeFileTransferAgent.class,
})
public class SnowflakeInternalStageTest {
  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testPutRemote() throws Exception {
    PowerMockito.mockStatic(TestUtils.class);
    SnowflakeConnectionV1 connection = Mockito.mock(SnowflakeConnectionV1.class);
    String exampleMeta =
        "{\"data\": {\"src_locations\": [\"foo/\"],\"status_code\": 0, \"message\": \"Success\", \"prefix\": \"EXAMPLE_PREFIX\", \"stageInfo\": {\"locationType\": \"S3\", \"location\": \"foo/streaming_ingest/\", \"path\": \"streaming_ingest/\", \"region\": \"us-east-1\", \"storageAccount\": null, \"isClientSideEncrypted\": true, \"creds\": {\"AWS_KEY_ID\": \"EXAMPLE_AWS_KEY_ID\", \"AWS_SECRET_KEY\": \"EXAMPLE_AWS_SECRET_KEY\", \"AWS_TOKEN\": \"EXAMPLE_AWS_TOKEN\", \"AWS_ID\": \"EXAMPLE_AWS_ID\", \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"}, \"presignedUrl\": null, \"endPoint\": null}}}";
    JsonNode exampleJson = mapper.readTree(exampleMeta);
    SnowflakeFileTransferMetadataV1 originalMetadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleJson).get(0);

    byte[] dataBytes = "Hello Upload".getBytes(StandardCharsets.UTF_8);

    PowerMockito.mockStatic(SnowflakeFileTransferAgent.class);
    SnowflakeInternalStage stage = new SnowflakeInternalStage(connection, originalMetadata);
    final ArgumentCaptor<SnowflakeFileTransferConfig> captor =
        ArgumentCaptor.forClass(SnowflakeFileTransferConfig.class);

    stage.putRemote("test/path", dataBytes);
    PowerMockito.verifyStatic(SnowflakeFileTransferAgent.class);
    SnowflakeFileTransferAgent.uploadWithoutConnection(captor.capture());
    SnowflakeFileTransferConfig capturedConfig = captor.getValue();

    Assert.assertEquals(false, capturedConfig.getRequireCompress());
    Assert.assertEquals(OCSPMode.FAIL_OPEN, capturedConfig.getOcspMode());
    Assert.assertEquals(originalMetadata, capturedConfig.getSnowflakeFileTransferMetadata());

    InputStream capturedInput = capturedConfig.getUploadStream();
    Assert.assertEquals("Hello Upload", IOUtils.toString(capturedInput));
  }

  @Test
  public void testRefreshSnowflakeMetadata() throws Exception {
    PowerMockito.mockStatic(HttpUtil.class);
    PowerMockito.mockStatic(TestUtils.class);
    PowerMockito.mockStatic(SnowflakeFileTransferAgent.class);

    ArrayList<SnowflakeFileTransferMetadata> mockList = new ArrayList<>();
    mockList.add(Mockito.mock(SnowflakeFileTransferMetadata.class));
    PowerMockito.when(SnowflakeFileTransferAgent.getFileTransferMetadatas(Mockito.any()))
        .thenReturn(mockList);

    SnowflakeConnectionV1 connection = Mockito.mock(SnowflakeConnectionV1.class);
    SFSession sfSession = Mockito.mock(SFSession.class);
    SnowflakeFileTransferMetadataV1 originalMetadata =
        Mockito.mock(SnowflakeFileTransferMetadataV1.class);

    SnowflakeConnectString connectString = Mockito.mock(SnowflakeConnectString.class);
    Mockito.when(connectString.getScheme()).thenReturn("http");
    Mockito.when(connectString.getHost()).thenReturn("EXAMPLE_HOST");
    Mockito.when(connectString.getPort()).thenReturn(123);

    Mockito.when(sfSession.getSessionToken()).thenReturn("EXAMPLE_SESSION_TOKEN");
    Mockito.when(sfSession.getSnowflakeConnectionString()).thenReturn(connectString);

    Mockito.when(connection.getSfSession()).thenReturn(sfSession);

    String exampleMeta =
        "{\"data\": {\"src_locations\": [\"foo/\"],\"status_code\": 0, \"message\": \"Success\", \"prefix\": \"EXAMPLE_PREFIX\", \"stageInfo\": {\"locationType\": \"S3\", \"location\": \"foo/streaming_ingest/\", \"path\": \"streaming_ingest/\", \"region\": \"us-east-1\", \"storageAccount\": null, \"isClientSideEncrypted\": true, \"creds\": {\"AWS_KEY_ID\": \"EXAMPLE_AWS_KEY_ID\", \"AWS_SECRET_KEY\": \"EXAMPLE_AWS_SECRET_KEY\", \"AWS_TOKEN\": \"EXAMPLE_AWS_TOKEN\", \"AWS_ID\": \"EXAMPLE_AWS_ID\", \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"}, \"presignedUrl\": null, \"endPoint\": null}}}";

    String exampleMetaResponse =
        "{\"src_locations\": [\"foo/\"],\"status_code\": 0, \"message\": \"Success\", \"prefix\": \"EXAMPLE_PREFIX\", \"stage_location\": {\"locationType\": \"S3\", \"location\": \"foo/streaming_ingest/\", \"path\": \"streaming_ingest/\", \"region\": \"us-east-1\", \"storageAccount\": null, \"isClientSideEncrypted\": true, \"creds\": {\"AWS_KEY_ID\": \"EXAMPLE_AWS_KEY_ID\", \"AWS_SECRET_KEY\": \"EXAMPLE_AWS_SECRET_KEY\", \"AWS_TOKEN\": \"EXAMPLE_AWS_TOKEN\", \"AWS_ID\": \"EXAMPLE_AWS_ID\", \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"}, \"presignedUrl\": null, \"endPoint\": null}}";

    JsonNode exampleJson = mapper.readTree(exampleMeta);

    SnowflakeInternalStage stage = new SnowflakeInternalStage(connection, originalMetadata);
    PowerMockito.when(
            HttpUtil.executeGeneralRequest(Mockito.any(), Mockito.anyInt(), Mockito.any()))
        .thenReturn(exampleMetaResponse);

    stage.refreshSnowflakeMetadata();

    PowerMockito.verifyStatic(HttpUtil.class);
    final ArgumentCaptor<HttpPost> postCaptor = ArgumentCaptor.forClass(HttpPost.class);
    final ArgumentCaptor<Integer> retryCaptor = ArgumentCaptor.forClass(Integer.class);
    final ArgumentCaptor<OCSPMode> ocspCaptor = ArgumentCaptor.forClass(OCSPMode.class);

    HttpUtil.executeGeneralRequest(
        postCaptor.capture(), retryCaptor.capture(), ocspCaptor.capture());
    Assert.assertEquals(
        new URI("http://EXAMPLE_HOST:123/v1/streaming/client/configure"),
        postCaptor.getValue().getURI());
    Assert.assertEquals(
        "application/json", postCaptor.getValue().getHeaders("accept")[0].getValue());
    Assert.assertEquals(
        "Snowflake token=\"EXAMPLE_SESSION_TOKEN\"",
        postCaptor.getValue().getHeaders("Authorization")[0].getValue());

    PowerMockito.verifyStatic(SnowflakeFileTransferAgent.class);
    final ArgumentCaptor<JsonNode> jsonCaptor = ArgumentCaptor.forClass(JsonNode.class);
    SnowflakeFileTransferAgent.getFileTransferMetadatas(jsonCaptor.capture());
    Assert.assertEquals(
        exampleJson.get("data").get("stageInfo"),
        jsonCaptor.getValue().get("data").get("stageInfo"));
  }
}

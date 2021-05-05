package net.snowflake.ingest.streaming.internal;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.jdbc.*;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.internal.amazonaws.util.IOUtils;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.ingest.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  TestUtils.class,
  HttpUtil.class,
  SnowflakeFileTransferAgent.class,
})
public class StreamingIngestStageTest {
  private ObjectMapper mapper = new ObjectMapper();
  final SnowflakeURL snowflakeURL =
      new SnowflakeURL.SnowflakeURLBuilder()
          .setUrl("EXAMPLE_HOST")
          .setSsl(false)
          .setPort(123)
          .setAccount("EXAMPLE_ACCOUNT")
          .build();

  final String exampleMeta =
      "{\"data\": {\"src_locations\": [\"placeholder/\"],\"status_code\": 0, \"message\": \"Success\", \"prefix\": \"EXAMPLE_PREFIX\", \"stageInfo\": {\"locationType\": \"S3\", \"location\": \"foo/streaming_ingest/\", \"path\": \"streaming_ingest/\", \"region\": \"us-east-1\", \"storageAccount\": null, \"isClientSideEncrypted\": true, \"creds\": {\"AWS_KEY_ID\": \"EXAMPLE_AWS_KEY_ID\", \"AWS_SECRET_KEY\": \"EXAMPLE_AWS_SECRET_KEY\", \"AWS_TOKEN\": \"EXAMPLE_AWS_TOKEN\", \"AWS_ID\": \"EXAMPLE_AWS_ID\", \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"}, \"presignedUrl\": null, \"endPoint\": null}}}";

  private void setupMocksForRefresh(long httpDelay) throws Exception {
    PowerMockito.mockStatic(HttpUtil.class);
    PowerMockito.mockStatic(TestUtils.class);

    String exampleMetaResponse =
        "{\"src_locations\": [\"foo/\"],\"status_code\": 0, \"message\": \"Success\", \"prefix\": \"EXAMPLE_PREFIX\", \"stage_location\": {\"locationType\": \"S3\", \"location\": \"foo/streaming_ingest/\", \"path\": \"streaming_ingest/\", \"region\": \"us-east-1\", \"storageAccount\": null, \"isClientSideEncrypted\": true, \"creds\": {\"AWS_KEY_ID\": \"EXAMPLE_AWS_KEY_ID\", \"AWS_SECRET_KEY\": \"EXAMPLE_AWS_SECRET_KEY\", \"AWS_TOKEN\": \"EXAMPLE_AWS_TOKEN\", \"AWS_ID\": \"EXAMPLE_AWS_ID\", \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"}, \"presignedUrl\": null, \"endPoint\": null}}";

    PowerMockito.when(
            HttpUtil.executeGeneralRequest(Mockito.any(), Mockito.anyInt(), Mockito.any()))
        .thenAnswer(
            (Answer<String>)
                invocationOnMock -> {
                  Thread.sleep(httpDelay);
                  return exampleMetaResponse;
                });
  }

  @Test
  public void testPutRemote() throws Exception {
    JsonNode exampleJson = mapper.readTree(exampleMeta);
    SnowflakeFileTransferMetadataV1 originalMetadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleJson).get(0);

    setupMocksForRefresh(0);
    byte[] dataBytes = "Hello Upload".getBytes(StandardCharsets.UTF_8);

    StreamingIngestStage stage = new StreamingIngestStage(snowflakeURL);
    PowerMockito.mockStatic(SnowflakeFileTransferAgent.class);

    //    PowerMockito.doThrow(new NullPointerException()).when(SnowflakeFileTransferAgent.class);
    //    SnowflakeFileTransferAgent.uploadWithoutConnection(Mockito.any());
    final ArgumentCaptor<SnowflakeFileTransferConfig> captor =
        ArgumentCaptor.forClass(SnowflakeFileTransferConfig.class);

    stage.putRemote("test/path", dataBytes);
    PowerMockito.verifyStatic(SnowflakeFileTransferAgent.class);
    SnowflakeFileTransferAgent.uploadWithoutConnection(captor.capture());
    SnowflakeFileTransferConfig capturedConfig = captor.getValue();

    Assert.assertEquals(false, capturedConfig.getRequireCompress());
    Assert.assertEquals(OCSPMode.FAIL_OPEN, capturedConfig.getOcspMode());

    SnowflakeFileTransferMetadataV1 capturedMetadata =
        (SnowflakeFileTransferMetadataV1) capturedConfig.getSnowflakeFileTransferMetadata();
    Assert.assertEquals("test/path", capturedMetadata.getPresignedUrlFileName());
    Assert.assertEquals(originalMetadata.getCommandType(), capturedMetadata.getCommandType());
    Assert.assertEquals(originalMetadata.getPresignedUrl(), capturedMetadata.getPresignedUrl());
    Assert.assertEquals(
        originalMetadata.getStageInfo().getStageType(),
        capturedMetadata.getStageInfo().getStageType());

    InputStream capturedInput = capturedConfig.getUploadStream();
    Assert.assertEquals("Hello Upload", IOUtils.toString(capturedInput));
  }

  @Test
  public void testPutRemoteRefreshes() throws Exception {
    JsonNode exampleJson = mapper.readTree(exampleMeta);
    SnowflakeFileTransferMetadataV1 originalMetadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleJson).get(0);

    setupMocksForRefresh(0);
    byte[] dataBytes = "Hello Upload".getBytes(StandardCharsets.UTF_8);

    StreamingIngestStage stage = new StreamingIngestStage(snowflakeURL);
    PowerMockito.mockStatic(SnowflakeFileTransferAgent.class);

    PowerMockito.doThrow(new NullPointerException()).when(SnowflakeFileTransferAgent.class);
    SnowflakeFileTransferAgent.uploadWithoutConnection(Mockito.any());
    final ArgumentCaptor<SnowflakeFileTransferConfig> captor =
        ArgumentCaptor.forClass(SnowflakeFileTransferConfig.class);

    try {
      stage.putRemote("test/path", dataBytes);
      Assert.assertTrue(false);
    } catch (NullPointerException npe) {
      // Expected behavior given mocked response
    }
    PowerMockito.verifyStatic(
        SnowflakeFileTransferAgent.class, Mockito.times(StreamingIngestStage.MAX_RETRY_COUNT + 1));
    SnowflakeFileTransferAgent.uploadWithoutConnection(captor.capture());
    SnowflakeFileTransferConfig capturedConfig = captor.getValue();

    Assert.assertEquals(false, capturedConfig.getRequireCompress());
    Assert.assertEquals(OCSPMode.FAIL_OPEN, capturedConfig.getOcspMode());

    SnowflakeFileTransferMetadataV1 capturedMetadata =
        (SnowflakeFileTransferMetadataV1) capturedConfig.getSnowflakeFileTransferMetadata();
    Assert.assertEquals("test/path", capturedMetadata.getPresignedUrlFileName());
    Assert.assertEquals(originalMetadata.getCommandType(), capturedMetadata.getCommandType());
    Assert.assertEquals(originalMetadata.getPresignedUrl(), capturedMetadata.getPresignedUrl());
    Assert.assertEquals(
        originalMetadata.getStageInfo().getStageType(),
        capturedMetadata.getStageInfo().getStageType());

    InputStream capturedInput = capturedConfig.getUploadStream();
    Assert.assertEquals("Hello Upload", IOUtils.toString(capturedInput));
  }

  @Test
  public void testRefreshSnowflakeMetadata() throws Exception {
    setupMocksForRefresh(0);
    StreamingIngestStage stage = new StreamingIngestStage(snowflakeURL);

    StreamingIngestStage.SnowflakeFileTransferMetadataWithAge metadataWithAge =
        stage.refreshSnowflakeMetadata(true);

    PowerMockito.verifyStatic(HttpUtil.class, Mockito.times(2));
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

    Assert.assertTrue(metadataWithAge.timestamp.isPresent());
    Assert.assertEquals(
        StageInfo.StageType.S3, metadataWithAge.fileTransferMetadata.getStageInfo().getStageType());
    Assert.assertEquals(
        "foo/streaming_ingest/", metadataWithAge.fileTransferMetadata.getStageInfo().getLocation());
    Assert.assertEquals(
        "placeholder", metadataWithAge.fileTransferMetadata.getPresignedUrlFileName());
  }

  @Test
  public void testRefreshSnowflakeMetadataSynchronized() throws Exception {
    setupMocksForRefresh(100);
    StreamingIngestStage stage = new StreamingIngestStage(snowflakeURL);
    // Set the age of the metadata so it will be refreshed
    stage.setFileTransferMetadataAge(0L);

    ThreadFactory buildUploadThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("ingest-build-upload-thread-%d").build();
    int buildUploadThreadCount = 2;
    ExecutorService workers =
        Executors.newFixedThreadPool(buildUploadThreadCount, buildUploadThreadFactory);

    StreamingIngestStage.SnowflakeFileTransferMetadataWithAge metadataWithAge;
    workers.submit(
        () -> {
          try {
            stage.refreshSnowflakeMetadata();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    workers.submit(
        () -> {
          try {
            stage.refreshSnowflakeMetadata();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    workers.awaitTermination(150, TimeUnit.MILLISECONDS);

    PowerMockito.verifyStatic(HttpUtil.class, Mockito.times(2));
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
  }
}

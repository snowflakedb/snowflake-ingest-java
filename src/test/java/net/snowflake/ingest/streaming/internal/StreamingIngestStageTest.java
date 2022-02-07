package net.snowflake.ingest.streaming.internal;

import static org.mockito.Mockito.times;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeFileTransferConfig;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.internal.amazonaws.util.IOUtils;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.google.common.util.concurrent.ThreadFactoryBuilder;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.utils.Constants;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.entity.BasicHttpEntity;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TestUtils.class, HttpUtil.class, SnowflakeFileTransferAgent.class})
public class StreamingIngestStageTest {
  private ObjectMapper mapper = new ObjectMapper();

  final String exampleRemoteMeta =
      "{\"data\": {\"src_locations\": [\"placeholder/\"],\"status_code\": 0, \"message\":"
          + " \"Success\", \"prefix\": \"EXAMPLE_PREFIX\", \"stageInfo\": {\"locationType\":"
          + " \"S3\", \"location\": \"foo/streaming_ingest/\", \"path\": \"streaming_ingest/\","
          + " \"region\": \"us-east-1\", \"storageAccount\": null, \"isClientSideEncrypted\":"
          + " true, \"creds\": {\"AWS_KEY_ID\": \"EXAMPLE_AWS_KEY_ID\", \"AWS_SECRET_KEY\":"
          + " \"EXAMPLE_AWS_SECRET_KEY\", \"AWS_TOKEN\": \"EXAMPLE_AWS_TOKEN\", \"AWS_ID\":"
          + " \"EXAMPLE_AWS_ID\", \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"}, \"presignedUrl\": null,"
          + " \"endPoint\": null}}}";

  final String exampleRemoteMetaGCS =
      "{\"data\": {\"src_locations\": [\"placeholder/\"],\"status_code\": 0, \"message\":"
          + " \"Success\", \"prefix\": \"EXAMPLE_PREFIX\", \"stageInfo\": {\"locationType\":"
          + " \"GCS\", \"location\": \"foo/streaming_ingest/\", \"path\": \"streaming_ingest/\","
          + " \"region\": \"us-east-1\", \"storageAccount\": null, \"isClientSideEncrypted\":"
          + " true, \"creds\": {\"AWS_KEY_ID\": \"EXAMPLE_AWS_KEY_ID\", \"AWS_SECRET_KEY\":"
          + " \"EXAMPLE_AWS_SECRET_KEY\", \"AWS_TOKEN\": \"EXAMPLE_AWS_TOKEN\", \"AWS_ID\":"
          + " \"EXAMPLE_AWS_ID\", \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"}, \"presignedUrl\": null,"
          + " \"endPoint\": null}}}";

  String exampleRemoteMetaResponse =
      "{\"src_locations\": [\"foo/\"],\"status_code\": 0, \"message\": \"Success\", \"prefix\":"
          + " \"EXAMPLE_PREFIX\", \"stage_location\": {\"locationType\": \"S3\", \"location\":"
          + " \"foo/streaming_ingest/\", \"path\": \"streaming_ingest/\", \"region\":"
          + " \"us-east-1\", \"storageAccount\": null, \"isClientSideEncrypted\": true,"
          + " \"creds\": {\"AWS_KEY_ID\": \"EXAMPLE_AWS_KEY_ID\", \"AWS_SECRET_KEY\":"
          + " \"EXAMPLE_AWS_SECRET_KEY\", \"AWS_TOKEN\": \"EXAMPLE_AWS_TOKEN\", \"AWS_ID\":"
          + " \"EXAMPLE_AWS_ID\", \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"}, \"presignedUrl\": null,"
          + " \"endPoint\": null}}";

  private void setupMocksForRefresh() throws Exception {
    PowerMockito.mockStatic(HttpUtil.class);
    PowerMockito.mockStatic(TestUtils.class);
  }

  @Test
  public void testPutRemote() throws Exception {
    JsonNode exampleJson = mapper.readTree(exampleRemoteMeta);
    SnowflakeFileTransferMetadataV1 originalMetadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleJson).get(0);

    byte[] dataBytes = "Hello Upload".getBytes(StandardCharsets.UTF_8);

    StreamingIngestStage stage =
        new StreamingIngestStage(
            true,
            "role",
            null,
            null,
            "clientName",
            new StreamingIngestStage.SnowflakeFileTransferMetadataWithAge(
                originalMetadata, Optional.of(System.currentTimeMillis())));
    PowerMockito.mockStatic(SnowflakeFileTransferAgent.class);

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
  public void testPutLocal() throws Exception {
    byte[] dataBytes = "Hello Upload".getBytes(StandardCharsets.UTF_8);
    String fullFilePath = "testOutput";
    String fileName = "putLocalOutput";

    StreamingIngestStage stage =
        Mockito.spy(
            new StreamingIngestStage(
                true,
                "role",
                null,
                null,
                "clientName",
                new StreamingIngestStage.SnowflakeFileTransferMetadataWithAge(
                    fullFilePath, Optional.of(System.currentTimeMillis()))));
    Mockito.doReturn(true).when(stage).isLocalFS();

    stage.put(fileName, dataBytes);
    Path outputPath = Paths.get(fullFilePath, fileName);
    List<String> output = Files.readAllLines(outputPath);
    Assert.assertEquals(1, output.size());
    Assert.assertEquals("Hello Upload", output.get(0));
  }

  @Test
  public void testPutRemoteRefreshes() throws Exception {
    JsonNode exampleJson = mapper.readTree(exampleRemoteMeta);
    SnowflakeFileTransferMetadataV1 originalMetadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleJson).get(0);

    byte[] dataBytes = "Hello Upload".getBytes(StandardCharsets.UTF_8);

    StreamingIngestStage stage =
        new StreamingIngestStage(
            true,
            "role",
            null,
            null,
            "clientName",
            new StreamingIngestStage.SnowflakeFileTransferMetadataWithAge(
                originalMetadata, Optional.of(System.currentTimeMillis())));
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
        SnowflakeFileTransferAgent.class, times(StreamingIngestStage.MAX_RETRY_COUNT + 1));
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
  public void testPutRemoteGCS() throws Exception {
    JsonNode exampleJson = mapper.readTree(exampleRemoteMetaGCS);
    SnowflakeFileTransferMetadataV1 originalMetadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleJson).get(0);

    byte[] dataBytes = "Hello Upload".getBytes(StandardCharsets.UTF_8);

    StreamingIngestStage stage =
        Mockito.spy(
            new StreamingIngestStage(
                true,
                "role",
                null,
                null,
                "clientName",
                new StreamingIngestStage.SnowflakeFileTransferMetadataWithAge(
                    originalMetadata, Optional.of(System.currentTimeMillis()))));
    PowerMockito.mockStatic(SnowflakeFileTransferAgent.class);
    SnowflakeFileTransferMetadataV1 metaMock = Mockito.mock(SnowflakeFileTransferMetadataV1.class);

    Mockito.doReturn(metaMock).when(stage).fetchSignedURL(Mockito.any());
    stage.putRemote("test/path", dataBytes);
    SnowflakeFileTransferAgent.uploadWithoutConnection(Mockito.any());
    Mockito.verify(stage, times(1)).fetchSignedURL("test/path");
  }

  @Test
  public void testRefreshSnowflakeMetadataRemote() throws Exception {
    RequestBuilder mockBuilder = Mockito.mock(RequestBuilder.class);
    HttpClient mockClient = Mockito.mock(HttpClient.class);
    HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
    StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
    Mockito.when(mockStatusLine.getStatusCode()).thenReturn(200);

    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(
        new ByteArrayInputStream(exampleRemoteMetaResponse.getBytes(StandardCharsets.UTF_8)));

    Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);
    Mockito.when(mockClient.execute(Mockito.any())).thenReturn(mockResponse);

    StreamingIngestStage stage =
        new StreamingIngestStage(true, "role", mockClient, mockBuilder, "clientName");

    StreamingIngestStage.SnowflakeFileTransferMetadataWithAge metadataWithAge =
        stage.refreshSnowflakeMetadata(true);

    final ArgumentCaptor<String> endpointCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map<Object, Object>> mapCaptor = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(mockBuilder)
        .generateStreamingIngestPostRequest(
            mapCaptor.capture(), endpointCaptor.capture(), Mockito.any());
    Assert.assertEquals(Constants.CLIENT_CONFIGURE_ENDPOINT, endpointCaptor.getValue());
    Assert.assertTrue(metadataWithAge.timestamp.isPresent());
    Assert.assertEquals(
        StageInfo.StageType.S3, metadataWithAge.fileTransferMetadata.getStageInfo().getStageType());
    Assert.assertEquals(
        "foo/streaming_ingest/", metadataWithAge.fileTransferMetadata.getStageInfo().getLocation());
    Assert.assertEquals(
        "placeholder", metadataWithAge.fileTransferMetadata.getPresignedUrlFileName());
  }

  @Test
  public void testFetchSignedURL() throws Exception {
    RequestBuilder mockBuilder = Mockito.mock(RequestBuilder.class);
    HttpClient mockClient = Mockito.mock(HttpClient.class);
    HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
    StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
    Mockito.when(mockStatusLine.getStatusCode()).thenReturn(200);

    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(
        new ByteArrayInputStream(exampleRemoteMetaResponse.getBytes(StandardCharsets.UTF_8)));

    Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);
    Mockito.when(mockClient.execute(Mockito.any())).thenReturn(mockResponse);

    StreamingIngestStage stage =
        new StreamingIngestStage(true, "role", mockClient, mockBuilder, "clientName");

    SnowflakeFileTransferMetadataV1 metadata = stage.fetchSignedURL("path/fileName");

    final ArgumentCaptor<String> endpointCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map<Object, Object>> mapCaptor = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(mockBuilder)
        .generateStreamingIngestPostRequest(
            mapCaptor.capture(), endpointCaptor.capture(), Mockito.any());
    Assert.assertEquals(Constants.CLIENT_CONFIGURE_ENDPOINT, endpointCaptor.getValue());
    Assert.assertEquals(StageInfo.StageType.S3, metadata.getStageInfo().getStageType());
    Assert.assertEquals("foo/streaming_ingest/", metadata.getStageInfo().getLocation());
    Assert.assertEquals("path/fileName", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testRefreshSnowflakeMetadataSynchronized() throws Exception {
    setupMocksForRefresh();
    JsonNode exampleJson = mapper.readTree(exampleRemoteMeta);
    SnowflakeFileTransferMetadataV1 originalMetadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleJson).get(0);

    RequestBuilder mockBuilder = Mockito.mock(RequestBuilder.class);
    HttpClient mockClient = Mockito.mock(HttpClient.class);
    HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
    StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
    Mockito.when(mockStatusLine.getStatusCode()).thenReturn(200);

    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(
        new ByteArrayInputStream(exampleRemoteMetaResponse.getBytes(StandardCharsets.UTF_8)));

    Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    Mockito.when(mockResponse.getEntity()).thenReturn(entity);
    Mockito.when(mockClient.execute(Mockito.any())).thenReturn(mockResponse);

    StreamingIngestStage stage =
        new StreamingIngestStage(
            true,
            "role",
            mockClient,
            mockBuilder,
            "clientName",
            new StreamingIngestStage.SnowflakeFileTransferMetadataWithAge(
                originalMetadata, Optional.of(0L)));

    ThreadFactory buildUploadThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("ingest-build-upload-thread-%d").build();
    int buildUploadThreadCount = 2;
    ExecutorService workers =
        Executors.newFixedThreadPool(buildUploadThreadCount, buildUploadThreadFactory);

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

    Mockito.verify(mockClient).execute(Mockito.any());
  }
}

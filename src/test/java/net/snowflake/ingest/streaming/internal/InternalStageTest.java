/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.client.core.Constants.CLOUD_STORAGE_CREDENTIALS_EXPIRED;
import static net.snowflake.ingest.utils.Constants.CLIENT_CONFIGURE_ENDPOINT;
import static net.snowflake.ingest.utils.HttpUtil.HTTP_PROXY_PASSWORD;
import static net.snowflake.ingest.utils.HttpUtil.HTTP_PROXY_USER;
import static net.snowflake.ingest.utils.HttpUtil.NON_PROXY_HOSTS;
import static net.snowflake.ingest.utils.HttpUtil.PROXY_HOST;
import static net.snowflake.ingest.utils.HttpUtil.PROXY_PORT;
import static net.snowflake.ingest.utils.HttpUtil.USE_PROXY;
import static net.snowflake.ingest.utils.HttpUtil.generateProxyPropertiesForJDBC;
import static net.snowflake.ingest.utils.HttpUtil.shouldBypassProxy;
import static org.mockito.Mockito.times;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeFileTransferConfig;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.internal.amazonaws.util.IOUtils;
import net.snowflake.client.jdbc.internal.apache.http.HttpEntity;
import net.snowflake.client.jdbc.internal.apache.http.StatusLine;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.entity.BasicHttpEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.google.common.util.concurrent.ThreadFactoryBuilder;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
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
public class InternalStageTest {

  private final String prefix = "EXAMPLE_PREFIX";

  private final long deploymentId = 123;

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
          + " \"EXAMPLE_AWS_ID\", \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"}, \"presignedUrl\":"
          + " \"presignedUrl\", \"endPoint\": null}}}";

  String exampleRemoteMetaResponse =
      "{\"src_locations\": [\"foo/\"],"
          + " \"deployment_id\": "
          + deploymentId
          + ","
          + " \"status_code\": 0, \"message\": \"Success\", \"prefix\":"
          + " \""
          + prefix
          + "\", \"stage_location\": {\"locationType\": \"S3\", \"location\":"
          + " \"foo/streaming_ingest/\", \"path\": \"streaming_ingest/\", \"region\":"
          + " \"us-east-1\", \"storageAccount\": null, \"isClientSideEncrypted\": true,"
          + " \"creds\": {\"AWS_KEY_ID\": \"EXAMPLE_AWS_KEY_ID\", \"AWS_SECRET_KEY\":"
          + " \"EXAMPLE_AWS_SECRET_KEY\", \"AWS_TOKEN\": \"EXAMPLE_AWS_TOKEN\", \"AWS_ID\":"
          + " \"EXAMPLE_AWS_ID\", \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"}, \"presignedUrl\": null,"
          + " \"endPoint\": null}}";
  String remoteMetaResponseDifferentDeployment =
      "{\"src_locations\": [\"foo/\"],"
          + " \"deployment_id\": "
          + (deploymentId + 1)
          + ","
          + " \"status_code\": 0, \"message\": \"Success\", \"prefix\":"
          + " \""
          + prefix
          + "\", \"stage_location\": {\"locationType\": \"S3\", \"location\":"
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

    InternalStageManager storageManager = Mockito.mock(InternalStageManager.class);
    Mockito.when(storageManager.getClientPrefix()).thenReturn("testPrefix");

    InternalStage stage =
        new InternalStage(
            storageManager,
            "clientName",
            "testPrefix",
            InternalStageManager.NO_TABLE_REF,
            false /* useIcebergFileTransferAgent */,
            new SnowflakeFileTransferMetadataWithAge(
                originalMetadata, Optional.of(System.currentTimeMillis())),
            1);
    PowerMockito.mockStatic(SnowflakeFileTransferAgent.class);

    final ArgumentCaptor<SnowflakeFileTransferConfig> captor =
        ArgumentCaptor.forClass(SnowflakeFileTransferConfig.class);

    stage.put(
        new BlobPath("test/path" /* uploadPath */, "test/path" /* fileRegistrationPath */),
        dataBytes);
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

    InternalStage stage =
        Mockito.spy(
            new InternalStage(
                null,
                "clientName",
                "testPrefix",
                InternalStageManager.NO_TABLE_REF,
                false /* useIcebergFileTransferAgent */,
                new SnowflakeFileTransferMetadataWithAge(
                    fullFilePath, Optional.of(System.currentTimeMillis())),
                1));
    Mockito.doReturn(true).when(stage).isLocalFS();

    stage.put(
        new BlobPath(fileName /* uploadPath */, fileName /* fileRegistrationPath */), dataBytes);
    Path outputPath = Paths.get(fullFilePath, fileName);
    List<String> output = Files.readAllLines(outputPath);
    Assert.assertEquals(1, output.size());
    Assert.assertEquals("Hello Upload", output.get(0));
  }

  @Test
  public void doTestPutRemoteRefreshes() throws Exception {
    int maxUploadRetryCount = 2;
    JsonNode exampleJson = mapper.readTree(exampleRemoteMeta);
    SnowflakeFileTransferMetadataV1 originalMetadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleJson).get(0);

    byte[] dataBytes = "Hello Upload".getBytes(StandardCharsets.UTF_8);

    InternalStageManager storageManager = Mockito.mock(InternalStageManager.class);
    Mockito.when(storageManager.getClientPrefix()).thenReturn("testPrefix");

    InternalStage stage =
        new InternalStage(
            storageManager,
            "clientName",
            "testPrefix",
            InternalStageManager.NO_TABLE_REF,
            false /* useIcebergFileTransferAgent */,
            new SnowflakeFileTransferMetadataWithAge(
                originalMetadata, Optional.of(System.currentTimeMillis())),
            maxUploadRetryCount);
    PowerMockito.mockStatic(SnowflakeFileTransferAgent.class);
    SnowflakeSQLException e =
        new SnowflakeSQLException(
            "Fake bad creds", CLOUD_STORAGE_CREDENTIALS_EXPIRED, "S3 credentials have expired");
    PowerMockito.doThrow(e).when(SnowflakeFileTransferAgent.class);
    SnowflakeFileTransferAgent.uploadWithoutConnection(Mockito.any());
    final ArgumentCaptor<SnowflakeFileTransferConfig> captor =
        ArgumentCaptor.forClass(SnowflakeFileTransferConfig.class);

    try {
      stage.put(
          new BlobPath("test/path" /* uploadPath */, "test/path" /* fileRegistrationPath */),
          dataBytes);
      Assert.fail("Should not succeed");
    } catch (SFException ex) {
      // Expected behavior given mocked response
    }
    PowerMockito.verifyStatic(SnowflakeFileTransferAgent.class, times(maxUploadRetryCount + 1));
    SnowflakeFileTransferAgent.uploadWithoutConnection(captor.capture());
    SnowflakeFileTransferConfig capturedConfig = captor.getValue();

    Assert.assertFalse(capturedConfig.getRequireCompress());
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

    InternalStageManager storageManager = Mockito.mock(InternalStageManager.class);
    Mockito.when(storageManager.getClientPrefix()).thenReturn("testPrefix");

    InternalStage stage =
        Mockito.spy(
            new InternalStage(
                storageManager,
                "clientName",
                "testPrefix",
                InternalStageManager.NO_TABLE_REF,
                false /* useIcebergFileTransferAgent */,
                new SnowflakeFileTransferMetadataWithAge(
                    originalMetadata, Optional.of(System.currentTimeMillis())),
                1));
    PowerMockito.mockStatic(SnowflakeFileTransferAgent.class);
    SnowflakeFileTransferMetadataV1 metaMock = Mockito.mock(SnowflakeFileTransferMetadataV1.class);

    Mockito.doReturn(metaMock).when(stage).fetchSignedURL(Mockito.any());
    stage.put(
        new BlobPath("test/path" /* uploadPath */, "test/path" /* fileRegistrationPath */),
        dataBytes);
    SnowflakeFileTransferAgent.uploadWithoutConnection(Mockito.any());
    Mockito.verify(stage, times(1)).fetchSignedURL("test/path");
  }

  @Test
  public void testRefreshSnowflakeMetadataRemote() throws Exception {
    RequestBuilder mockBuilder = Mockito.mock(RequestBuilder.class);
    CloseableHttpClient mockClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
    SnowflakeStreamingIngestClientInternal mockClientInternal =
        Mockito.mock(SnowflakeStreamingIngestClientInternal.class);
    Mockito.when(mockClientInternal.getRole()).thenReturn("role");
    StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
    Mockito.when(mockStatusLine.getStatusCode()).thenReturn(200);

    Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    Mockito.when(mockResponse.getEntity()).thenReturn(createHttpEntity(exampleRemoteMetaResponse));
    Mockito.when(mockClient.execute(Mockito.any())).thenReturn(mockResponse);

    SnowflakeServiceClient snowflakeServiceClient =
        new SnowflakeServiceClient(mockClient, mockBuilder);
    InternalStageManager storageManager =
        new InternalStageManager(true, "role", "client", snowflakeServiceClient);

    InternalStage stage =
        new InternalStage(
            storageManager,
            "clientName",
            "testPrefix",
            InternalStageManager.NO_TABLE_REF,
            false /* useIcebergFileTransferAgent */,
            (SnowflakeFileTransferMetadataWithAge) null,
            1);

    SnowflakeFileTransferMetadataWithAge metadataWithAge = stage.refreshSnowflakeMetadata(true);

    final ArgumentCaptor<String> endpointCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockBuilder)
        .generateStreamingIngestPostRequest(
            stringCaptor.capture(), endpointCaptor.capture(), Mockito.any());
    Assert.assertEquals(CLIENT_CONFIGURE_ENDPOINT, endpointCaptor.getValue());
    Assert.assertTrue(metadataWithAge.timestamp.isPresent());
    Assert.assertEquals(
        StageInfo.StageType.S3, metadataWithAge.fileTransferMetadata.getStageInfo().getStageType());
    Assert.assertEquals(
        "foo/streaming_ingest/", metadataWithAge.fileTransferMetadata.getStageInfo().getLocation());
    // Here we need to compare paths and not just strings because on windows, due to how JDBC driver
    // works with path separators, presignedUrlFileName is absolute, but on Linux it is relative
    Assert.assertEquals(
        Paths.get("placeholder").toAbsolutePath(),
        Paths.get(metadataWithAge.fileTransferMetadata.getPresignedUrlFileName()).toAbsolutePath());
    Assert.assertEquals(prefix + "_" + deploymentId, storageManager.getClientPrefix());
  }

  @Test
  public void testRefreshSnowflakeMetadataDeploymentIdMismatch() throws Exception {
    RequestBuilder mockBuilder = Mockito.mock(RequestBuilder.class);
    CloseableHttpClient mockClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
    Mockito.when(mockStatusLine.getStatusCode()).thenReturn(200);
    Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(
        new ByteArrayInputStream(exampleRemoteMetaResponse.getBytes(StandardCharsets.UTF_8)));

    BasicHttpEntity entityFromDifferentDeployment = new BasicHttpEntity();
    entityFromDifferentDeployment.setContent(
        new ByteArrayInputStream(
            remoteMetaResponseDifferentDeployment.getBytes(StandardCharsets.UTF_8)));
    Mockito.when(mockResponse.getEntity())
        .thenReturn(entity)
        .thenReturn(entityFromDifferentDeployment);
    Mockito.when(mockClient.execute(Mockito.any()))
        .thenReturn(mockResponse)
        .thenReturn(mockResponse);

    SnowflakeServiceClient snowflakeServiceClient =
        new SnowflakeServiceClient(mockClient, mockBuilder);
    InternalStageManager storageManager =
        new InternalStageManager(true, "role", "clientName", snowflakeServiceClient);

    InternalStage storage = storageManager.getStorage("");
    storage.refreshSnowflakeMetadata(true);

    Assert.assertEquals(prefix + "_" + deploymentId, storageManager.getClientPrefix());

    SFException exception =
        Assert.assertThrows(SFException.class, () -> storage.refreshSnowflakeMetadata(true));
    Assert.assertEquals(
        ErrorCode.CLIENT_DEPLOYMENT_ID_MISMATCH.getMessageCode(), exception.getVendorCode());
    Assert.assertEquals(
        "Deployment ID mismatch, Client was created on: "
            + deploymentId
            + ", Got upload location for: "
            + (deploymentId + 1)
            + ". Please"
            + " restart client: clientName.",
        exception.getMessage());
  }

  @Test
  public void testFetchSignedURL() throws Exception {
    RequestBuilder mockBuilder = Mockito.mock(RequestBuilder.class);
    CloseableHttpClient mockClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
    SnowflakeStreamingIngestClientInternal mockClientInternal =
        Mockito.mock(SnowflakeStreamingIngestClientInternal.class);
    Mockito.when(mockClientInternal.getRole()).thenReturn("role");
    SnowflakeServiceClient snowflakeServiceClient =
        new SnowflakeServiceClient(mockClient, mockBuilder);
    InternalStageManager storageManager =
        new InternalStageManager(true, "role", "client", snowflakeServiceClient);
    StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
    Mockito.when(mockStatusLine.getStatusCode()).thenReturn(200);

    Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    Mockito.when(mockResponse.getEntity()).thenReturn(createHttpEntity(exampleRemoteMetaResponse));
    Mockito.when(mockClient.execute(Mockito.any())).thenReturn(mockResponse);

    InternalStage stage =
        new InternalStage(
            storageManager,
            "clientName",
            "testPrefix",
            InternalStageManager.NO_TABLE_REF,
            false /* useIcebergFileTransferAgent */,
            (SnowflakeFileTransferMetadataWithAge) null,
            1);

    SnowflakeFileTransferMetadataV1 metadata = stage.fetchSignedURL("path/fileName");

    final ArgumentCaptor<String> endpointCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockBuilder)
        .generateStreamingIngestPostRequest(
            stringCaptor.capture(), endpointCaptor.capture(), Mockito.any());
    Assert.assertEquals(CLIENT_CONFIGURE_ENDPOINT, endpointCaptor.getValue());
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
    CloseableHttpClient mockClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
    SnowflakeStreamingIngestClientInternal mockClientInternal =
        Mockito.mock(SnowflakeStreamingIngestClientInternal.class);
    Mockito.when(mockClientInternal.getRole()).thenReturn("role");
    SnowflakeServiceClient snowflakeServiceClient =
        new SnowflakeServiceClient(mockClient, mockBuilder);
    InternalStageManager storageManager =
        new InternalStageManager(true, "role", "client", snowflakeServiceClient);
    StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
    Mockito.when(mockStatusLine.getStatusCode()).thenReturn(200);

    Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    Mockito.when(mockResponse.getEntity()).thenReturn(createHttpEntity(exampleRemoteMetaResponse));
    Mockito.when(mockClient.execute(Mockito.any())).thenReturn(mockResponse);

    InternalStage stage =
        new InternalStage(
            storageManager,
            "clientName",
            "testPrefix",
            InternalStageManager.NO_TABLE_REF,
            false /* useIcebergFileTransferAgent */,
            (SnowflakeFileTransferMetadataWithAge) null,
            1);

    ThreadFactory buildUploadThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("ingest-build-upload-thread-%d").build();
    int buildUploadThreadCount = 2;
    ExecutorService workers =
        Executors.newFixedThreadPool(buildUploadThreadCount, buildUploadThreadFactory);

    workers.submit(
        () -> {
          try {
            stage.refreshSnowflakeMetadata(false);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    workers.submit(
        () -> {
          try {
            stage.refreshSnowflakeMetadata(false);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    workers.shutdown();

    Assert.assertTrue(workers.awaitTermination(1, TimeUnit.SECONDS));

    Mockito.verify(mockClient).execute(Mockito.any());
  }

  @Test
  public void testGenerateProxyPropertiesForJDBC() {
    String oldUseProxy = System.getProperty(USE_PROXY);
    String oldProxyHost = System.getProperty(PROXY_HOST);
    String oldProxyPort = System.getProperty(PROXY_PORT);
    String oldUser = System.getProperty(HTTP_PROXY_USER);
    String oldPassword = System.getProperty(HTTP_PROXY_PASSWORD);
    String oldNonProxyHosts = System.getProperty(NON_PROXY_HOSTS);

    String proxyHost = "localhost";
    String proxyPort = "8080";
    String user = "admin";
    String password = "test";
    String nonProxyHosts = "*.snowflakecomputing.com";

    try {
      // Test empty properties when USE_PROXY is NOT set;
      Properties props = generateProxyPropertiesForJDBC();
      Assert.assertTrue(props.isEmpty());

      System.setProperty(USE_PROXY, "true");
      System.setProperty(PROXY_HOST, proxyHost);
      System.setProperty(PROXY_PORT, proxyPort);
      System.setProperty(HTTP_PROXY_USER, user);
      System.setProperty(HTTP_PROXY_PASSWORD, password);
      System.setProperty(NON_PROXY_HOSTS, nonProxyHosts);

      // Verify that properties are set
      props = generateProxyPropertiesForJDBC();
      Assert.assertEquals("true", props.get(SFSessionProperty.USE_PROXY.getPropertyKey()));
      Assert.assertEquals(proxyHost, props.get(SFSessionProperty.PROXY_HOST.getPropertyKey()));
      Assert.assertEquals(proxyPort, props.get(SFSessionProperty.PROXY_PORT.getPropertyKey()));
      Assert.assertEquals(user, props.get(SFSessionProperty.PROXY_USER.getPropertyKey()));
      Assert.assertEquals(password, props.get(SFSessionProperty.PROXY_PASSWORD.getPropertyKey()));
      Assert.assertEquals(
          nonProxyHosts, props.get(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey()));
    } finally {
      // Cleanup
      if (oldUseProxy != null) {
        System.setProperty(USE_PROXY, oldUseProxy);
        System.setProperty(PROXY_HOST, oldProxyHost);
        System.setProperty(PROXY_PORT, oldProxyPort);
      }
      if (oldUser != null) {
        System.setProperty(HTTP_PROXY_USER, oldUser);
        System.setProperty(HTTP_PROXY_PASSWORD, oldPassword);
      }
      if (oldNonProxyHosts != null) {
        System.setProperty(NON_PROXY_HOSTS, oldNonProxyHosts);
      }
    }
  }

  @Test
  public void testShouldBypassProxy() {
    String oldNonProxyHosts = System.getProperty(NON_PROXY_HOSTS);
    String accountName = "accountName12345";
    String accountDashName = "account-name12345";
    String accountUnderscoreName = "account_name12345";
    String nonProxyHosts = "*.snowflakecomputing.com|localhost";

    System.setProperty(NON_PROXY_HOSTS, nonProxyHosts);
    Assert.assertTrue(shouldBypassProxy(accountName));
    Assert.assertTrue(shouldBypassProxy(accountDashName));
    Assert.assertTrue(shouldBypassProxy(accountUnderscoreName));

    String accountNamePrivateLink = "accountName12345.privatelink";
    nonProxyHosts = "*.privatelink.snowflakecomputing.com|localhost";
    System.setProperty(NON_PROXY_HOSTS, nonProxyHosts);
    Assert.assertTrue(shouldBypassProxy(accountNamePrivateLink));

    // Previous tests should return false with the new nonProxyHosts value
    Assert.assertFalse(shouldBypassProxy(accountName));
    Assert.assertFalse(shouldBypassProxy(accountDashName));
    Assert.assertFalse(shouldBypassProxy(accountUnderscoreName));

    // All tests should return false after clearing the nonProxyHosts property
    System.clearProperty(NON_PROXY_HOSTS);
    Assert.assertFalse(shouldBypassProxy(accountName));
    Assert.assertFalse(shouldBypassProxy(accountDashName));
    Assert.assertFalse(shouldBypassProxy(accountUnderscoreName));
    Assert.assertFalse(shouldBypassProxy(accountNamePrivateLink));

    if (oldNonProxyHosts != null) {
      System.setProperty(NON_PROXY_HOSTS, oldNonProxyHosts);
    }
  }

  @Test
  public void testRefreshMetadataOnFirstPutException() throws Exception {
    int maxUploadRetryCount = 2;
    JsonNode exampleJson = mapper.readTree(exampleRemoteMeta);
    SnowflakeFileTransferMetadataV1 originalMetadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleJson).get(0);

    byte[] dataBytes = "Hello Upload".getBytes(StandardCharsets.UTF_8);

    InternalStageManager storageManager = Mockito.mock(InternalStageManager.class);
    Mockito.when(storageManager.getClientPrefix()).thenReturn("testPrefix");

    InternalStage stage =
        new InternalStage(
            storageManager,
            "clientName",
            "testPrefix",
            InternalStageManager.NO_TABLE_REF,
            false /* useIcebergFileTransferAgent */,
            new SnowflakeFileTransferMetadataWithAge(
                originalMetadata, Optional.of(System.currentTimeMillis())),
            maxUploadRetryCount);
    PowerMockito.mockStatic(SnowflakeFileTransferAgent.class);
    SnowflakeSQLException e =
        new SnowflakeSQLException(
            "Fake bad creds", CLOUD_STORAGE_CREDENTIALS_EXPIRED, "S3 credentials have expired");
    PowerMockito.doAnswer(
            new org.mockito.stubbing.Answer() {
              private boolean firstInvocation = true;

              public Object answer(org.mockito.invocation.InvocationOnMock invocation)
                  throws Throwable {
                if (firstInvocation) {
                  firstInvocation = false;
                  throw e; // Throw the exception only for the first invocation
                }
                return null; // Do nothing on subsequent invocations
              }
            })
        .when(SnowflakeFileTransferAgent.class);
    SnowflakeFileTransferAgent.uploadWithoutConnection(Mockito.any());
    final ArgumentCaptor<SnowflakeFileTransferConfig> captor =
        ArgumentCaptor.forClass(SnowflakeFileTransferConfig.class);

    stage.put(
        new BlobPath("test/path" /* uploadPath */, "test/path" /* fileRegistrationPath */),
        dataBytes);

    PowerMockito.verifyStatic(SnowflakeFileTransferAgent.class, times(maxUploadRetryCount));
    SnowflakeFileTransferAgent.uploadWithoutConnection(captor.capture());
    SnowflakeFileTransferConfig capturedConfig = captor.getValue();

    Assert.assertFalse(capturedConfig.getRequireCompress());
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

  private HttpEntity createHttpEntity(String content) {
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
    return entity;
  }
}

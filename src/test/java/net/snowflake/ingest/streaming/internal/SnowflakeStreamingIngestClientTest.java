package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.ACCOUNT_URL;
import static net.snowflake.ingest.utils.Constants.CHANNEL_STATUS_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.JDBC_PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.MAX_STREAMING_INGEST_API_CHANNEL_RETRY;
import static net.snowflake.ingest.utils.Constants.PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.REGISTER_BLOB_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;
import static net.snowflake.ingest.utils.Constants.ROLE;
import static net.snowflake.ingest.utils.Constants.USER;
import static net.snowflake.ingest.utils.ParameterProvider.ENABLE_SNOWPIPE_STREAMING_METRICS_MAP_KEY;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.internal.apache.commons.io.IOUtils;
import net.snowflake.client.jdbc.internal.apache.http.HttpEntity;
import net.snowflake.client.jdbc.internal.apache.http.HttpHeaders;
import net.snowflake.client.jdbc.internal.apache.http.StatusLine;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
import net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.nist.NISTObjectIdentifiers;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.OperatorCreationException;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfoBuilder;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.jcajce.JcaPKCS8EncryptedPrivateKeyInfoBuilder;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.jcajce.JcePKCSPBEOutputEncryptorBuilder;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

public class SnowflakeStreamingIngestClientTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> channel1;
  SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> channel2;
  SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> channel3;
  SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> channel4;

  @Before
  public void setup() {
    objectMapper.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.ANY);
    objectMapper.setVisibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.ANY);
    channel1 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel1",
            "db",
            "schemaName",
            "tableName",
            "0",
            0L,
            0L,
            null,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);
    channel2 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel2",
            "db",
            "schemaName",
            "tableName",
            "0",
            2L,
            0L,
            null,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);
    channel3 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel3",
            "db",
            "schemaName",
            "tableName3",
            "0",
            3L,
            0L,
            null,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);
    channel4 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel4",
            "db",
            "schemaName",
            "tableName4",
            "0",
            3L,
            0L,
            null,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);
  }

  @Test
  @Ignore // Until able to test in PROD
  public void testConstructorParameters() throws Exception {
    Properties prop = new Properties();
    prop.put(USER, TestUtils.getUser());
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ROLE, "role");
    prop.put(ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS_MAP_KEY, 123);

    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_MAP_KEY, 321);

    SnowflakeStreamingIngestClientInternal<?> client =
        (SnowflakeStreamingIngestClientInternal<?>)
            SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();

    Assert.assertEquals("client", client.getName());
    Assert.assertEquals(123, client.getParameterProvider().getBufferFlushIntervalInMs());
    Assert.assertEquals(321, client.getParameterProvider().getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(
        ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT,
        client.getParameterProvider().getInsertThrottleIntervalInMs());
    Assert.assertFalse(client.isClosed());
  }

  @Test
  @Ignore
  public void testClientFactoryWithJmxMetrics() throws Exception {
    Properties prop = new Properties();
    prop.put(USER, TestUtils.getUser());
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ROLE, "role");

    SnowflakeStreamingIngestClientInternal<?> client =
        (SnowflakeStreamingIngestClientInternal<?>)
            SnowflakeStreamingIngestClientFactory.builder("client")
                .setProperties(prop)
                .setParameterOverrides(
                    Collections.singletonMap(ENABLE_SNOWPIPE_STREAMING_METRICS_MAP_KEY, true))
                .build();

    Assert.assertEquals("client", client.getName());
    Assert.assertFalse(client.isClosed());

    Assert.assertTrue(client.getParameterProvider().hasEnabledSnowpipeStreamingMetrics());
  }

  @Test
  public void testClientFactoryMissingName() throws Exception {
    Properties prop = new Properties();
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ROLE, "role");

    try {
      SnowflakeStreamingIngestClient client =
          SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();
      Assert.fail("Client creation should fail.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.MISSING_CONFIG.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClientFactoryMissingURL() throws Exception {
    Properties prop = new Properties();
    prop.put(USER, TestUtils.getUser());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ROLE, "role");

    try {
      SnowflakeStreamingIngestClient client =
          SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();
      Assert.fail("Client creation should fail.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.MISSING_CONFIG.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClientFactoryInvalidURL() throws Exception {
    Properties prop = new Properties();
    prop.put(USER, TestUtils.getUser());
    prop.put(ACCOUNT_URL, "invalid");
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ROLE, "role");

    try {
      SnowflakeStreamingIngestClient client =
          SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();
      Assert.fail("Client creation should fail.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_URL.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClientFactoryInvalidPrivateKey() throws Exception {
    Properties prop = new Properties();
    prop.put(USER, TestUtils.getUser());
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, "invalid");
    prop.put(ROLE, "role");

    try {
      SnowflakeStreamingIngestClient client =
          SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();
      Assert.fail("Client creation should fail.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_PRIVATE_KEY.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  @Ignore // Wait for the client/configure endpoint to be available in PROD, can't mock the
  // HttpUtil.executeGeneralRequest call because it's also used when setting up the
  // connection
  public void testClientFactorySuccess() throws Exception {
    Properties prop = new Properties();
    prop.put(USER, TestUtils.getUser());
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ROLE, "role");

    SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();

    Assert.assertEquals("client", client.getName());
    Assert.assertFalse(client.isClosed());
  }

  @Test
  @Ignore // SNOW-540567: NON-FIPS provider causing failures during release jekins job
  public void testEncryptedPrivateKey() throws Exception {
    String testPassphrase = "TestPassword1234!";
    String testKey =
        generateAESKey(
            Utils.parsePrivateKey(TestUtils.getPrivateKey()), testPassphrase.toCharArray());

    try {
      Utils.parseEncryptedPrivateKey(testKey, testPassphrase.substring(1));
      Assert.fail("Decryption should fail.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ENCRYPTED_KEY.getMessageCode(), e.getVendorCode());
    }

    Utils.parseEncryptedPrivateKey(testKey, testPassphrase);
  }

  private String generateAESKey(PrivateKey key, char[] passwd)
      throws IOException, OperatorCreationException {
    Security.addProvider(new BouncyCastleProvider());
    StringWriter writer = new StringWriter();
    JcaPEMWriter pemWriter = new JcaPEMWriter(writer);
    PKCS8EncryptedPrivateKeyInfoBuilder pkcs8EncryptedPrivateKeyInfoBuilder =
        new JcaPKCS8EncryptedPrivateKeyInfoBuilder(key);
    pemWriter.writeObject(
        pkcs8EncryptedPrivateKeyInfoBuilder.build(
            new JcePKCSPBEOutputEncryptorBuilder(NISTObjectIdentifiers.id_aes256_CBC)
                .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                .build(passwd)));
    pemWriter.close();
    return writer.toString();
  }

  @Test
  public void testGetChannelsStatusWithRequest() throws Exception {
    ChannelsStatusResponse.ChannelStatusResponseDTO channelStatus =
        new ChannelsStatusResponse.ChannelStatusResponseDTO();
    channelStatus.setStatusCode((long) RESPONSE_SUCCESS);
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("honk");
    response.setChannels(Collections.singletonList(channelStatus));
    String responseString = objectMapper.writeValueAsString(response);

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
    Mockito.when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream(responseString));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair()));
    SnowflakeStreamingIngestClientInternal<?> client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);

    SnowflakeStreamingIngestChannelInternal<?> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
            "db",
            "schemaName",
            "tableName",
            "0",
            0L,
            0L,
            null,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);

    ChannelsStatusRequest.ChannelStatusRequestDTO dto =
        new ChannelsStatusRequest.ChannelStatusRequestDTO(channel);
    ChannelsStatusRequest request = new ChannelsStatusRequest();
    request.setRequestId("null_0");
    request.setChannels(Collections.singletonList(dto));
    ChannelsStatusResponse result = client.getChannelsStatus(Collections.singletonList(channel));
    Assert.assertEquals(response.getMessage(), result.getMessage());
    Mockito.verify(requestBuilder)
        .generateStreamingIngestPostRequest(
            objectMapper.writeValueAsString(request), CHANNEL_STATUS_ENDPOINT, "channel status");
  }

  @Test
  public void testGetChannelsStatusWithRequestError() throws Exception {
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("honk");
    response.setChannels(new ArrayList<>());
    String responseString = objectMapper.writeValueAsString(response);

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(500);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
    Mockito.when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream(responseString));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair()));
    SnowflakeStreamingIngestClientInternal<?> client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);

    SnowflakeStreamingIngestChannelInternal<?> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
            "db",
            "schemaName",
            "tableName",
            "0",
            0L,
            0L,
            null,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);

    try {
      client.getChannelsStatus(Collections.singletonList(channel));
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.CHANNEL_STATUS_FAILURE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testRegisterBlobRequestCreationSuccess() throws Exception {
    Properties prop = new Properties();
    prop.put(USER, TestUtils.getUser());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ACCOUNT_URL, TestUtils.getAccountURL());
    prop.put(ROLE, TestUtils.getRole());
    prop = Utils.createProperties(prop);

    String urlStr = "https://sfctest0.snowflakecomputing.com:80";
    SnowflakeURL url = new SnowflakeURL(urlStr);

    KeyPair keyPair = Utils.createKeyPairFromPrivateKey((PrivateKey) prop.get(JDBC_PRIVATE_KEY));
    RequestBuilder requestBuilder =
        new RequestBuilder(url, prop.get(USER).toString(), keyPair, null, null);

    SnowflakeStreamingIngestChannelInternal<?> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
            "db",
            "schemaName",
            "tableName",
            "0",
            0L,
            0L,
            null,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);

    ChannelMetadata channelMetadata =
        ChannelMetadata.builder()
            .setOwningChannel(channel)
            .setRowSequencer(channel.incrementAndGetRowSequencer())
            .setOffsetToken(channel.getOffsetToken())
            .build();

    Map<String, RowBufferStats> columnEps = new HashMap<>();
    columnEps.put("column", new RowBufferStats());
    EpInfo epInfo = AbstractRowBuffer.buildEpInfoFromStats(1, columnEps);

    ChunkMetadata chunkMetadata =
        ChunkMetadata.builder()
            .setOwningTable(channel)
            .setChunkStartOffset(0L)
            .setChunkLength(100)
            .setChannelList(Collections.singletonList(channelMetadata))
            .setChunkMD5("md5")
            .setEncryptionKeyId(1234L)
            .setEpInfo(epInfo)
            .build();

    List<BlobMetadata> blobs =
        Collections.singletonList(
            new BlobMetadata("path", "md5", Collections.singletonList(chunkMetadata)));

    Map<Object, Object> payload = new HashMap<>();
    payload.put("request_id", null);
    payload.put("blobs", blobs);

    HttpPost request =
        requestBuilder.generateStreamingIngestPostRequest(
            payload, REGISTER_BLOB_ENDPOINT, "register blob");

    Assert.assertEquals(
        String.format("%s%s", urlStr, REGISTER_BLOB_ENDPOINT),
        request.getRequestLine().getUri().toString());
    Assert.assertNotNull(request.getFirstHeader(HttpHeaders.USER_AGENT));
    Assert.assertNotNull(request.getFirstHeader(HttpHeaders.AUTHORIZATION));
    Assert.assertEquals("POST", request.getMethod());
    Assert.assertEquals(
        RequestBuilder.HTTP_HEADER_CONTENT_TYPE_JSON,
        request.getFirstHeader(HttpHeaders.ACCEPT).getValue());
    Assert.assertEquals(
        RequestBuilder.JWT_TOKEN_TYPE,
        request.getFirstHeader(RequestBuilder.SF_HEADER_AUTHORIZATION_TOKEN_TYPE).getValue());
  }

  private Pair<List<BlobMetadata>, Set<ChunkRegisterStatus>> getRetryBlobMetadata() {
    Map<String, RowBufferStats> columnEps = new HashMap<>();
    columnEps.put("column", new RowBufferStats());
    EpInfo epInfo = AbstractRowBuffer.buildEpInfoFromStats(1, columnEps);

    ChannelMetadata channelMetadata1 =
        ChannelMetadata.builder()
            .setOwningChannel(channel1)
            .setRowSequencer(channel1.incrementAndGetRowSequencer())
            .setOffsetToken(channel1.getOffsetToken())
            .build();
    ChannelMetadata channelMetadata2 =
        ChannelMetadata.builder()
            .setOwningChannel(channel2)
            .setRowSequencer(channel2.incrementAndGetRowSequencer())
            .setOffsetToken(channel2.getOffsetToken())
            .build();
    ChannelMetadata channelMetadata3 =
        ChannelMetadata.builder()
            .setOwningChannel(channel3)
            .setRowSequencer(channel3.incrementAndGetRowSequencer())
            .setOffsetToken(channel3.getOffsetToken())
            .build();
    ChannelMetadata channelMetadata4 =
        ChannelMetadata.builder()
            .setOwningChannel(channel4)
            .setRowSequencer(channel4.incrementAndGetRowSequencer())
            .setOffsetToken(channel4.getOffsetToken())
            .build();

    List<BlobMetadata> blobs = new ArrayList<>();
    List<ChunkMetadata> chunks1 = new ArrayList<>();
    List<ChunkMetadata> chunks2 = new ArrayList<>();

    List<ChannelMetadata> channels1 = new ArrayList<>();
    channels1.add(channelMetadata1);
    channels1.add(channelMetadata2);
    ChunkMetadata chunkMetadata1 =
        ChunkMetadata.builder()
            .setOwningTable(channel1)
            .setChunkStartOffset(0L)
            .setChunkLength(100)
            .setChannelList(channels1)
            .setChunkMD5("md51")
            .setEncryptionKeyId(1234L)
            .setEpInfo(epInfo)
            .build();
    ChunkMetadata chunkMetadata2 =
        ChunkMetadata.builder()
            .setOwningTable(channel2)
            .setChunkStartOffset(0L)
            .setChunkLength(100)
            .setChannelList(Collections.singletonList(channelMetadata3))
            .setChunkMD5("md52")
            .setEncryptionKeyId(1234L)
            .setEpInfo(epInfo)
            .build();
    ChunkMetadata chunkMetadata3 =
        ChunkMetadata.builder()
            .setOwningTable(channel3)
            .setChunkStartOffset(0L)
            .setChunkLength(100)
            .setChannelList(Collections.singletonList(channelMetadata4))
            .setChunkMD5("md53")
            .setEncryptionKeyId(1234L)
            .setEpInfo(epInfo)
            .build();

    chunks1.add(chunkMetadata1);
    chunks1.add(chunkMetadata2);
    chunks2.add(chunkMetadata3);
    blobs.add(new BlobMetadata("path1", "md51", chunks1));
    blobs.add(new BlobMetadata("path2", "md52", chunks2));

    List<ChannelRegisterStatus> channelRegisterStatuses = new ArrayList<>();
    ChannelRegisterStatus status1 = new ChannelRegisterStatus();
    status1.setStatusCode(RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL);
    status1.setChannelName(channelMetadata1.getChannelName());
    status1.setChannelSequencer(channelMetadata1.getClientSequencer());

    ChannelRegisterStatus status2 = new ChannelRegisterStatus();
    status2.setStatusCode(RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL);
    status2.setChannelName(channelMetadata2.getChannelName());
    status2.setChannelSequencer(channelMetadata2.getClientSequencer());

    channelRegisterStatuses.add(status1);
    channelRegisterStatuses.add(status2);

    Set<ChunkRegisterStatus> badChunks = new HashSet<>();
    ChunkRegisterStatus badChunkRegisterStatus = new ChunkRegisterStatus();
    badChunkRegisterStatus.setDBName(chunkMetadata1.getDBName());
    badChunkRegisterStatus.setSchemaName(chunkMetadata1.getSchemaName());
    badChunkRegisterStatus.setTableName(chunkMetadata1.getTableName());
    badChunkRegisterStatus.setChannelsStatus(channelRegisterStatuses);
    badChunks.add(badChunkRegisterStatus);
    return new Pair(blobs, badChunks);
  }

  @Test
  public void testGetRetryBlobs() throws Exception {
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    RequestBuilder requestBuilder =
        new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair());

    SnowflakeStreamingIngestClientInternal<?> client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);
    Pair<List<BlobMetadata>, Set<ChunkRegisterStatus>> testData = getRetryBlobMetadata();
    List<BlobMetadata> blobs = testData.getFirst();
    Set<ChunkRegisterStatus> badChunks = testData.getSecond();
    List<BlobMetadata> result = client.getRetryBlobs(badChunks, blobs);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("path1", result.get(0).getPath());
    Assert.assertEquals("md51", result.get(0).getMD5());
    Assert.assertEquals(1, result.get(0).getChunks().size());
    Assert.assertEquals(2, result.get(0).getChunks().get(0).getChannels().size());
    Assert.assertEquals(
        Sets.newHashSet("channel1", "channel2"),
        result.get(0).getChunks().get(0).getChannels().stream()
            .map(c -> c.getChannelName())
            .collect(Collectors.toSet()));
    Assert.assertEquals(ParameterProvider.BLOB_FORMAT_VERSION_DEFAULT, result.get(0).getVersion());
  }

  @Test
  public void testRegisterBlobErrorResponse() throws Exception {
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(500);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
    String response = "testRegisterBlobErrorResponse";
    Mockito.when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream(response));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair());
    SnowflakeStreamingIngestClientInternal<?> client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);

    try {
      List<BlobMetadata> blobs =
          Collections.singletonList(
              new BlobMetadata("path", "md5", new ArrayList<ChunkMetadata>()));
      client.registerBlobs(blobs);
      Assert.fail("Register blob should fail on 404 error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.REGISTER_BLOB_FAILURE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testRegisterBlobSnowflakeInternalErrorResponse() throws Exception {
    String response =
        "{\n"
            + "  \"status_code\" : 21,\n"
            + "  \"message\" : \"ERR_CHANNEL_HAS_INVALID_ROW_SEQUENCER\",\n"
            + "  \"blobs\" : [ {\n"
            + "    \"chunks\" : [ {\n"
            + "      \"channels\" : [ {\n"
            + "        \"status_code\" : 0\n"
            + "      } ]\n"
            + "    } ]\n"
            + "  } ]\n"
            + "}";

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
    Mockito.when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream(response));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair());
    SnowflakeStreamingIngestClientInternal<?> client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);

    try {
      List<BlobMetadata> blobs =
          Collections.singletonList(
              new BlobMetadata("path", "md5", new ArrayList<ChunkMetadata>()));
      client.registerBlobs(blobs);
      Assert.fail("Register blob should fail on SF internal error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.REGISTER_BLOB_FAILURE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testRegisterBlobSuccessResponse() throws Exception {
    String response =
        "{\n"
            + "  \"status_code\" : 0,\n"
            + "  \"message\" : \"Success\",\n"
            + "  \"blobs\" : [ {\n"
            + "    \"chunks\" : [ {\n"
            + "      \"database\" : \"DB_STREAMINGINGEST\",\n"
            + "      \"schema\" : \"PUBLIC\",\n"
            + "      \"table\" : \"T_STREAMINGINGEST\",\n"
            + "      \"channels\" : [ {\n"
            + "        \"status_code\" : 0,\n"
            + "        \"channel\" : \"CHANNEL\",\n"
            + "        \"client_sequencer\" : 0\n"
            + "      }, {\n"
            + "        \"status_code\" : 0,\n"
            + "        \"channel\" : \"CHANNEL1\",\n"
            + "        \"client_sequencer\" : 0\n"
            + "      } ]\n"
            + "    } ]\n"
            + "  } ]\n"
            + "}";

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
    Mockito.when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream(response));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair());
    SnowflakeStreamingIngestClientInternal<?> client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);

    List<BlobMetadata> blobs =
        Collections.singletonList(new BlobMetadata("path", "md5", new ArrayList<ChunkMetadata>()));
    client.registerBlobs(blobs);
  }

  @Test
  public void testRegisterBlobsRetries() throws Exception {
    Pair<List<BlobMetadata>, Set<ChunkRegisterStatus>> testData = getRetryBlobMetadata();
    List<BlobMetadata> blobs = testData.getFirst();
    Set<ChunkRegisterStatus> badChunks = testData.getSecond();

    ChunkRegisterStatus goodChunkRegisterStatus = new ChunkRegisterStatus();
    goodChunkRegisterStatus.setDBName(blobs.get(0).getChunks().get(0).getDBName());
    goodChunkRegisterStatus.setSchemaName(blobs.get(0).getChunks().get(0).getSchemaName());
    goodChunkRegisterStatus.setTableName(blobs.get(0).getChunks().get(0).getTableName());
    ChannelRegisterStatus goodStatus1 = new ChannelRegisterStatus();
    goodStatus1.setStatusCode(RESPONSE_SUCCESS);
    goodStatus1.setChannelName("channel3");
    goodStatus1.setChannelSequencer(3L);
    goodChunkRegisterStatus.setChannelsStatus(Collections.singletonList(goodStatus1));

    ChunkRegisterStatus goodChunkRegisterStatus2 = new ChunkRegisterStatus();
    goodChunkRegisterStatus2.setDBName(blobs.get(0).getChunks().get(0).getDBName());
    goodChunkRegisterStatus2.setSchemaName(blobs.get(0).getChunks().get(0).getSchemaName());
    goodChunkRegisterStatus2.setTableName(blobs.get(0).getChunks().get(0).getTableName());
    ChannelRegisterStatus goodStatus2 = new ChannelRegisterStatus();
    goodStatus2.setStatusCode(RESPONSE_SUCCESS);
    goodStatus2.setChannelName("channel4");
    goodStatus2.setChannelSequencer(3L);
    goodChunkRegisterStatus2.setChannelsStatus(Collections.singletonList(goodStatus2));

    RegisterBlobResponse initialResponse = new RegisterBlobResponse();
    initialResponse.setMessage("successish");
    initialResponse.setStatusCode(RESPONSE_SUCCESS);

    RegisterBlobResponse retryResponse = new RegisterBlobResponse();
    retryResponse.setMessage("successish");
    retryResponse.setStatusCode(RESPONSE_SUCCESS);

    List<BlobRegisterStatus> blobRegisterStatuses = new ArrayList<>();
    BlobRegisterStatus blobRegisterStatus1 = new BlobRegisterStatus();
    blobRegisterStatus1.setChunksStatus(badChunks.stream().collect(Collectors.toList()));
    blobRegisterStatuses.add(blobRegisterStatus1);
    BlobRegisterStatus blobRegisterStatus2 = new BlobRegisterStatus();
    blobRegisterStatus2.setChunksStatus(Collections.singletonList(goodChunkRegisterStatus));
    blobRegisterStatuses.add(blobRegisterStatus2);
    BlobRegisterStatus blobRegisterStatus3 = new BlobRegisterStatus();
    blobRegisterStatus3.setChunksStatus(Collections.singletonList(goodChunkRegisterStatus2));
    blobRegisterStatuses.add(blobRegisterStatus3);
    initialResponse.setBlobsStatus(blobRegisterStatuses);

    retryResponse.setBlobsStatus(Collections.singletonList(blobRegisterStatus1));

    String responseString = objectMapper.writeValueAsString(initialResponse);
    String retryResponseString = objectMapper.writeValueAsString(retryResponse);

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
    Mockito.when(httpEntity.getContent())
        .thenReturn(
            IOUtils.toInputStream(responseString),
            IOUtils.toInputStream(retryResponseString),
            IOUtils.toInputStream(retryResponseString),
            IOUtils.toInputStream(retryResponseString));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair()));
    SnowflakeStreamingIngestClientInternal<VectorSchemaRoot> client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);

    client.getChannelCache().addChannel(channel1);
    client.getChannelCache().addChannel(channel2);
    client.getChannelCache().addChannel(channel3);
    client.getChannelCache().addChannel(channel4);
    client.registerBlobs(blobs);
    Mockito.verify(requestBuilder, Mockito.times(MAX_STREAMING_INGEST_API_CHANNEL_RETRY + 1))
        .generateStreamingIngestPostRequest(Mockito.anyString(), Mockito.any(), Mockito.any());
    Assert.assertFalse(channel1.isValid());
    Assert.assertFalse(channel2.isValid());
  }

  @Test
  public void testRegisterBlobsRetriesSucceeds() throws Exception {
    Pair<List<BlobMetadata>, Set<ChunkRegisterStatus>> testData = getRetryBlobMetadata();
    List<BlobMetadata> blobs = testData.getFirst();
    Set<ChunkRegisterStatus> badChunks = testData.getSecond();

    ChunkRegisterStatus goodChunkRegisterStatus = new ChunkRegisterStatus();
    goodChunkRegisterStatus.setDBName(blobs.get(0).getChunks().get(0).getDBName());
    goodChunkRegisterStatus.setSchemaName(blobs.get(0).getChunks().get(0).getSchemaName());
    goodChunkRegisterStatus.setTableName(blobs.get(0).getChunks().get(0).getTableName());
    ChannelRegisterStatus goodStatus1 = new ChannelRegisterStatus();
    goodStatus1.setStatusCode(RESPONSE_SUCCESS);
    goodStatus1.setChannelName("channel3");
    goodStatus1.setChannelSequencer(3L);
    goodChunkRegisterStatus.setChannelsStatus(Collections.singletonList(goodStatus1));

    ChunkRegisterStatus goodChunkRegisterStatus2 = new ChunkRegisterStatus();
    goodChunkRegisterStatus2.setDBName(blobs.get(0).getChunks().get(0).getDBName());
    goodChunkRegisterStatus2.setSchemaName(blobs.get(0).getChunks().get(0).getSchemaName());
    goodChunkRegisterStatus2.setTableName(blobs.get(0).getChunks().get(0).getTableName());
    ChannelRegisterStatus goodStatus2 = new ChannelRegisterStatus();
    goodStatus2.setStatusCode(RESPONSE_SUCCESS);
    goodStatus2.setChannelName("channel4");
    goodStatus2.setChannelSequencer(3L);
    goodChunkRegisterStatus2.setChannelsStatus(Collections.singletonList(goodStatus2));

    List<ChunkRegisterStatus> goodChunkRegisterRetryStatus =
        badChunks.stream()
            .map(
                chunkRegisterStatus -> {
                  ChunkRegisterStatus newStatus = new ChunkRegisterStatus();
                  newStatus.setTableName(chunkRegisterStatus.getTableName());
                  newStatus.setDBName(chunkRegisterStatus.getDBName());
                  newStatus.setSchemaName(chunkRegisterStatus.getSchemaName());
                  newStatus.setChannelsStatus(
                      chunkRegisterStatus.getChannelsStatus().stream()
                          .map(
                              channelRegisterStatus -> {
                                ChannelRegisterStatus newChannelStatus =
                                    new ChannelRegisterStatus();
                                newChannelStatus.setStatusCode(RESPONSE_SUCCESS);
                                newChannelStatus.setChannelSequencer(
                                    channelRegisterStatus.getChannelSequencer());
                                newChannelStatus.setChannelName(
                                    channelRegisterStatus.getChannelName());
                                return newChannelStatus;
                              })
                          .collect(Collectors.toList()));
                  return newStatus;
                })
            .collect(Collectors.toList());

    RegisterBlobResponse initialResponse = new RegisterBlobResponse();
    initialResponse.setMessage("successish");
    initialResponse.setStatusCode(RESPONSE_SUCCESS);

    RegisterBlobResponse retryResponse = new RegisterBlobResponse();
    retryResponse.setMessage("successish");
    retryResponse.setStatusCode(RESPONSE_SUCCESS);

    List<BlobRegisterStatus> blobRegisterStatuses = new ArrayList<>();
    BlobRegisterStatus blobRegisterStatus1 = new BlobRegisterStatus();
    blobRegisterStatus1.setChunksStatus(badChunks.stream().collect(Collectors.toList()));
    blobRegisterStatuses.add(blobRegisterStatus1);
    BlobRegisterStatus blobRegisterStatus2 = new BlobRegisterStatus();
    blobRegisterStatus2.setChunksStatus(Collections.singletonList(goodChunkRegisterStatus));
    blobRegisterStatuses.add(blobRegisterStatus2);
    BlobRegisterStatus blobRegisterStatus3 = new BlobRegisterStatus();
    blobRegisterStatus3.setChunksStatus(Collections.singletonList(goodChunkRegisterStatus2));
    blobRegisterStatuses.add(blobRegisterStatus3);
    initialResponse.setBlobsStatus(blobRegisterStatuses);

    BlobRegisterStatus retryBlobRegisterStatus = new BlobRegisterStatus();
    retryBlobRegisterStatus.setChunksStatus(goodChunkRegisterRetryStatus);
    retryResponse.setBlobsStatus(Collections.singletonList(retryBlobRegisterStatus));

    String responseString = objectMapper.writeValueAsString(initialResponse);
    String retryResponseString = objectMapper.writeValueAsString(retryResponse);

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
    Mockito.when(httpEntity.getContent())
        .thenReturn(
            IOUtils.toInputStream(responseString), IOUtils.toInputStream(retryResponseString));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair()));
    SnowflakeStreamingIngestClientInternal<VectorSchemaRoot> client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);

    client.getChannelCache().addChannel(channel1);
    client.getChannelCache().addChannel(channel2);
    client.getChannelCache().addChannel(channel3);
    client.getChannelCache().addChannel(channel4);

    client.registerBlobs(blobs);
    Mockito.verify(requestBuilder, Mockito.times(2))
        .generateStreamingIngestPostRequest(Mockito.anyString(), Mockito.any(), Mockito.any());
    Assert.assertTrue(channel1.isValid());
    Assert.assertTrue(channel2.isValid());
  }

  @Test
  public void testRegisterBlobResponseWithInvalidChannel() throws Exception {
    String dbName = "DB_STREAMINGINGEST";
    String schemaName = "PUBLIC";
    String tableName = "T_STREAMINGINGEST";
    String channel1Name = "CHANNEL1";
    String channel2Name = "CHANNEL2";
    long channel1Sequencer = 0;
    long channel2Sequencer = 0;

    String response =
        String.format(
            "{\n"
                + "  \"status_code\" : 0,\n"
                + "  \"message\" : \"Success\",\n"
                + "  \"blobs\" : [ {\n"
                + "    \"chunks\" : [ {\n"
                + "      \"database\" : \"%s\",\n"
                + "      \"schema\" : \"%s\",\n"
                + "      \"table\" : \"%s\",\n"
                + "      \"channels\" : [ {\n"
                + "        \"status_code\" : 0,\n"
                + "        \"channel\" : \"%s\",\n"
                + "        \"client_sequencer\" : %d\n"
                + "      }, {\n"
                + "        \"status_code\" : 20,\n"
                + "        \"channel\" : \"%s\",\n"
                + "        \"client_sequencer\" : %d\n"
                + "      } ]\n"
                + "    } ]\n"
                + "  } ]\n"
                + "}",
            dbName,
            schemaName,
            tableName,
            channel1Name,
            channel1Sequencer,
            channel2Name,
            channel2Sequencer);

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
    Mockito.when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream(response));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair());
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);

    SnowflakeStreamingIngestChannelInternal channel1 =
        new SnowflakeStreamingIngestChannelInternal(
            channel1Name,
            dbName,
            schemaName,
            tableName,
            "0",
            channel1Sequencer,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);
    SnowflakeStreamingIngestChannelInternal channel2 =
        new SnowflakeStreamingIngestChannelInternal(
            channel2Name,
            dbName,
            schemaName,
            tableName,
            "0",
            channel2Sequencer,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);
    client.getChannelCache().addChannel(channel1);
    client.getChannelCache().addChannel(channel2);

    Assert.assertTrue(channel1.isValid());
    Assert.assertTrue(channel2.isValid());

    List<BlobMetadata> blobs =
        Collections.singletonList(new BlobMetadata("path", "md5", new ArrayList<ChunkMetadata>()));
    client.registerBlobs(blobs);

    // Channel2 should be invalidated now
    Assert.assertTrue(channel1.isValid());
    Assert.assertFalse(channel2.isValid());
  }

  @Test
  public void testFlush() throws Exception {
    SnowflakeStreamingIngestClientInternal client =
        Mockito.spy(new SnowflakeStreamingIngestClientInternal("client"));
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("Success");
    response.setChannels(new ArrayList<>());

    Mockito.doReturn(response).when(client).getChannelsStatus(Mockito.any());

    client.flush(false).get();

    // Calling flush on closed client should fail
    client.close();
    try {
      client.flush(false).get();
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.CLOSED_CLIENT.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClose() throws Exception {
    SnowflakeStreamingIngestClientInternal client =
        Mockito.spy(new SnowflakeStreamingIngestClientInternal("client"));
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("Success");
    response.setChannels(new ArrayList<>());

    Mockito.doReturn(response).when(client).getChannelsStatus(Mockito.any());

    Assert.assertFalse(client.isClosed());
    client.close();
    Assert.assertTrue(client.isClosed());
    // Calling close again on closed client shouldn't fail
    client.close();

    // Calling open channel on closed client should fail
    try {
      OpenChannelRequest request =
          OpenChannelRequest.builder("CHANNEL")
              .setDBName("STREAMINGINGEST_TEST")
              .setSchemaName("PUBLIC")
              .setTableName("T_STREAMINGINGEST")
              .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
              .build();

      client.openChannel(request);
      Assert.fail("request should fail on closed client.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.CLOSED_CLIENT.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testCloseWithError() throws Exception {
    SnowflakeStreamingIngestClientInternal client =
        Mockito.spy(new SnowflakeStreamingIngestClientInternal("client"));
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(20L);
    response.setMessage("Failure");
    response.setChannels(new ArrayList<>());

    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new Exception("Simulating Error"));
    Mockito.when(client.flush(true)).thenReturn(future);

    Assert.assertFalse(client.isClosed());
    try {
      client.close();
      Assert.fail("close should throw");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.RESOURCE_CLEANUP_FAILURE.getMessageCode(), e.getVendorCode());
    }
    Assert.assertTrue(client.isClosed());

    // Calling close again on closed client shouldn't fail
    client.close();

    // Calling open channel on closed client should fail
    try {
      OpenChannelRequest request =
          OpenChannelRequest.builder("CHANNEL")
              .setDBName("STREAMINGINGEST_TEST")
              .setSchemaName("PUBLIC")
              .setTableName("T_STREAMINGINGEST")
              .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
              .build();

      client.openChannel(request);
      Assert.fail("request should fail on closed client.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.CLOSED_CLIENT.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testVerifyChannelsAreFullyCommittedSuccess() throws Exception {
    SnowflakeStreamingIngestClientInternal client =
        Mockito.spy(new SnowflakeStreamingIngestClientInternal("client"));
    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel1",
            "db",
            "schema",
            "table",
            "0",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);
    client.getChannelCache().addChannel(channel);

    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("Success");
    ChannelsStatusResponse.ChannelStatusResponseDTO channelStatus =
        new ChannelsStatusResponse.ChannelStatusResponseDTO();
    channelStatus.setStatusCode(26L);
    channelStatus.setPersistedOffsetToken("0");
    response.setChannels(Collections.singletonList(channelStatus));

    Mockito.doReturn(response).when(client).getChannelsStatus(Mockito.any());

    client.close();
  }
}

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.ACCOUNT_URL;
import static net.snowflake.ingest.utils.Constants.CHANNEL_STATUS_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.JDBC_PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.REGISTER_BLOB_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.ROLE;
import static net.snowflake.ingest.utils.Constants.USER;
import static net.snowflake.ingest.utils.ParameterProvider.ENABLE_SNOWPIPE_STREAMING_JMX_METRICS_MAP_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import net.snowflake.client.jdbc.internal.apache.commons.io.IOUtils;
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
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

public class SnowflakeStreamingIngestClientTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

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

    SnowflakeStreamingIngestClientInternal client =
        (SnowflakeStreamingIngestClientInternal)
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

    SnowflakeStreamingIngestClientInternal client =
        (SnowflakeStreamingIngestClientInternal)
            SnowflakeStreamingIngestClientFactory.builder("client")
                .setProperties(prop)
                .setParameterOverrides(
                    Collections.singletonMap(ENABLE_SNOWPIPE_STREAMING_JMX_METRICS_MAP_KEY, true))
                .build();

    Assert.assertEquals("client", client.getName());
    Assert.assertFalse(client.isClosed());

    Assert.assertTrue(client.getParameterProvider().hasEnabledSnowpipeStreamingJmxMetrics());
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
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("honk");
    response.setChannels(new ArrayList<>());
    String responseString = objectMapper.writeValueAsString(response);

    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
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
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);

    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
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

    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
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
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);

    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
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
    prop.put(ROLE, "role");
    prop = Utils.createProperties(prop, false);

    String urlStr = "https://sfctest0.snowflakecomputing.com:80";
    SnowflakeURL url = new SnowflakeURL(urlStr);

    KeyPair keyPair = Utils.createKeyPairFromPrivateKey((PrivateKey) prop.get(JDBC_PRIVATE_KEY));
    RequestBuilder requestBuilder = new RequestBuilder(url, prop.get(USER).toString(), keyPair);

    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
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
    EpInfo epInfo = ArrowRowBuffer.buildEpInfoFromStats(1, columnEps);

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

  @Test
  public void testRegisterBlobErrorResponse() throws Exception {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
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
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal(
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

    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
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

    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
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

    List<BlobMetadata> blobs =
        Collections.singletonList(new BlobMetadata("path", "md5", new ArrayList<ChunkMetadata>()));
    client.registerBlobs(blobs);
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

    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
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

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.ACCOUNT_URL;
import static net.snowflake.ingest.utils.Constants.CHANNEL_STATUS_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.JDBC_PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.JDBC_USER;
import static net.snowflake.ingest.utils.Constants.PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.REGISTER_BLOB_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.ROLE_NAME;
import static net.snowflake.ingest.utils.Constants.USER_NAME;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
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
import net.snowflake.client.jdbc.internal.apache.commons.io.IOUtils;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.bouncycastle.asn1.nist.NISTObjectIdentifiers;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfoBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS8EncryptedPrivateKeyInfoBuilder;
import org.bouncycastle.pkcs.jcajce.JcePKCSPBEOutputEncryptorBuilder;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class SnowflakeStreamingIngestClientTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testClientFactoryMissingName() throws Exception {
    Properties prop = new Properties();
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ROLE_NAME, "role");

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
    prop.put(USER_NAME, TestUtils.getUser());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ROLE_NAME, "role");

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
    prop.put(USER_NAME, TestUtils.getUser());
    prop.put(ACCOUNT_URL, "invalid");
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ROLE_NAME, "role");

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
    prop.put(USER_NAME, TestUtils.getUser());
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, "invalid");
    prop.put(ROLE_NAME, "role");

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
    prop.put(USER_NAME, TestUtils.getUser());
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ROLE_NAME, "role");

    SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();

    Assert.assertEquals("client", client.getName());
    Assert.assertFalse(client.isClosed());
  }

  @Test
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
    Security.addProvider(new BouncyCastleFipsProvider());
    StringWriter writer = new StringWriter();
    JcaPEMWriter pemWriter = new JcaPEMWriter(writer);
    PKCS8EncryptedPrivateKeyInfoBuilder pkcs8EncryptedPrivateKeyInfoBuilder =
        new JcaPKCS8EncryptedPrivateKeyInfoBuilder(key);
    pemWriter.writeObject(
        pkcs8EncryptedPrivateKeyInfoBuilder.build(
            new JcePKCSPBEOutputEncryptorBuilder(NISTObjectIdentifiers.id_aes256_CBC)
                .setProvider("BCFIPS")
                .build(passwd)));
    pemWriter.close();
    return writer.toString();
  }

  @Test
  public void testGetChannelsStatusWithRequest() throws Exception {
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("honk");
    response.setChannels(new ChannelsStatusResponse.ChannelStatusResponseDTO[0]);
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
            requestBuilder);

    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schemaName", "tableName", "0", 0L, 0L, null, true);

    ChannelsStatusRequest.ChannelRequestDTO dto =
        new ChannelsStatusRequest.ChannelRequestDTO(channel);
    ChannelsStatusRequest request = new ChannelsStatusRequest();
    request.setChannels(new ChannelsStatusRequest.ChannelRequestDTO[] {dto});
    ChannelsStatusResponse result = client.getChannelsStatus(request);
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
    response.setChannels(new ChannelsStatusResponse.ChannelStatusResponseDTO[0]);
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
            requestBuilder);

    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schemaName", "tableName", "0", 0L, 0L, null, true);

    ChannelsStatusRequest.ChannelRequestDTO dto =
        new ChannelsStatusRequest.ChannelRequestDTO(channel);
    ChannelsStatusRequest request = new ChannelsStatusRequest();
    request.setChannels(new ChannelsStatusRequest.ChannelRequestDTO[] {dto});
    try {
      client.getChannelsStatus(request);
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.CHANNEL_STATUS_FAILURE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testGetChannelsStatusWithOutRequest() throws Exception {
    OpenChannelResponse response = new OpenChannelResponse();
    response.setStatusCode(0L);
    response.setMessage("honk");
    response.setClientSequencer(0L);
    response.setOffsetToken("0");
    response.setRowSequencer(0L);
    response.setTableColumns(Lists.newArrayList());

    // ObjectMapper needs visibility changed to deserialize package private fields in this test
    ObjectMapper anyVisibilityObjectMapper = new ObjectMapper();
    anyVisibilityObjectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    String responseString = anyVisibilityObjectMapper.writeValueAsString(response);

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
        Mockito.spy(
            new SnowflakeStreamingIngestClientInternal(
                "client",
                new SnowflakeURL("snowflake.dev.local:8082"),
                null,
                httpClient,
                true,
                requestBuilder));

    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schema", "table", "0", 0L, 0L, null, true);

    Mockito.doReturn(null).when(client).getChannelsStatus(Mockito.any());

    OpenChannelRequest openChannelRequest =
        OpenChannelRequest.builder("channel")
            .setDBName("db")
            .setSchemaName("schema")
            .setTableName("table")
            .build();
    client.openChannel(openChannelRequest);

    ChannelsStatusRequest.ChannelRequestDTO dto =
        new ChannelsStatusRequest.ChannelRequestDTO(channel);
    ChannelsStatusRequest expectedRequest = new ChannelsStatusRequest();
    expectedRequest.setChannels(new ChannelsStatusRequest.ChannelRequestDTO[] {dto});

    client.getChannelsStatus();
    ArgumentCaptor<ChannelsStatusRequest> captor =
        ArgumentCaptor.forClass(ChannelsStatusRequest.class);
    Mockito.verify(client).getChannelsStatus(captor.capture());
    ChannelsStatusRequest actualRequest = captor.getValue();

    Assert.assertEquals(expectedRequest.getRequestId(), actualRequest.getRequestId());
    Assert.assertEquals(expectedRequest.getChannels().length, actualRequest.getChannels().length);
    Assert.assertEquals(
        expectedRequest.getChannels()[0].getChannelName(),
        actualRequest.getChannels()[0].getChannelName());
    Assert.assertEquals(
        expectedRequest.getChannels()[0].getDatabaseName(),
        actualRequest.getChannels()[0].getDatabaseName());
    Assert.assertEquals(
        expectedRequest.getChannels()[0].getSchemaName(),
        actualRequest.getChannels()[0].getSchemaName());
    Assert.assertEquals(
        expectedRequest.getChannels()[0].getTableName(),
        actualRequest.getChannels()[0].getTableName());
    Assert.assertEquals(
        expectedRequest.getChannels()[0].getRowSequencer(),
        actualRequest.getChannels()[0].getRowSequencer());
    Assert.assertEquals(
        expectedRequest.getChannels()[0].getClientSequencer(),
        actualRequest.getChannels()[0].getClientSequencer());
  }

  @Test
  public void testRegisterBlobRequestCreationSuccess() throws Exception {
    Properties prop = new Properties();
    prop.put(USER_NAME, TestUtils.getUser());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ROLE_NAME, "role");
    prop = Utils.createProperties(prop, false);

    String urlStr = "https://sfctest0.snowflakecomputing.com:80";
    SnowflakeURL url = new SnowflakeURL(urlStr);

    KeyPair keyPair = Utils.createKeyPairFromPrivateKey((PrivateKey) prop.get(JDBC_PRIVATE_KEY));
    RequestBuilder requestBuilder =
        new RequestBuilder(url, prop.get(JDBC_USER).toString(), keyPair);

    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schemaName", "tableName", "0", 0L, 0L, null, true);

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
            .setEpInfo(epInfo)
            .build();

    List<BlobMetadata> blobs =
        Collections.singletonList(
            new BlobMetadata("name", "path", Collections.singletonList(chunkMetadata)));

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
            requestBuilder);

    try {
      List<BlobMetadata> blobs =
          Collections.singletonList(
              new BlobMetadata("name", "path", new ArrayList<ChunkMetadata>()));
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
            requestBuilder);

    try {
      List<BlobMetadata> blobs =
          Collections.singletonList(
              new BlobMetadata("name", "path", new ArrayList<ChunkMetadata>()));
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
            requestBuilder);

    List<BlobMetadata> blobs =
        Collections.singletonList(new BlobMetadata("name", "path", new ArrayList<ChunkMetadata>()));
    client.registerBlobs(blobs);
  }

  @Test
  public void testFlush() throws Exception {
    SnowflakeStreamingIngestClient client = new SnowflakeStreamingIngestClientInternal("client");

    client.flush().get();

    // Calling flush on closed client should fail
    client.close().get();
    try {
      client.flush().get();
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.CLOSED_CLIENT.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClose() throws Exception {
    SnowflakeStreamingIngestClient client = new SnowflakeStreamingIngestClientInternal("client");

    Assert.assertFalse(client.isClosed());
    client.close().get();
    Assert.assertTrue(client.isClosed());

    // Calling close again on closed client shouldn't fail
    client.close().get();

    // Calling open channel on closed client should fail
    try {
      OpenChannelRequest request =
          OpenChannelRequest.builder("CHANNEL")
              .setDBName("STREAMINGINGEST_TEST")
              .setSchemaName("PUBLIC")
              .setTableName("T_STREAMINGINGEST")
              .build();

      client.openChannel(request);
      Assert.fail("request should fail on closed client.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.CLOSED_CLIENT.getMessageCode(), e.getVendorCode());
    }
  }
}

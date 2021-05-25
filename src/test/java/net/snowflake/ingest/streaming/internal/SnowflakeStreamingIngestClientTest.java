package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.*;

import java.io.IOException;
import java.io.StringWriter;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.PrivateKey;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.nist.NISTObjectIdentifiers;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.OperatorCreationException;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfoBuilder;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.jcajce.JcaPKCS8EncryptedPrivateKeyInfoBuilder;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.jcajce.JcePKCSPBEOutputEncryptorBuilder;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.StreamingUtils;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

public class SnowflakeStreamingIngestClientTest {

  @Test
  public void testClientFactoryMissingName() throws Exception {
    Properties prop = new Properties();
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());

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
            StreamingUtils.parsePrivateKey(TestUtils.getPrivateKey()),
            testPassphrase.toCharArray());

    try {
      StreamingUtils.parseEncryptedPrivateKey(testKey, testPassphrase.substring(1));
      Assert.fail("Decryption should fail.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ENCRYPTED_KEY.getMessageCode(), e.getVendorCode());
    }

    StreamingUtils.parseEncryptedPrivateKey(testKey, testPassphrase);
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
  public void testRegisterBlobRequestCreationMissingField() throws Exception {
    try {
      RegisterBlobRequest request = RegisterBlobRequest.builder().build();
      Assert.fail("Request should fail with missing table name");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.NULL_VALUE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testRegisterBlobRequestCreationSuccess() throws Exception {
    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schemaName", "tableName", "0", 0L, 0L, null, true);

    ChannelMetadata channelMetadata =
        ChannelMetadata.builder()
            .setOwningChannel(channel)
            .setRowSequencer(channel.incrementAndGetRowSequencer())
            .setOffsetToken(channel.getOffsetToken())
            .build();

    ChunkMetadata chunkMetadata =
        ChunkMetadata.builder()
            .setOwningTable(channel)
            .setChunkStartOffset(0L)
            .setChunkLength(100)
            .setChannelList(Arrays.asList(channelMetadata))
            .build();

    List<BlobMetadata> blobs =
        Arrays.asList(new BlobMetadata("path", Arrays.asList(chunkMetadata)));

    RegisterBlobRequest request = RegisterBlobRequest.builder().setBlobList(blobs).build();

    String urlStr = "https://sfctest0.snowflakecomputing.com:80";
    SnowflakeURL url = new SnowflakeURL(urlStr);

    HttpRequest httpRequest = request.getHttpRequest(url);
    Assert.assertEquals(
        String.format("%s%s", urlStr, REGISTER_BLOB_ENDPOINT), httpRequest.uri().toString());
    Assert.assertEquals("application/json", httpRequest.headers().firstValue("content-type").get());
    Assert.assertEquals("application/json", httpRequest.headers().firstValue("accept").get());
    Assert.assertEquals("POST", httpRequest.method());
  }

  @Test
  public void testRegisterBlobErrorResponse() throws Exception {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(httpResponse.statusCode()).thenReturn(404);
    Mockito.when(httpClient.send(Mockito.any(), Mockito.any())).thenReturn(httpResponse);

    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal(
            "client", new SnowflakeURL("snowflake.dev.local:8082"), null, httpClient, true);

    try {
      List<BlobMetadata> blobs =
          Arrays.asList(new BlobMetadata("path", new ArrayList<ChunkMetadata>()));
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
    Mockito.when(httpResponse.statusCode()).thenReturn(200);
    Mockito.when(httpResponse.body()).thenReturn(response);
    Mockito.when(httpClient.send(Mockito.any(), Mockito.any())).thenReturn(httpResponse);

    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal(
            "client", new SnowflakeURL("snowflake.dev.local:8082"), null, httpClient, true);

    try {
      List<BlobMetadata> blobs =
          Arrays.asList(new BlobMetadata("path", new ArrayList<ChunkMetadata>()));
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
    Mockito.when(httpResponse.statusCode()).thenReturn(200);
    Mockito.when(httpResponse.body()).thenReturn(response);
    Mockito.when(httpClient.send(Mockito.any(), Mockito.any())).thenReturn(httpResponse);

    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal(
            "client", new SnowflakeURL("snowflake.dev.local:8082"), null, httpClient, true);

    List<BlobMetadata> blobs =
        Arrays.asList(new BlobMetadata("path", new ArrayList<ChunkMetadata>()));
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

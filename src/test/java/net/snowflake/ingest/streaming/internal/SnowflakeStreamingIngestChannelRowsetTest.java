package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.ACCOUNT_URL;
import static net.snowflake.ingest.utils.Constants.OPEN_ROWSET_CHANNEL_ON_PIPE_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.ROLE;
import static net.snowflake.ingest.utils.Constants.USER;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.internal.apache.commons.io.IOUtils;
import net.snowflake.client.jdbc.internal.apache.http.HttpEntity;
import net.snowflake.client.jdbc.internal.apache.http.HttpHeaders;
import net.snowflake.client.jdbc.internal.apache.http.StatusLine;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class SnowflakeStreamingIngestChannelRowsetTest {

  @Test
  public void testChannelFactoryNullFields() {
    String name = "CHANNEL";
    String dbName = "DATABASE";
    String schemaName = "SCHEMA";
    String pipeName = "PIPE";
    String continuationToken = "encode(client_sequencer, row_sequencer)";
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("client");

    Object[] fields =
        new Object[] {
          name,
          dbName,
          schemaName,
          pipeName,
          client,
          continuationToken,
          OpenChannelRequest.ChannelType.ROWSET_API
        };

    for (int i = 0; i < fields.length; i++) {
      Object tmp = fields[i];
      fields[i] = null;
      try {
        SnowflakeStreamingIngestChannelFactory.<StubChunkData>builder((String) fields[0])
            .setDBName((String) fields[1])
            .setSchemaName((String) fields[2])
            .setPipeName((String) fields[3])
            .setOwningClient((SnowflakeStreamingIngestClientInternal<StubChunkData>) fields[4])
            .setContinuationToken((String) fields[5])
            .setChannelType((OpenChannelRequest.ChannelType) fields[6])
            .build();
        Assert.fail("Channel factory should fail with null fields");
      } catch (SFException e) {
        Assert.assertTrue(
            ErrorCode.NULL_OR_EMPTY_STRING.getMessageCode().equals(e.getVendorCode())
                || ErrorCode.NULL_VALUE.getMessageCode().equals(e.getVendorCode()));
      }
      fields[i] = tmp;
    }
  }

  @Test
  public void testChannelFactorySuccess() {
    String name = "CHANNEL";
    String dbName = "DATABASE";
    String schemaName = "SCHEMA";
    String pipeName = "PIPE";
    String offsetToken = "0";
    String continuationToken = "encode(client_sequencer, row_sequencer)";

    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("client");

    SnowflakeStreamingIngestChannel channel =
        SnowflakeStreamingIngestChannelFactory.<StubChunkData>builder(name)
            .setDBName(dbName)
            .setSchemaName(schemaName)
            .setPipeName(pipeName)
            .setOffsetToken(offsetToken)
            .setOwningClient(client)
            .setContinuationToken(continuationToken)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .setChannelType(OpenChannelRequest.ChannelType.ROWSET_API)
            .build();

    Assert.assertEquals(name, channel.getName());
    Assert.assertEquals(dbName, channel.getDBName());
    Assert.assertEquals(schemaName, channel.getSchemaName());
    Assert.assertEquals(pipeName, channel.getTableOrPipeName());
    Assert.assertEquals(
        String.format("%s.%s.%s.%s", dbName, schemaName, pipeName, name),
        channel.getFullyQualifiedName());
    Assert.assertEquals(
        String.format("%s.%s.%s", dbName, schemaName, pipeName),
        channel.getFullyQualifiedTableOrPipeName());
    Assert.assertEquals(OpenChannelRequest.ChannelType.ROWSET_API, channel.getType());
  }

  @Test
  public void testOpenChannelRequestCreationSuccess() {
    OpenChannelRequest request =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName("STREAMINGINGEST_TEST")
            .setSchemaName("PUBLIC")
            .setTableName("T_STREAMINGINGEST")
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .setChannelType(OpenChannelRequest.ChannelType.ROWSET_API)
            .build();

    Assert.assertEquals(
        "STREAMINGINGEST_TEST.PUBLIC.T_STREAMINGINGEST", request.getFullyQualifiedTableName());
    Assert.assertFalse(request.isOffsetTokenProvided());
  }

  @Test
  public void testOpenChannelPostRequest() throws Exception {
    Properties prop = new Properties();
    prop.put(USER, TestUtils.getUser());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop.put(ACCOUNT_URL, TestUtils.getAccountURL());
    prop.put(ROLE, TestUtils.getRole());
    prop = Utils.createProperties(prop);

    String urlStr = "https://sfctest0.snowflakecomputing.com:80";
    SnowflakeURL url = new SnowflakeURL(urlStr);

    KeyPair keyPair =
        Utils.createKeyPairFromPrivateKey(
            (PrivateKey) prop.get(SFSessionProperty.PRIVATE_KEY.getPropertyKey()));
    RequestBuilder requestBuilder =
        new RequestBuilder(url, prop.get(USER).toString(), keyPair, null, null);

    Map<Object, Object> payload = new HashMap<>();
    payload.put("channel", "channel");

    String path = OPEN_ROWSET_CHANNEL_ON_PIPE_ENDPOINT;
    path = path.replace("{databaseName}", "db");
    path = path.replace("{schemaName}", "schema");
    path = path.replace("{pipeName}", "pipe");

    HttpPost request =
        requestBuilder.generateStreamingIngestPostRequest(
            payload, path, "open rowset channel", null);

    Assert.assertEquals(String.format("%s%s", urlStr, path), request.getRequestLine().getUri());
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
  public void testOpenChannelErrorResponse() throws Exception {
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(500);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
    String responseString = "testOpenChannelErrorResponse";
    Mockito.when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream(responseString));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair());
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder,
            null);

    OpenChannelRequest request =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName("STREAMINGINGEST_TEST")
            .setSchemaName("PUBLIC")
            .setTableName("T_STREAMINGINGEST")
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .setChannelType(OpenChannelRequest.ChannelType.ROWSET_API)
            .build();

    try {
      client.openChannel(request);
      Assert.fail("Open channel should fail on 500 error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.OPEN_CHANNEL_FAILURE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testOpenChannelSuccessResponse() throws Exception {
    String name = "CHANNEL";
    String dbName = "STREAMINGINGEST_TEST";
    String schemaName = "PUBLIC";
    String tableName = "T_STREAMINGINGEST";
    String continuationToken = "encode(client_sequencer, row_sequencer)";
    String response =
        "{\n"
            + "  \"status_code\" : 0,\n"
            + "  \"error_message\" : null,\n"
            + "  \"offset_token\" : null,\n"
            + "  \"continuation_token\" : \"encode(client_sequencer, row_sequencer)\"\n"
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

    OpenChannelRequest request =
        OpenChannelRequest.builder(name)
            .setDBName(dbName)
            .setSchemaName(schemaName)
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .setChannelType(OpenChannelRequest.ChannelType.ROWSET_API)
            .build();

    SnowflakeStreamingIngestChannel channel = client.openChannel(request);
    Assert.assertEquals(name, channel.getName());
    Assert.assertEquals(dbName, channel.getDBName());
    Assert.assertEquals(schemaName, channel.getSchemaName());
    Assert.assertEquals(tableName, channel.getTableName());
    Assert.assertEquals(
        continuationToken,
        ((SnowflakeStreamingIngestChannelRowset) channel).getContinuationToken());
  }
}

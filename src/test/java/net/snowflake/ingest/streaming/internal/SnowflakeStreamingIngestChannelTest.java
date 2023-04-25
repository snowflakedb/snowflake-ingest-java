package net.snowflake.ingest.streaming.internal;

import static java.time.ZoneOffset.UTC;
import static net.snowflake.ingest.utils.Constants.ACCOUNT_URL;
import static net.snowflake.ingest.utils.Constants.OPEN_CHANNEL_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;
import static net.snowflake.ingest.utils.Constants.ROLE;
import static net.snowflake.ingest.utils.Constants.USER;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class SnowflakeStreamingIngestChannelTest {

  /**
   * Mock memory provider, allows the user to set what memory values should be returned. Fields are
   * volatile, so can be modified by another threads than the one reading it.
   */
  private static class MockedMemoryInfoProvider implements MemoryInfoProvider {
    public volatile long freeMemory;
    public volatile long maxMemory;

    @Override
    public long getMaxMemory() {
      return maxMemory;
    }

    @Override
    public long getTotalMemory() {
      return maxMemory;
    }

    @Override
    public long getFreeMemory() {
      return freeMemory;
    }
  }

  @Test
  public void testChannelFactoryNullFields() {
    String name = "CHANNEL";
    String dbName = "DATABASE";
    String schemaName = "SCHEMA";
    String tableName = "TABLE";
    long channelSequencer = 0L;
    long rowSequencer = 0L;
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("client");

    Object[] fields =
        new Object[] {
          name, dbName, schemaName, tableName, channelSequencer, rowSequencer, client, UTC
        };

    for (int i = 0; i < fields.length; i++) {
      Object tmp = fields[i];
      fields[i] = null;
      try {
        SnowflakeStreamingIngestChannelFactory.<StubChunkData>builder((String) fields[0])
            .setDBName((String) fields[1])
            .setSchemaName((String) fields[2])
            .setTableName((String) fields[3])
            .setRowSequencer((Long) fields[4])
            .setChannelSequencer((Long) fields[5])
            .setOwningClient((SnowflakeStreamingIngestClientInternal<StubChunkData>) fields[6])
            .setDefaultTimezone((ZoneId) fields[7])
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
    String tableName = "TABLE";
    String offsetToken = "0";
    String encryptionKey = "key";
    Long channelSequencer = 0L;
    long rowSequencer = 0L;

    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("client");

    SnowflakeStreamingIngestChannelInternal<StubChunkData> channel =
        SnowflakeStreamingIngestChannelFactory.<StubChunkData>builder(name)
            .setDBName(dbName)
            .setSchemaName(schemaName)
            .setTableName(tableName)
            .setOffsetToken(offsetToken)
            .setRowSequencer(rowSequencer)
            .setChannelSequencer(channelSequencer)
            .setOwningClient(client)
            .setEncryptionKey(encryptionKey)
            .setEncryptionKeyId(1234L)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .setDefaultTimezone(UTC)
            .build();

    Assert.assertEquals(name, channel.getName());
    Assert.assertEquals(dbName, channel.getDBName());
    Assert.assertEquals(schemaName, channel.getSchemaName());
    Assert.assertEquals(tableName, channel.getTableName());
    Assert.assertEquals(offsetToken, channel.getChannelState().getOffsetToken());
    Assert.assertEquals(channelSequencer, channel.getChannelSequencer());
    Assert.assertEquals(rowSequencer + 1L, channel.getChannelState().incrementAndGetRowSequencer());
    Assert.assertEquals(
        String.format("%s.%s.%s.%s", dbName, schemaName, tableName, name),
        channel.getFullyQualifiedName());
    Assert.assertEquals(
        String.format("%s.%s.%s", dbName, schemaName, tableName),
        channel.getFullyQualifiedTableName());
  }

  @Test
  public void testChannelValid() {
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("client");
    SnowflakeStreamingIngestChannelInternal<StubChunkData> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
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
            UTC);

    Assert.assertTrue(channel.isValid());
    channel.invalidate("from testChannelValid");
    Assert.assertFalse(channel.isValid());

    // Can't insert rows to invalid channel
    try {
      Map<String, Object> row = new HashMap<>();
      row.put("col", 1);
      channel.insertRows(Collections.singletonList(row), "1");
      Assert.fail("Channel insert row should fail");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_CHANNEL.getMessageCode(), e.getVendorCode());
    }

    // Can't get latest committed offset token on invalid channel
    try {
      channel.getLatestCommittedOffsetToken();
      Assert.fail("Channel get offset token should failed");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_CHANNEL.getMessageCode(), e.getVendorCode());
    }

    // Can't close on invalid channel
    try {
      channel.close();
      Assert.fail("Channel close should failed");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_CHANNEL.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testChannelClose() {
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("client");
    SnowflakeStreamingIngestChannelInternal<StubChunkData> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
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
            UTC);

    Assert.assertFalse(channel.isClosed());
    channel.markClosed();
    Assert.assertTrue(channel.isClosed());

    // Can't insert rows to closed channel
    try {
      Map<String, Object> row = new HashMap<>();
      row.put("col", 1);
      channel.insertRows(Collections.singletonList(row), "1");
      Assert.fail("Channel insert row should fail");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.CLOSED_CHANNEL.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testOpenChannelRequestCreationMissingField() {
    try {
      OpenChannelRequest.builder("CHANNEL")
          .setDBName("STREAMINGINGEST_TEST")
          .setSchemaName("PUBLIC")
          .build();
      Assert.fail("Request should fail with missing table name");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.NULL_OR_EMPTY_STRING.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testOpenChannelRequestCreationSuccess() {
    OpenChannelRequest request =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName("STREAMINGINGEST_TEST")
            .setSchemaName("PUBLIC")
            .setTableName("T_STREAMINGINGEST")
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    Assert.assertEquals(
        "STREAMINGINGEST_TEST.PUBLIC.T_STREAMINGINGEST", request.getFullyQualifiedTableName());
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
    payload.put("channel", "CHANNEL");
    payload.put("table", "T_STREAMINGINGEST");
    payload.put("database", "STREAMINGINGEST_TEST");
    payload.put("schema", "PUBLIC");
    payload.put("write_mode", Constants.WriteMode.CLOUD_STORAGE.name());

    HttpPost request =
        requestBuilder.generateStreamingIngestPostRequest(
            payload, OPEN_CHANNEL_ENDPOINT, "open channel");

    Assert.assertEquals(
        String.format("%s%s", urlStr, OPEN_CHANNEL_ENDPOINT), request.getRequestLine().getUri());
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
            .build();

    try {
      client.openChannel(request);
      Assert.fail("Open channel should fail on 500 error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.OPEN_CHANNEL_FAILURE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testOpenChannelSnowflakeInternalErrorResponse() throws Exception {
    String response =
        "{\n"
            + "  \"status_code\" : 2,\n"
            + "  \"message\" : \"ERR_CHANNEL_NAME_EXISTS\",\n"
            + "  \"client_sequencer\" : 0,\n"
            + "  \"row_sequencer\" : 0,\n"
            + "  \"offset_token\" : \"\",\n"
            + "  \"table_columns\" : [ {\n"
            + "    \"name\" : \"C1\",\n"
            + "    \"type\" : \"NUMBER(38,0)\",\n"
            + "    \"logical_type\" : \"fixed\",\n"
            + "    \"physical_type\" : \"SB16\",\n"
            + "    \"precision\" : 38,\n"
            + "    \"scale\" : 0,\n"
            + "    \"byte_length\" : null,\n"
            + "    \"length\" : null,\n"
            + "    \"nullable\" : true\n"
            + "  }, {\n"
            + "    \"name\" : \"C2\",\n"
            + "    \"type\" : \"NUMBER(38,0)\",\n"
            + "    \"logical_type\" : \"fixed\",\n"
            + "    \"physical_type\" : \"SB16\",\n"
            + "    \"precision\" : 38,\n"
            + "    \"scale\" : 0,\n"
            + "    \"byte_length\" : null,\n"
            + "    \"length\" : null,\n"
            + "    \"nullable\" : true\n"
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
            .build();

    try {
      client.openChannel(request);
      Assert.fail("Open channel should fail on Snowflake internal error");
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
    String response =
        "{\n"
            + "  \"status_code\" : 0,\n"
            + "  \"message\" : \"Success\",\n"
            + "  \"database\" : \"STREAMINGINGEST_TEST\",\n"
            + "  \"schema\" : \"PUBLIC\",\n"
            + "  \"table\" : \"T_STREAMINGINGEST\",\n"
            + "  \"channel\" : \"CHANNEL\",\n"
            + "  \"client_sequencer\" : 0,\n"
            + "  \"row_sequencer\" : 0,\n"
            + "  \"offset_token\" : \"\",\n"
            + "  \"encryption_key_id\" : 17229585102,\n"
            + "  \"table_columns\" : [ {\n"
            + "    \"name\" : \"C1\",\n"
            + "    \"type\" : \"NUMBER(38,0)\",\n"
            + "    \"logical_type\" : \"fixed\",\n"
            + "    \"physical_type\" : \"SB16\",\n"
            + "    \"precision\" : 38,\n"
            + "    \"scale\" : 0,\n"
            + "    \"byte_length\" : null,\n"
            + "    \"length\" : null,\n"
            + "    \"nullable\" : true\n"
            + "  }, {\n"
            + "    \"name\" : \"C2\",\n"
            + "    \"type\" : \"NUMBER(38,0)\",\n"
            + "    \"logical_type\" : \"fixed\",\n"
            + "    \"physical_type\" : \"SB16\",\n"
            + "    \"precision\" : 38,\n"
            + "    \"scale\" : 0,\n"
            + "    \"byte_length\" : null,\n"
            + "    \"length\" : null,\n"
            + "    \"nullable\" : true\n"
            + "  } ],\n"
            + "  \"encryption_key\" : \"3/l6q2xeDurO4ljfde4DXA==\"\n"
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
            .build();

    SnowflakeStreamingIngestChannel channel = client.openChannel(request);
    Assert.assertEquals(name, channel.getName());
    Assert.assertEquals(dbName, channel.getDBName());
    Assert.assertEquals(schemaName, channel.getSchemaName());
    Assert.assertEquals(tableName, channel.getTableName());
  }

  @Test
  public void testInsertRow() {
    SnowflakeStreamingIngestClientInternal<?> client;
    boolean isArrowDefault =
        ParameterProvider.BLOB_FORMAT_VERSION_DEFAULT == Constants.BdecVersion.ONE;
    if (isArrowDefault) {
      client = new SnowflakeStreamingIngestClientInternal<VectorSchemaRoot>("client_ARROW");
    } else {
      client = new SnowflakeStreamingIngestClientInternal<ParquetChunkData>("client_PARQUET");
    }

    SnowflakeStreamingIngestChannelInternal<?> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
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
            UTC);

    ColumnMetadata col = new ColumnMetadata();
    col.setName("COL");
    col.setPhysicalType("SB16");
    col.setNullable(false);
    col.setLogicalType("FIXED");
    col.setPrecision(38);
    col.setScale(0);

    // Setup column fields and vectors
    channel.setupSchema(Collections.singletonList(col));

    Map<String, Object> row = new HashMap<>();
    row.put("col", 1);

    // Get data before insert to verify that there is no row (data should be null)
    ChannelData<?> data = channel.getData("my_snowpipe_streaming.bdec");
    Assert.assertNull(data);

    long insertStartTimeInMs = System.currentTimeMillis();
    InsertValidationResponse response = channel.insertRow(row, "1");
    Assert.assertFalse(response.hasErrors());
    response = channel.insertRows(Collections.singletonList(row), "2");
    Assert.assertFalse(response.hasErrors());
    long insertEndTimeInMs = System.currentTimeMillis();

    // Get data again to verify the row is inserted
    data = channel.getData("my_snowpipe_streaming.bdec");
    Assert.assertEquals(2, data.getRowCount());
    Assert.assertEquals((Long) 1L, data.getRowSequencer());
    Assert.assertEquals(
        1,
        isArrowDefault
            ? ((ChannelData<VectorSchemaRoot>) data).getVectors().getFieldVectors().size()
            : ((ChannelData<ParquetChunkData>) data).getVectors().rows.get(0).size());
    Assert.assertEquals("2", data.getOffsetToken());
    Assert.assertTrue(data.getBufferSize() > 0);
    Assert.assertTrue(insertStartTimeInMs <= data.getMinMaxInsertTimeInMs().getFirst());
    Assert.assertTrue(insertEndTimeInMs >= data.getMinMaxInsertTimeInMs().getSecond());
  }

  @Test
  public void testInsertRowThrottling() {
    final long maxMemory = 1000000L;

    final MockedMemoryInfoProvider memoryInfoProvider = new MockedMemoryInfoProvider();
    memoryInfoProvider.maxMemory = maxMemory;

    SnowflakeStreamingIngestClientInternal<?> client =
        new SnowflakeStreamingIngestClientInternal<>("client");
    SnowflakeStreamingIngestChannelInternal<?> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
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
            UTC);

    ParameterProvider parameterProvider = new ParameterProvider();
    memoryInfoProvider.freeMemory =
        maxMemory * (parameterProvider.getInsertThrottleThresholdInPercentage() - 1) / 100;

    CompletableFuture<Void> future =
        CompletableFuture.runAsync(() -> channel.throttleInsertIfNeeded(memoryInfoProvider));

    try {
      future.get(5L, TimeUnit.SECONDS);
      Assert.fail("the insert should be throttled.");
    } catch (TimeoutException ignored) {
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("unexpected exception encountered.");
    }

    memoryInfoProvider.freeMemory = maxMemory;

    // We should succeed now
    try {
      future.get(5L, TimeUnit.SECONDS);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("unexpected exception encountered.");
    }
  }

  @Test
  public void testFlush() throws Exception {
    SnowflakeStreamingIngestClientInternal<?> client =
        Mockito.spy(new SnowflakeStreamingIngestClientInternal<>("client"));
    SnowflakeStreamingIngestChannelInternal<?> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
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
            UTC);
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("Success");
    response.setChannels(new ArrayList<>());

    Mockito.doReturn(response).when(client).getChannelsStatus(Mockito.any());

    channel.flush(false).get();

    // Calling flush on closed client should fail
    channel.close().get();
    try {
      channel.flush(false).get();
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.CLOSED_CHANNEL.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClose() throws Exception {
    SnowflakeStreamingIngestClientInternal<?> client =
        Mockito.spy(new SnowflakeStreamingIngestClientInternal<>("client"));
    SnowflakeStreamingIngestChannel channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
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
            UTC);
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("Success");
    response.setChannels(new ArrayList<>());

    Mockito.doReturn(response).when(client).getChannelsStatus(Mockito.any());

    Assert.assertFalse(channel.isClosed());
    channel.close().get();
    Assert.assertTrue(channel.isClosed());

    // Calling close again on closed channel shouldn't fail
    channel.close().get();
  }

  @Test
  public void testGetLatestCommittedOffsetToken() {
    String offsetToken = "10";
    SnowflakeStreamingIngestClientInternal<?> client =
        Mockito.spy(new SnowflakeStreamingIngestClientInternal<>("client"));
    SnowflakeStreamingIngestChannel channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
            "db",
            "schema",
            "table",
            offsetToken,
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            UTC);

    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("Success");

    // Test success case
    ChannelsStatusResponse.ChannelStatusResponseDTO channelStatus =
        new ChannelsStatusResponse.ChannelStatusResponseDTO();
    channelStatus.setStatusCode(RESPONSE_SUCCESS);
    channelStatus.setPersistedOffsetToken(offsetToken);
    response.setChannels(Collections.singletonList(channelStatus));

    Mockito.doReturn(response).when(client).getChannelsStatus(Mockito.any());

    Assert.assertEquals(offsetToken, channel.getLatestCommittedOffsetToken());

    // Test error case
    channelStatus.setStatusCode(-1L);
    try {
      Assert.assertEquals(offsetToken, channel.getLatestCommittedOffsetToken());
      Assert.fail("Get offset token should fail");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.CHANNEL_STATUS_INVALID.getMessageCode(), e.getVendorCode());
    }
  }
}

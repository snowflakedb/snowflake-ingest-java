package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.JDBC_PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.JDBC_USER;
import static net.snowflake.ingest.utils.Constants.OPEN_CHANNEL_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.USER_NAME;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.jdbc.internal.apache.commons.io.IOUtils;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class SnowflakeStreamingIngestChannelTest {

  @Test
  public void testChannelFactoryNullFields() {
    String name = "CHANNEL";
    String dbName = "DATABASE";
    String schemaName = "SCHEMA";
    String tableName = "TABLE";
    Long channelSequencer = 0L;
    Long rowSequencer = 0L;
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal("client");

    Object[] fields =
        new Object[] {name, dbName, schemaName, tableName, channelSequencer, rowSequencer, client};

    for (int i = 0; i < fields.length; i++) {
      Object tmp = fields[i];
      fields[i] = null;
      try {
        SnowflakeStreamingIngestChannelInternal channel =
            SnowflakeStreamingIngestChannelFactory.builder((String) fields[0])
                .setDBName((String) fields[1])
                .setSchemaName((String) fields[2])
                .setTableName((String) fields[3])
                .setRowSequencer((Long) fields[4])
                .setChannelSequencer((Long) fields[5])
                .setOwningClient((SnowflakeStreamingIngestClientInternal) fields[6])
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
    Long channelSequencer = 0L;
    Long rowSequencer = 0L;

    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal("client");

    SnowflakeStreamingIngestChannelInternal channel =
        SnowflakeStreamingIngestChannelFactory.builder(name)
            .setDBName(dbName)
            .setSchemaName(schemaName)
            .setTableName(tableName)
            .setOffsetToken(offsetToken)
            .setRowSequencer(rowSequencer)
            .setChannelSequencer(channelSequencer)
            .setOwningClient(client)
            .build();

    Assert.assertEquals(name, channel.getName());
    Assert.assertEquals(dbName, channel.getDBName());
    Assert.assertEquals(schemaName, channel.getSchemaName());
    Assert.assertEquals(tableName, channel.getTableName());
    Assert.assertEquals(offsetToken, channel.getOffsetToken());
    Assert.assertEquals(channelSequencer, channel.getChannelSequencer());
    Assert.assertEquals(rowSequencer + 1L, channel.incrementAndGetRowSequencer());
    Assert.assertEquals(
        String.format("%s.%s.%s.%s", dbName, schemaName, tableName, name),
        channel.getFullyQualifiedName());
    Assert.assertEquals(
        String.format("%s.%s.%s", dbName, schemaName, tableName),
        channel.getFullyQualifiedTableName());
  }

  @Test
  public void testChannelValid() {
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal("client");
    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schema", "table", "0", 0L, 0L, client, true);

    Assert.assertTrue(channel.isValid());
    channel.invalidate();
    Assert.assertTrue(!channel.isValid());

    // Can't insert rows to invalid channel
    try {
      Map<String, Object> row = new HashMap<>();
      row.put("col", 1);
      channel.insertRows(Collections.singletonList(row), "1");
      Assert.fail("Channel insert row should fail");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_CHANNEL.getMessageCode(), e.getVendorCode());
    }

    // Can't flush on invalid channel
    try {
      channel.flush();
      Assert.fail("Channel flush should failed");
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
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal("client");
    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schema", "table", "0", 0L, 0L, client, true);

    Assert.assertTrue(!channel.isClosed());
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
      OpenChannelRequest request =
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
            .build();

    Assert.assertEquals(
        "STREAMINGINGEST_TEST.PUBLIC.T_STREAMINGINGEST", request.getFullyQualifiedTableName());
  }

  @Test
  public void testOpenChannelPostRequest() throws Exception {
    Properties prop = new Properties();
    prop.put(USER_NAME, TestUtils.getUser());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());
    prop = Utils.createProperties(prop, false);

    String urlStr = "https://sfctest0.snowflakecomputing.com:80";
    SnowflakeURL url = new SnowflakeURL(urlStr);

    KeyPair keyPair = Utils.createKeyPairFromPrivateKey((PrivateKey) prop.get(JDBC_PRIVATE_KEY));
    RequestBuilder requestBuilder =
        new RequestBuilder(url, prop.get(JDBC_USER).toString(), keyPair);

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
        String.format("%s%s", urlStr, OPEN_CHANNEL_ENDPOINT),
        request.getRequestLine().getUri().toString());
    Assert.assertNotNull(request.getFirstHeader(HttpHeaders.USER_AGENT));
    Assert.assertNotNull(request.getFirstHeader(HttpHeaders.AUTHORIZATION));
    Assert.assertEquals("POST", request.getMethod());
  }

  @Test
  public void testOpenChannelErrorResponse() throws Exception {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
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
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal(
            "client",
            new SnowflakeURL("snowflake.dev.local:8082"),
            null,
            httpClient,
            true,
            requestBuilder);

    OpenChannelRequest request =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName("STREAMINGINGEST_TEST")
            .setSchemaName("PUBLIC")
            .setTableName("T_STREAMINGINGEST")
            .build();

    try {
      SnowflakeStreamingIngestChannel channel = client.openChannel(request);
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

    OpenChannelRequest request =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName("STREAMINGINGEST_TEST")
            .setSchemaName("PUBLIC")
            .setTableName("T_STREAMINGINGEST")
            .build();

    try {
      SnowflakeStreamingIngestChannel channel = client.openChannel(request);
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

    OpenChannelRequest request =
        OpenChannelRequest.builder(name)
            .setDBName(dbName)
            .setSchemaName(schemaName)
            .setTableName(tableName)
            .build();

    SnowflakeStreamingIngestChannel channel = client.openChannel(request);
    Assert.assertEquals(name, channel.getName());
    Assert.assertEquals(dbName, channel.getDBName());
    Assert.assertEquals(schemaName, channel.getSchemaName());
    Assert.assertEquals(tableName, channel.getTableName());
  }

  @Test
  public void testInsertRow() {
    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schema", "table", "0", 0L, 0L, null, true);

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
    ChannelData data = channel.getData();
    Assert.assertNull(data);

    try {
      channel.insertRow(row, "1");
      channel.insertRows(Collections.singletonList(row), "2");
    } catch (Exception e) {
      Assert.fail("Row buffer insert row failed");
    }

    // Get data again to verify the row is inserted
    data = channel.getData();
    Assert.assertEquals(2, data.getRowCount());
    Assert.assertEquals((Long) 1L, data.getRowSequencer());
    Assert.assertEquals(1, data.getVectors().size());
    Assert.assertEquals("2", data.getOffsetToken());
    Assert.assertTrue(data.getBufferSize() > 0);
  }

  @Test
  public void testFlush() throws Exception {
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal("client");
    SnowflakeStreamingIngestChannel channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schema", "table", "0", 0L, 0L, client, true);

    channel.flush().get();

    // Calling flush on closed client should fail
    channel.close().get();
    try {
      channel.flush().get();
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.CLOSED_CHANNEL.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClose() throws Exception {
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal("client");
    SnowflakeStreamingIngestChannel channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schema", "table", "0", 0L, 0L, client, true);

    Assert.assertFalse(channel.isClosed());
    channel.close().get();
    Assert.assertTrue(channel.isClosed());

    // Calling close again on closed channel shouldn't fail
    channel.close().get();
  }
}

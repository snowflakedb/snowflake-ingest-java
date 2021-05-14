package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.Constants.OPEN_CHANNEL_ENDPOINT;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class SnowflakeStreamingIngestChannelTest extends TestCase {

  @Test
  public void testChannelFactoryNullFields() throws Exception {
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
        Assert.assertEquals(ErrorCode.NULL_OR_EMPTY_STRING.getMessageCode(), e.getVendorCode());
      }
      fields[i] = tmp;
    }
  }

  @Test
  public void testChannelFactorySuccess() throws Exception {
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
  }

  @Test
  public void testChannelClose() {
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal("client");
    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schema", "table", "0", 0L, 0L, client, true);

    Assert.assertTrue(channel.isClosed());
    channel.markClosed();
    Assert.assertTrue(!channel.isClosed());

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
  public void testOpenChannelRequestCreationMissingField() throws Exception {
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
  public void testOpenChannelRequestCreationSuccess() throws Exception {
    OpenChannelRequest request =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName("STREAMINGINGEST_TEST")
            .setSchemaName("PUBLIC")
            .setTableName("T_STREAMINGINGEST")
            .build();

    Assert.assertEquals(
        "STREAMINGINGEST_TEST.PUBLIC.T_STREAMINGINGEST", request.getFullyQualifiedTableName());

    String urlStr = "https://sfctest0.snowflakecomputing.com:80";
    SnowflakeURL url = new SnowflakeURL(urlStr);

    HttpRequest httpRequest = request.getHttpRequest(url);
    Assert.assertEquals(
        String.format("%s%s", urlStr, OPEN_CHANNEL_ENDPOINT), httpRequest.uri().toString());
    Assert.assertEquals("application/json", httpRequest.headers().firstValue("content-type").get());
    Assert.assertEquals("application/json", httpRequest.headers().firstValue("accept").get());
    Assert.assertEquals("POST", httpRequest.method());
  }

  @Test
  public void testOpenChannelErrorResponse() throws Exception {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(httpResponse.statusCode()).thenReturn(500);
    Mockito.when(httpClient.send(Mockito.any(), Mockito.any())).thenReturn(httpResponse);

    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal(
            "client", new SnowflakeURL("snowflake.dev.local:8082"), null, httpClient, true);

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
    Mockito.when(httpResponse.statusCode()).thenReturn(200);
    Mockito.when(httpResponse.body()).thenReturn(response);
    Mockito.when(httpClient.send(Mockito.any(), Mockito.any())).thenReturn(httpResponse);

    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal(
            "client", new SnowflakeURL("snowflake.dev.local:8082"), null, httpClient, true);

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
    String accountURL = "snowflake.dev.local:8082";
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
    Mockito.when(httpResponse.statusCode()).thenReturn(200);
    Mockito.when(httpResponse.body()).thenReturn(response);
    Mockito.when(httpClient.send(Mockito.any(), Mockito.any())).thenReturn(httpResponse);

    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal(
            "client", new SnowflakeURL("snowflake.dev.local:8082"), null, httpClient, true);

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
    channel.setupSchema(Arrays.asList(col));

    Map<String, Object> row = new HashMap<>();
    row.put("col", 1);

    // Get data before insert to verify that there is no row (data should be null)
    ChannelData data = channel.getData();
    Assert.assertEquals(null, data);

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
}

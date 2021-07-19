package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_NO_HEADER;
import static net.snowflake.ingest.utils.Constants.COMPRESS_BLOB_TWICE;
import static net.snowflake.ingest.utils.Constants.ENABLE_PERF_MEASUREMENT;

import java.io.FileInputStream;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Example streaming ingest sdk integration test */
public class StreamingIngestIT {
  private static String PROFILE_PATH = "profile.properties";
  private static String TEST_TABLE = "STREAMING_INGEST_TEST_TABLE";
  private static String TEST_DB = "STREAMING_INGEST_TEST_DB";
  private static String TEST_SCHEMA = "STREAMING_INGEST_TEST_SCHEMA";

  private SnowflakeStreamingIngestClientInternal client;
  private Connection jdbcConnection;

  @Before
  public void beforeAll() throws Exception {
    Properties prop = new Properties();
    prop.load(new FileInputStream(PROFILE_PATH));

    // Create a streaming ingest client
    SnowflakeURL accountURL = new SnowflakeURL(prop.getProperty(Constants.ACCOUNT_URL));
    Properties parsedProp = Utils.createProperties(prop, accountURL.sslEnabled());
    jdbcConnection = new SnowflakeDriver().connect(accountURL.getJdbcUrl(), parsedProp);
    jdbcConnection
        .createStatement()
        .execute(String.format("create or replace database %s;", TEST_DB));
    jdbcConnection
        .createStatement()
        .execute(String.format("create or replace schema %s;", TEST_SCHEMA));
    jdbcConnection
        .createStatement()
        .execute(String.format("create or replace table %s (c1 char(10));", TEST_TABLE));
    jdbcConnection
        .createStatement()
        .execute("alter session set enable_streaming_ingest_reads=true;");
    jdbcConnection
        .createStatement()
        .execute("alter session set ENABLE_STREAMING_INGEST_UNNAMED_STAGE_QUERY=true;");
    jdbcConnection
        .createStatement()
        .execute(String.format("use warehouse %s", prop.get("warehouse")));
    client =
        (SnowflakeStreamingIngestClientInternal)
            SnowflakeStreamingIngestClientFactory.builder("client1").setProperties(prop).build();
  }

  @After
  public void afterAll() throws Exception {
    client.close().get();
    jdbcConnection.createStatement().execute(String.format("drop database %s", TEST_DB));
  }

  @Test
  public void testSimpleIngest() throws Exception {
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(TEST_DB)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
    for (int val = 0; val < 1000; val++) {
      Map<String, Object> row = new HashMap<>();
      row.put("c1", Integer.toString(val));
      channel1.insertRow(row, Integer.toString(val));
    }
    client.flush().get();
    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("999")) {
        ResultSet result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select count(*) from %s.%s.%s", TEST_DB, TEST_SCHEMA, TEST_TABLE));
        result.next();
        Assert.assertEquals(1000, result.getLong(1));

        ResultSet result2 =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select * from %s.%s.%s order by c1 limit 2",
                        TEST_DB, TEST_SCHEMA, TEST_TABLE));
        result2.next();
        Assert.assertEquals("0", result2.getString(1));
        result2.next();
        Assert.assertEquals("1", result2.getString(1));
        // Verify perf metrics
        if (ENABLE_PERF_MEASUREMENT) {
          Assert.assertEquals(1, client.blobSizeHistogram.getCount());
          if (BLOB_NO_HEADER && COMPRESS_BLOB_TWICE) {
            Assert.assertEquals(3445, client.blobSizeHistogram.getSnapshot().getMax());
          } else if (BLOB_NO_HEADER) {
            Assert.assertEquals(3579, client.blobSizeHistogram.getSnapshot().getMax());
          } else if (COMPRESS_BLOB_TWICE) {
            Assert.assertEquals(3981, client.blobSizeHistogram.getSnapshot().getMax());
          } else {
            Assert.assertEquals(4115, client.blobSizeHistogram.getSnapshot().getMax());
          }
        }
        return;
      }
      Thread.sleep(500);
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testDecimalColumnIngest() throws Exception {
    String decimalTableName = "decimal_table";
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s (tinyfloat NUMBER(38,2));", decimalTableName));
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL_DECI")
            .setDBName(TEST_DB)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(decimalTableName)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

    Map<String, Object> row = new HashMap<>();
    row.put("tinyfloat", -1.1);
    channel1.insertRow(row, null);
    row.put("tinyfloat", 2.2);
    channel1.insertRow(row, null);
    row.put("tinyfloat", BigInteger.valueOf(10).pow(35));
    channel1.insertRow(row, "1");

    client.flush().get();
    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("1")) {
        ResultSet result = null;
        result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select * from %s.%s.%s", TEST_DB, TEST_SCHEMA, decimalTableName));

        result.next();
        Assert.assertEquals(-1.1, result.getFloat("TINYFLOAT"), 0.001);
        result.next();
        Assert.assertEquals(2.2, result.getFloat("TINYFLOAT"), 0.001);
        result.next();
        Assert.assertEquals(
            BigInteger.valueOf(10).pow(35).floatValue(), result.getFloat("TINYFLOAT"), 10);
        return;
      } else {
        Thread.sleep(2000);
      }
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testMultiColumnIngest() throws Exception {
    String multiTableName = "multi_column";
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s (s text, i integer, f float, var variant, t"
                    + " timestamp_ntz, tinyfloat NUMBER(3,1));",
                multiTableName));
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL_MULTI")
            .setDBName(TEST_DB)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(multiTableName)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
    long timestamp = System.currentTimeMillis();
    timestamp = timestamp / 1000;
    Map<String, Object> row = new HashMap<>();
    row.put("s", "honk");
    row.put("i", 1);
    row.put("f", 3.14);
    row.put("tinyfloat", 1.1);
    row.put("var", "{\"e\":2.7}");
    row.put("t", timestamp);
    channel1.insertRow(row, "1");

    client.flush().get();
    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("1")) {
        ResultSet result = null;
        result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format("select * from %s.%s.%s", TEST_DB, TEST_SCHEMA, multiTableName));

        result.next();
        Assert.assertEquals("honk", result.getString("S"));
        Assert.assertEquals(1, result.getLong("I"));
        Assert.assertEquals(3.14, result.getFloat("F"), 0.0001);
        Assert.assertEquals(1.1, result.getFloat("TINYFLOAT"), 0.001);
        Assert.assertEquals("{\n" + "  \"e\": 2.7\n" + "}", result.getString("VAR"));
        SnowflakeDateTimeFormat ntzFormat =
            SnowflakeDateTimeFormat.fromSqlFormat("DY, DD MON YYYY HH24:MI:SS TZHTZM");
        Assert.assertEquals(timestamp * 1000, ntzFormat.parse(result.getString("T")).getTime());
        return;
      } else {
        Thread.sleep(2000);
      }
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testNullableColumns() throws Exception {
    String multiTableName = "multi_column";
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s (s text, notnull text NOT NULL);", multiTableName));
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL_MULTI")
            .setDBName(TEST_DB)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(multiTableName)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

    Map<String, Object> row = new HashMap<>();
    row.put("s", "honk");
    row.put("notnull", "foo");
    channel1.insertRow(row, "1");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("s", null);
    row2.put("notnull", "bar");
    channel1.insertRow(row2, "2");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("notnull", "foobar");
    channel1.insertRow(row3, "3");

    client.flush().get();
    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("3")) {
        ResultSet result = null;
        result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format("select * from %s.%s.%s", TEST_DB, TEST_SCHEMA, multiTableName));

        result.next();
        Assert.assertEquals("honk", result.getString("S"));
        Assert.assertEquals("foo", result.getString("NOTNULL"));

        result.next();
        Assert.assertNull(result.getString("S"));
        Assert.assertEquals("bar", result.getString("NOTNULL"));

        result.next();
        Assert.assertNull(result.getString("S"));
        Assert.assertEquals("foobar", result.getString("NOTNULL"));
        return;
      } else {
        Thread.sleep(2000);
      }
    }
    Assert.fail("Row sequencer not updated before timeout");
  }
}

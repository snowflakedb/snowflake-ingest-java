package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_NO_HEADER;
import static net.snowflake.ingest.utils.Constants.COMPRESS_BLOB_TWICE;
import static net.snowflake.ingest.utils.Constants.ENABLE_PERF_MEASUREMENT;
import static net.snowflake.ingest.utils.Constants.STAGE_NAME;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.jdbc.SnowflakeDriver;
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
        .execute(String.format("create or replace stage %s", STAGE_NAME));
    client =
        (SnowflakeStreamingIngestClientInternal)
            SnowflakeStreamingIngestClientFactory.builder("client1").setProperties(prop).build();
  }

  @After
  public void afterAll() throws Exception {
    client.close().get();
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
    ChannelsStatusResponse beforeStatus = client.getChannelsStatus();
    for (int val = 0; val < 1000; val++) {
      Map<String, Object> row = new HashMap<>();
      row.put("c1", Integer.toString(val));
      channel1.insertRow(row, null);
    }
    client.flush().get();
    for (int i = 1; i < 15; i++) {
      ChannelsStatusResponse afterStatus = client.getChannelsStatus();
      if (afterStatus.getChannels()[0].getPersistedRowSequencer()
          > beforeStatus.getChannels()[0].getPersistedRowSequencer()) {
        ResultSet result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select count(*) from %s.%s.%s", TEST_DB, TEST_SCHEMA, TEST_TABLE));
        result.next();
        Assert.assertEquals(1000, result.getLong(1));
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
      } else {
        Thread.sleep(500);
      }
    }
    Assert.fail("Row sequencer not updated before timeout");
  }
}

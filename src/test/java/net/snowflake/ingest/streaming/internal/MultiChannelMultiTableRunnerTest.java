package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import static net.snowflake.ingest.utils.Constants.ROLE;

public class MultiChannelMultiTableRunnerTest {
  private static final int NUM_CHANNELS = 15;
  private static final int NUM_TABLES = 7;

  private static final int ITEMS_COUNT = 200_000;

  @Test
  public void name() throws Exception {
    final String databaseName =
        String.format("MULTI_CHANNEL_MULTI_TABLE_RUNNER_%s", System.currentTimeMillis());
    Connection conn = TestUtils.getConnection(true);
    conn.createStatement().execute(String.format("create or replace database %s;", databaseName));
    conn.createStatement().execute(String.format("use database %s;", databaseName));
    conn.createStatement().execute("use schema public;");

    List<String> tables = new ArrayList<>();

    for (int i = 0; i < NUM_TABLES; i++) {
      String tableName = String.format("test_table_%d", i);
      tables.add(tableName);
      conn.createStatement()
          .execute(
              String.format(
                  "create table %s (id number, tablename text, channelname text)", tableName));
    }

    conn.createStatement().execute(String.format("use warehouse %s;", TestUtils.getWarehouse()));

    Properties props = TestUtils.getProperties(Constants.BdecVersion.THREE);
    if (props.getProperty(ROLE).equals("DEFAULT_ROLE")) {
      props.setProperty(ROLE, "ACCOUNTADMIN");
    }
    SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("client1").setProperties(props).build();

    final Thread[] threads = new Thread[NUM_CHANNELS];
    ConcurrentLinkedQueue<Exception> errors = new ConcurrentLinkedQueue<>();

    for (int threadId = 0; threadId < NUM_CHANNELS; threadId++) {
      final int threadId2 = threadId;

      threads[threadId] =
          new Thread(
              () -> {
                final String channelName = String.format("channel_%d", threadId2);
                try {
                  String targetTable = tables.get(threadId2 % NUM_TABLES);
                  final SnowflakeStreamingIngestChannel channel =
                      client.openChannel(
                          OpenChannelRequest.builder(channelName)
                              .setDBName(databaseName)
                              .setSchemaName("PUBLIC")
                              .setTableName(targetTable)
                              .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                              .build());

                  Map<String, Object> row = new HashMap<>();
                  for (int i = 0; i < ITEMS_COUNT; i++) {
                    row.put("id", i);
                    row.put("tablename", targetTable);
                    row.put("channelname", channelName);
                    InsertValidationResponse validation = channel.insertRow(row, String.valueOf(i));
                    if (validation.hasErrors()) {
                      throw validation.getInsertErrors().get(0).getException();
                    }
                  }
                } catch (Exception e) {
                  errors.add(e);
                }
              });
    }


    for (Thread t : threads) {
      t.start();
    }

    System.out.println("All threads started");
    Instant start = Instant.now();

    for (Thread t : threads) {
      t.join();
    }

    System.out.printf("Test finished, duration: %d ms%n", Duration.between(start, Instant.now()).toMillis());

    if (!errors.isEmpty()) {
      errors.forEach(Throwable::printStackTrace);
      Assert.fail(String.format("Exception thrown count=%d", errors.size()));
    }



    tables.forEach(
        table -> {
          try {
            ResultSet rs =
                conn.createStatement()
                    .executeQuery(String.format("select count(*) from %s", table));
            rs.next();
            int count = rs.getInt(1);
            System.out.println(count);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }
}

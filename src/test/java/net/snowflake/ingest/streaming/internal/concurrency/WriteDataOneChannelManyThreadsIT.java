package net.snowflake.ingest.streaming.internal.concurrency;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.Constants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Contains test verifying concurrent behavior during concurrent ingestion of data from multiple
 * threads. Each test ingests data for the specified amount of time and at the end it migrates the
 * table.
 */
public class WriteDataOneChannelManyThreadsIT {
  private static final int THREAD_COUNT = 32;

  /** How long should the test run */
  private static final Duration TEST_DURATION = Duration.of(1, ChronoUnit.MINUTES);

  private static final Random RANDOM = new Random();
  private Connection conn;
  private String database;

  @Before
  public void setUp() throws Exception {
    conn = TestUtils.getConnection(true);
    database = TestUtils.createDatabase(conn);
  }

  @After
  public void tearDown() throws Exception {
    conn.createStatement().execute(String.format("drop database %s", database));
  }

  /** Test data ingestion from multiple threads via a single shared channel instance. */
  @Test
  public void testIngestViaSingleChannel() throws Exception {
    final Instant testStartTime = Instant.now();
    final String tableName = createTable(0);
    try (SnowflakeStreamingIngestClient client =
        TestUtils.createStreamingIngestClient(Constants.BdecVersion.THREE)) {
      final SnowflakeStreamingIngestChannel channel =
          TestUtils.openChannel(client, database, tableName);
      ConcurrencyTestUtils.doInManyThreads(
          THREAD_COUNT,
          threadId -> {
            long counter = 0;
            while (Duration.between(testStartTime, Instant.now()).compareTo(TEST_DURATION) <= 0) {
              InsertValidationResponse insertValidationResponse =
                  channel.insertRow(createRow(counter), String.format("%d_%d", threadId, counter));
              Assert.assertFalse(insertValidationResponse.hasErrors());
              counter++;
            }
          });

      // All threads finished writing, insert a final row, so we know which offset to wait for
      channel.insertRow(createRow(Long.MAX_VALUE), "OFFSET1");
      TestUtils.waitForOffset(channel, "OFFSET1");

      migrateTable(tableName);
    }
  }

  /**
   * Test data ingestion from multiple threads via multiple channels (from a single client) into a
   * single table
   */
  @Test
  public void testIngestViaMultipleChannelsSingleTable() throws Exception {
    final Instant testStartTime = Instant.now();
    String tableName = createTable(0);

    try (SnowflakeStreamingIngestClient client =
        TestUtils.createStreamingIngestClient(Constants.BdecVersion.THREE)) {
      // Each thread will create its own channel and ingest data
      ConcurrencyTestUtils.doInManyThreads(
          THREAD_COUNT,
          threadId -> {
            long counter = 0;
            final SnowflakeStreamingIngestChannel channel =
                TestUtils.openChannel(client, database, tableName);
            String offset = "";
            while (Duration.between(testStartTime, Instant.now()).compareTo(TEST_DURATION) <= 0) {
              offset = String.format("%d_%d", threadId, counter);
              InsertValidationResponse insertValidationResponse =
                  channel.insertRow(createRow(counter), offset);
              Assert.assertFalse(insertValidationResponse.hasErrors());
              counter++;
            }
            // Each thread will wait for its channel to commit all data
            TestUtils.waitForOffset(channel, offset);
          });

      migrateTable(tableName);
    }
  }

  /**
   * Test data ingestion from multiple threads via multiple channels (from multiple clients) into a
   * single table
   */
  @Test
  public void testIngestViaMultipleChannelsAndClientsSingleTable() throws InterruptedException {
    final Instant testStartTime = Instant.now();
    String tableName = createTable(0);

    // Each thread will create its own client, channel and ingest data
    ConcurrencyTestUtils.doInManyThreads(
        THREAD_COUNT,
        threadId -> {
          try (SnowflakeStreamingIngestClient client =
              TestUtils.createStreamingIngestClient(Constants.BdecVersion.THREE)) {
            long counter = 0;
            final SnowflakeStreamingIngestChannel channel =
                TestUtils.openChannel(client, database, tableName);
            String offset = "";
            while (Duration.between(testStartTime, Instant.now()).compareTo(TEST_DURATION) <= 0) {
              offset = String.format("%d_%d", threadId, counter);
              InsertValidationResponse insertValidationResponse =
                  channel.insertRow(createRow(counter), offset);
              Assert.assertFalse(insertValidationResponse.hasErrors());
              counter++;
            }
            // Each thread will wait for its data to be committed
            TestUtils.waitForOffset(channel, offset);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    migrateTable(tableName);

  }

  /**
   * Test data ingestion from multiple threads via multiple channels (from a single client) into
   * multiple tables
   */
  @Test
  public void testIngestViaMultipleChannelsMultipleTables() throws Exception {
    final Instant testStartTime = Instant.now();

    try (SnowflakeStreamingIngestClient client =
        TestUtils.createStreamingIngestClient(Constants.BdecVersion.THREE)) {
      // Each thread will create its own channel and ingest data
      ConcurrencyTestUtils.doInManyThreads(
          THREAD_COUNT,
          threadId -> {
            String tableName = createTable(threadId);
            long counter = 0;
            final SnowflakeStreamingIngestChannel channel =
                TestUtils.openChannel(client, database, tableName);
            String offset = "";
            while (Duration.between(testStartTime, Instant.now()).compareTo(TEST_DURATION) <= 0) {
              offset = String.format("%d_%d", threadId, counter);
              InsertValidationResponse insertValidationResponse =
                  channel.insertRow(createRow(counter), offset);
              Assert.assertFalse(insertValidationResponse.hasErrors());
              counter++;
            }
            // Each thread will wait for its channel to commit all data
            TestUtils.waitForOffset(channel, offset);

            // Migrate table
            migrateTable(tableName);
          });
    }
  }

  /**
   * Test data ingestion from multiple threads via multiple channels (from multiple clients) into
   * multiple tables
   */
  @Test
  public void testIngestViaMultipleChannelsAndClientsMultipleTables() throws InterruptedException {
    final Instant testStartTime = Instant.now();
    // Each thread will create its own client, channel and ingest data
    ConcurrencyTestUtils.doInManyThreads(
        THREAD_COUNT,
        threadId -> {
          try (SnowflakeStreamingIngestClient client =
              TestUtils.createStreamingIngestClient(Constants.BdecVersion.THREE)) {
            String tableName = createTable(threadId);
            long counter = 0;
            final SnowflakeStreamingIngestChannel channel =
                TestUtils.openChannel(client, database, tableName);
            String offset = "";
            while (Duration.between(testStartTime, Instant.now()).compareTo(TEST_DURATION) <= 0) {
              offset = String.format("%d_%d", threadId, counter);
              InsertValidationResponse insertValidationResponse =
                  channel.insertRow(createRow(counter), offset);
              Assert.assertFalse(insertValidationResponse.hasErrors());
              counter++;
            }
            // Each thread will wait for its data to be committed
            TestUtils.waitForOffset(channel, offset);

            migrateTable(tableName);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  private void migrateTable(String tableName) {
    try {
      conn.createStatement().execute(String.format("alter table %s migrate;", tableName));
    } catch (SQLException e) {
      throw new RuntimeException(String.format("Cannot migrate table %s", tableName), e);
    }
  }

  private String createTable(int id) {
    final String tableName = String.format("table_%d", id);
    try {
      conn.createStatement()
          .execute(
              String.format(
                  "create or replace table %s.PUBLIC.%s(a varchar, b varchar, c varchar)",
                  database, tableName));
    } catch (SQLException e) {
      throw new RuntimeException("Exception while creating table occurred", e);
    }
    return tableName;
  }
  /**
   * Returns a random row. One column is always not null, another is randomly null and the last one
   * is always null.
   */
  private Map<String, Object> createRow(long i) {
    String value = String.valueOf(i);
    Map<String, Object> row = new HashMap<>();
    row.put("a", value);
    row.put("b", RANDOM.nextInt(100) < 30 ? value : null);
    row.put("c", null);
    return row;
  }
}

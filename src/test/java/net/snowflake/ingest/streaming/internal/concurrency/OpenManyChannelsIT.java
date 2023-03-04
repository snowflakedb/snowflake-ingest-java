package net.snowflake.ingest.streaming.internal.concurrency;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.SFException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Tries to open several thousand channels into the same table from multiple threads in parallel */
public class OpenManyChannelsIT {
  private static final int THREAD_COUNT = 20;
  private static final int CHANNELS_PER_THREAD = 250;
  private static final String TABLE_NAME = "T1";

  private String databaseName;

  private Connection conn;

  private SnowflakeStreamingIngestClient client;

  @Before
  public void setUp() throws Exception {
    conn = TestUtils.getConnection(true);
    databaseName = TestUtils.createDatabase(conn);
    conn.createStatement()
        .execute(
            String.format(
                "create or replace table %s.PUBLIC.%s (col int)", databaseName, TABLE_NAME));
    client = TestUtils.createStreamingIngestClient(Constants.BdecVersion.THREE);
  }

  /**
   * Reopens the same channel from multiple threads, asserts that each channel has a unique client
   * sequencer. At the end it verifies that only the channel with the highest client sequencer can
   * ingest data
   */
  @Test
  public void reopenSameChannel() throws Exception {
    ConcurrentHashMap<Integer, SnowflakeStreamingIngestChannel> clientSequencerToChannelMap =
        new ConcurrentHashMap<>();

    ConcurrencyTestUtils.doInManyThreads(
        THREAD_COUNT,
        threadId -> {
          for (int j = 0; j < CHANNELS_PER_THREAD; j++) {
            SnowflakeStreamingIngestChannel channel =
                TestUtils.openChannel(client, databaseName, TABLE_NAME);
            clientSequencerToChannelMap.put((int) getClientSequencer(channel), channel);
          }
        });

    // Verify that each reopened channel received its own sequencer
    Assert.assertEquals(THREAD_COUNT * CHANNELS_PER_THREAD, clientSequencerToChannelMap.size());

    // Verify that ingestion into to the channel with the highest client sequencer works
    int highestSequencer =
        clientSequencerToChannelMap.keySet().stream().max(Comparator.naturalOrder()).get();
    Assert.assertEquals(THREAD_COUNT * CHANNELS_PER_THREAD - 1, highestSequencer);
    String offsetToken = UUID.randomUUID().toString();
    SnowflakeStreamingIngestChannel lastOpenedChannel =
        clientSequencerToChannelMap.get(highestSequencer);
    lastOpenedChannel.insertRow(new HashMap<>(), offsetToken);
    TestUtils.waitForOffset(lastOpenedChannel, offsetToken);

    // Verify that ingestion into all other channels does not work
    for (Map.Entry<Integer, SnowflakeStreamingIngestChannel> entry :
        clientSequencerToChannelMap.entrySet()) {
      if (entry.getKey() != highestSequencer) {
        try {
          entry.getValue().insertRow(new HashMap<>(), UUID.randomUUID().toString());
          Assert.fail("Ingestion into channel with non-latest client sequencer should fail");
        } catch (SFException e) {
          // all good, expected exception has been thrown
        }
      }
    }
  }

  /** Opens many channels in parallel, checks that each has client sequencer 0 */
  @Test
  public void testOpenManyDifferentChannels() throws Exception {
    ConcurrencyTestUtils.doInManyThreads(
        THREAD_COUNT,
        threadId -> {
          for (int j = 0; j < CHANNELS_PER_THREAD; j++) {
            SnowflakeStreamingIngestChannel channel =
                TestUtils.openChannel(client, databaseName, TABLE_NAME);
            Assert.assertEquals(0L, getClientSequencer(channel));
          }
        });
  }

  /**
   * Implementation of SnowflakeStreamingIngestChannel is package-private, we need to get the client
   * sequencer using reflection.
   */
  private long getClientSequencer(SnowflakeStreamingIngestChannel channel) {
    try {
      Field channelFlushContextField = channel.getClass().getDeclaredField("channelFlushContext");
      channelFlushContextField.setAccessible(true);
      Object flushContext = channelFlushContextField.get(channel);
      Field clientSequencerField = flushContext.getClass().getDeclaredField("channelSequencer");
      clientSequencerField.setAccessible(true);
      return (long) clientSequencerField.get(flushContext);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException("Reflection exception", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    conn.createStatement().execute(String.format("drop database %s;", databaseName));
    client.close();
    conn.close();
  }
}

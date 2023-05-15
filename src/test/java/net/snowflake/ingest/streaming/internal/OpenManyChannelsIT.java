package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.ROLE;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.SFException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Tries to open several thousand channels into the same table from multiple threads in parallel */
@Ignore("Will be reimplemented in dew: SNOW-807102")
public class OpenManyChannelsIT {
  private static final int THREAD_COUNT = 20;
  private static final int CHANNELS_PER_THREAD = 250;
  private static final String SCHEMA_NAME = "PUBLIC";
  private static final String TABLE_NAME = "T1";

  private String databaseName;

  private Connection conn;

  private SnowflakeStreamingIngestClient client;

  @Before
  public void setUp() throws Exception {
    databaseName =
        String.format("SDK_DATATYPE_COMPATIBILITY_IT_%s", RandomStringUtils.randomNumeric(9));
    conn = TestUtils.getConnection(true);
    conn.createStatement().execute(String.format("create or replace database %s;", databaseName));
    conn.createStatement()
        .execute(
            String.format(
                "create or replace table %s.%s.%s (col int)",
                databaseName, SCHEMA_NAME, TABLE_NAME));
    Properties props = TestUtils.getProperties(Constants.BdecVersion.ONE);
    if (props.getProperty(ROLE).equals("DEFAULT_ROLE")) {
      props.setProperty(ROLE, "ACCOUNTADMIN");
    }
    client = SnowflakeStreamingIngestClientFactory.builder("client1").setProperties(props).build();
  }

  /**
   * Reopens the same channel from multiple threads, asserts each channel has a unique client
   * sequencer. At the end it verifies that only the channel with highest client sequencer can
   * ingest data
   */
  @Test
  public void reopenSameChannel() throws Exception {
    String channelName = "CHANNEL";
    List<Thread> threads = new ArrayList<>();
    List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
    ConcurrentHashMap<Integer, SnowflakeStreamingIngestChannel> clientSequencerToChannelMap =
        new ConcurrentHashMap<>();

    for (int i = 0; i < THREAD_COUNT; i++) {
      Thread t =
          new Thread(
              () -> {
                for (int j = 0; j < CHANNELS_PER_THREAD; j++) {
                  OpenChannelRequest openChannelRequest =
                      OpenChannelRequest.builder(channelName)
                          .setDBName(databaseName)
                          .setSchemaName(SCHEMA_NAME)
                          .setTableName(TABLE_NAME)
                          .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
                          .build();
                  try {
                    SnowflakeStreamingIngestChannel channel =
                        client.openChannel(openChannelRequest);
                    Long channelSequencer =
                        ((SnowflakeStreamingIngestChannelInternal<Void>) channel)
                            .getChannelSequencer();
                    clientSequencerToChannelMap.put(channelSequencer.intValue(), channel);
                  } catch (Exception e) {
                    exceptions.add(e);
                    break;
                  }
                }
              });
      t.start();
      threads.add(t);
    }

    for (Thread t : threads) {
      t.join();
    }

    if (!exceptions.isEmpty()) {
      for (Exception e : exceptions) {
        e.printStackTrace();
      }
      Assert.fail(String.format("Exceptions thrown: %d", exceptions.size()));
    }

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
    List<Thread> threads = new ArrayList<>();
    List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
    for (int i = 0; i < THREAD_COUNT; i++) {
      final int threadId = i;
      Thread t =
          new Thread(
              () -> {
                for (int j = 0; j < CHANNELS_PER_THREAD; j++) {
                  OpenChannelRequest openChannelRequest =
                      OpenChannelRequest.builder(String.format("CHANNEL-%s-%s", threadId, j))
                          .setDBName(databaseName)
                          .setSchemaName(SCHEMA_NAME)
                          .setTableName(TABLE_NAME)
                          .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
                          .build();
                  try {
                    SnowflakeStreamingIngestChannel channel =
                        client.openChannel(openChannelRequest);
                    Long channelSequencer =
                        ((SnowflakeStreamingIngestChannelInternal<Void>) channel)
                            .getChannelSequencer();
                    Assert.assertEquals(0L, channelSequencer.longValue());
                  } catch (Exception e) {
                    exceptions.add(e);
                    break;
                  }
                }
              });
      t.start();
      threads.add(t);
    }

    for (Thread t : threads) {
      t.join();
    }

    if (!exceptions.isEmpty()) {
      for (Exception e : exceptions) {
        e.printStackTrace();
      }
      Assert.fail(String.format("Exceptions thrown: %d", exceptions.size()));
    }
  }

  @After
  public void tearDown() throws Exception {
    conn.createStatement().execute(String.format("drop database %s;", databaseName));
    client.close();
    conn.close();
  }
}

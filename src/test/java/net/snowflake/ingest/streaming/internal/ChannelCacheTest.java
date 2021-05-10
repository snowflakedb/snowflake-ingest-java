package net.snowflake.ingest.streaming.internal;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ChannelCacheTest {
  ChannelCache cache;
  SnowflakeStreamingIngestChannelV1 channel1;
  SnowflakeStreamingIngestChannelV1 channel2;
  SnowflakeStreamingIngestChannelV1 channel3;
  String dbName = "db";
  String schemaName = "schema";
  String table1Name = "table1";
  String table2Name = "table2";

  @Before
  public void setup() {
    cache = new ChannelCache();
    channel1 =
        new SnowflakeStreamingIngestChannelV1(
            "channel1", dbName, schemaName, table1Name, "0", 0L, 0L, null, true);
    channel2 =
        new SnowflakeStreamingIngestChannelV1(
            "channel2", dbName, schemaName, table1Name, "0", 0L, 0L, null, true);
    channel3 =
        new SnowflakeStreamingIngestChannelV1(
            "channel3", dbName, schemaName, table2Name, "0", 0L, 0L, null, true);
    cache.addChannel(channel1);
    cache.addChannel(channel2);
    cache.addChannel(channel3);
  }

  @Test
  public void testAddChannel() {
    String channelName = "channel";
    String tableName = "table";

    ChannelCache cache = new ChannelCache();
    Assert.assertEquals(0, cache.getSize());
    SnowflakeStreamingIngestChannelV1 channel =
        new SnowflakeStreamingIngestChannelV1(
            channelName, dbName, schemaName, tableName, "0", 0L, 0L, null, true);
    cache.addChannel(channel);
    Assert.assertEquals(1, cache.getSize());
    Assert.assertTrue(channel == cache.iterator().next().getValue().get(channelName));

    SnowflakeStreamingIngestChannelV1 channelDup =
        new SnowflakeStreamingIngestChannelV1(
            channelName, dbName, schemaName, tableName, "0", 1L, 0L, null, true);
    cache.addChannel(channelDup);
    // The old channel should be invalid now
    Assert.assertTrue(!channel.isValid());
    Assert.assertTrue(channelDup.isValid());
    Assert.assertEquals(1, cache.getSize());
    ConcurrentHashMap<String, SnowflakeStreamingIngestChannelV1> channels =
        cache.iterator().next().getValue();
    Assert.assertEquals(1, channels.size());
    Assert.assertTrue(channelDup == channels.get(channelName));
    Assert.assertFalse(channel == channelDup);
  }

  @Test
  public void testIterator() {
    Assert.assertEquals(2, cache.getSize());
    Iterator<Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelV1>>> iter =
        cache.iterator();
    Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelV1>> firstTable =
        iter.next();
    Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelV1>> secondTable =
        iter.next();
    Assert.assertFalse(iter.hasNext());
    if (firstTable.getKey().equals(channel1.getFullyQualifiedTableName())) {
      Assert.assertEquals(2, firstTable.getValue().size());
      Assert.assertEquals(channel3.getFullyQualifiedTableName(), secondTable.getKey());
      Assert.assertEquals(1, secondTable.getValue().size());
    } else if (secondTable.getKey().equals(channel1.getFullyQualifiedTableName())) {
      Assert.assertEquals(2, secondTable.getValue().size());
      Assert.assertEquals(channel3.getFullyQualifiedTableName(), firstTable.getKey());
      Assert.assertEquals(1, firstTable.getValue().size());
    } else {
      Assert.fail("Unknown table in cache");
    }
  }

  @Test
  public void testCloseAllChannels() {
    cache.closeAllChannels();
    Iterator<Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelV1>>> iter =
        cache.iterator();
    while (iter.hasNext()) {
      for (SnowflakeStreamingIngestChannelV1 channel : iter.next().getValue().values()) {
        Assert.assertTrue(channel.isClosed());
      }
    }
  }

  @Test
  public void testRemoveChannel() {
    cache.removeChannelIfSequencersMatch(channel1);
    Assert.assertEquals(2, cache.getSize());
    // Remove channel1 again should be a no op
    cache.removeChannelIfSequencersMatch(channel1);
    Assert.assertEquals(2, cache.getSize());
    cache.removeChannelIfSequencersMatch(channel2);
    Assert.assertEquals(1, cache.getSize());

    SnowflakeStreamingIngestChannelV1 channel3Dup =
        new SnowflakeStreamingIngestChannelV1(
            "channel3", dbName, schemaName, table1Name, "0", 1L, 0L, null, true);
    cache.removeChannelIfSequencersMatch(channel3Dup);
    // Verify that remove the same channel with a different channel sequencer is a no op
    Assert.assertEquals(1, cache.getSize());
    cache.removeChannelIfSequencersMatch(channel3);
    Assert.assertEquals(0, cache.getSize());
    // Remove channel2 again should be a no op
    cache.removeChannelIfSequencersMatch(channel2);
    Assert.assertEquals(0, cache.getSize());
  }
}

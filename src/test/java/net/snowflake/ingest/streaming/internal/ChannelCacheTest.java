package net.snowflake.ingest.streaming.internal;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ChannelCacheTest {
  ChannelCache cache;
  SnowflakeStreamingIngestClientInternal client;
  SnowflakeStreamingIngestChannelInternal channel1;
  SnowflakeStreamingIngestChannelInternal channel2;
  SnowflakeStreamingIngestChannelInternal channel3;
  String dbName = "db";
  String schemaName = "schema";
  String table1Name = "table1";
  String table2Name = "table2";

  @Before
  public void setup() {
    cache = new ChannelCache();
    client = new SnowflakeStreamingIngestClientInternal("client");
    channel1 =
        new SnowflakeStreamingIngestChannelInternal(
            "channel1", dbName, schemaName, table1Name, "0", 0L, 0L, client, true);
    channel2 =
        new SnowflakeStreamingIngestChannelInternal(
            "channel2", dbName, schemaName, table1Name, "0", 0L, 0L, client, true);
    channel3 =
        new SnowflakeStreamingIngestChannelInternal(
            "channel3", dbName, schemaName, table2Name, "0", 0L, 0L, client, true);
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
    SnowflakeStreamingIngestChannelInternal channel =
        new SnowflakeStreamingIngestChannelInternal(
            channelName, dbName, schemaName, tableName, "0", 0L, 0L, client, true);
    cache.addChannel(channel);
    Assert.assertEquals(1, cache.getSize());
    Assert.assertTrue(channel == cache.iterator().next().getValue().get(channelName));

    SnowflakeStreamingIngestChannelInternal channelDup =
        new SnowflakeStreamingIngestChannelInternal(
            channelName, dbName, schemaName, tableName, "0", 1L, 0L, client, true);
    cache.addChannel(channelDup);
    // The old channel should be invalid now
    Assert.assertTrue(!channel.isValid());
    Assert.assertTrue(channelDup.isValid());
    Assert.assertEquals(1, cache.getSize());
    ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal> channels =
        cache.iterator().next().getValue();
    Assert.assertEquals(1, channels.size());
    Assert.assertTrue(channelDup == channels.get(channelName));
    Assert.assertFalse(channel == channelDup);
  }

  @Test
  public void testIterator() {
    Assert.assertEquals(2, cache.getSize());
    Iterator<Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal>>>
        iter = cache.iterator();
    Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal>>
        firstTable = iter.next();
    Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal>>
        secondTable = iter.next();
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
    Iterator<Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal>>>
        iter = cache.iterator();
    while (iter.hasNext()) {
      for (SnowflakeStreamingIngestChannelInternal channel : iter.next().getValue().values()) {
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

    SnowflakeStreamingIngestChannelInternal channel3Dup =
        new SnowflakeStreamingIngestChannelInternal(
            "channel3", dbName, schemaName, table1Name, "0", 1L, 0L, client, true);
    cache.removeChannelIfSequencersMatch(channel3Dup);
    // Verify that remove the same channel with a different channel sequencer is a no op
    Assert.assertEquals(1, cache.getSize());
    cache.removeChannelIfSequencersMatch(channel3);
    Assert.assertEquals(0, cache.getSize());
    // Remove channel2 again should be a no op
    cache.removeChannelIfSequencersMatch(channel2);
    Assert.assertEquals(0, cache.getSize());
  }

  @Test
  public void testInvalidateChannel() {
    Assert.assertTrue(channel1.isValid());
    Assert.assertTrue(channel2.isValid());
    Assert.assertTrue(channel3.isValid());

    cache.invalidateChannelIfSequencersMatch(
        channel1.getDBName(),
        channel1.getSchemaName(),
        channel1.getTableName(),
        channel1.getName(),
        channel1.getChannelSequencer());
    Assert.assertFalse(channel1.isValid());

    cache.invalidateChannelIfSequencersMatch(
        channel2.getDBName(),
        channel2.getSchemaName(),
        channel2.getTableName(),
        channel2.getName(),
        channel2.getChannelSequencer());
    Assert.assertFalse(channel2.isValid());

    cache.invalidateChannelIfSequencersMatch(
        channel3.getDBName(),
        channel3.getSchemaName(),
        channel3.getTableName(),
        channel3.getName(),
        channel3.getChannelSequencer());
    Assert.assertFalse(channel3.isValid());
  }
}

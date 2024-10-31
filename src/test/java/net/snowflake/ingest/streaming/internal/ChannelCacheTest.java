/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static java.time.ZoneOffset.UTC;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.ParameterProvider;
import org.apache.parquet.column.ParquetProperties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ChannelCacheTest {

  @Parameterized.Parameters(name = "enableIcebergStreaming: {0}")
  public static Object[] enableIcebergStreaming() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public static boolean enableIcebergStreaming;

  ChannelCache<StubChunkData> cache;
  SnowflakeStreamingIngestClientInternal<StubChunkData> client;
  SnowflakeStreamingIngestChannelInternal<StubChunkData> channel1;
  SnowflakeStreamingIngestChannelInternal<StubChunkData> channel2;
  SnowflakeStreamingIngestChannelInternal<StubChunkData> channel3;
  String dbName = "db";
  String schemaName = "schema";
  String table1Name = "table1";
  String table2Name = "table2";

  @Before
  public void setup() {
    cache = new ChannelCache<>();
    CloseableHttpClient httpClient = MockSnowflakeServiceClient.createHttpClient();
    RequestBuilder requestBuilder =
        MockSnowflakeServiceClient.createRequestBuilder(httpClient, enableIcebergStreaming);
    Properties prop = new Properties();
    prop.setProperty(
        ParameterProvider.ENABLE_ICEBERG_STREAMING, String.valueOf(enableIcebergStreaming));
    client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client", null, prop, httpClient, true, requestBuilder, new HashMap<>());

    channel1 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel1",
            dbName,
            schemaName,
            table1Name,
            "0",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            UTC,
            null /* offsetTokenVerificationFunction */,
            enableIcebergStreaming
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0);
    channel2 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel2",
            dbName,
            schemaName,
            table1Name,
            "0",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            UTC,
            null /* offsetTokenVerificationFunction */,
            enableIcebergStreaming
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0);
    channel3 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel3",
            dbName,
            schemaName,
            table2Name,
            "0",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            UTC,
            null /* offsetTokenVerificationFunction */,
            enableIcebergStreaming
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0);
    cache.addChannel(channel1);
    cache.addChannel(channel2);
    cache.addChannel(channel3);
  }

  @Test
  public void testAddChannel() {
    String channelName = "channel";
    String tableName = "table";

    ChannelCache<StubChunkData> cache = new ChannelCache<>();
    Assert.assertEquals(0, cache.getSize());
    SnowflakeStreamingIngestChannelInternal<StubChunkData> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            channelName,
            dbName,
            schemaName,
            tableName,
            "0",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            UTC,
            null /* offsetTokenVerificationFunction */,
            enableIcebergStreaming
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0);
    cache.addChannel(channel);
    Assert.assertEquals(1, cache.getSize());
    Assert.assertTrue(channel == cache.entrySet().iterator().next().getValue().get(channelName));

    SnowflakeStreamingIngestChannelInternal<StubChunkData> channelDup =
        new SnowflakeStreamingIngestChannelInternal<>(
            channelName,
            dbName,
            schemaName,
            tableName,
            "0",
            1L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            UTC,
            null /* offsetTokenVerificationFunction */,
            enableIcebergStreaming
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0);
    cache.addChannel(channelDup);
    // The old channel should be invalid now
    Assert.assertTrue(!channel.isValid());
    Assert.assertTrue(channelDup.isValid());
    Assert.assertEquals(1, cache.getSize());
    ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<StubChunkData>> channels =
        cache.entrySet().iterator().next().getValue();
    Assert.assertEquals(1, channels.size());
    Assert.assertTrue(channelDup == channels.get(channelName));
    Assert.assertFalse(channel == channelDup);
  }

  @Test
  public void testIterator() {
    Assert.assertEquals(2, cache.getSize());
    Iterator<
            Map.Entry<
                String,
                ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<StubChunkData>>>>
        iter = cache.entrySet().iterator();
    Map.Entry<
            String,
            ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<StubChunkData>>>
        firstTable = iter.next();
    Map.Entry<
            String,
            ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<StubChunkData>>>
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
    Iterator<
            Map.Entry<
                String,
                ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<StubChunkData>>>>
        iter = cache.entrySet().iterator();
    while (iter.hasNext()) {
      for (SnowflakeStreamingIngestChannelInternal<?> channel : iter.next().getValue().values()) {
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

    SnowflakeStreamingIngestChannelInternal<StubChunkData> channel3Dup =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel3",
            dbName,
            schemaName,
            table1Name,
            "0",
            1L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            UTC,
            null /* offsetTokenVerificationFunction */,
            enableIcebergStreaming
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0);
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
        channel1.getChannelSequencer(),
        "Invalidated by test");
    Assert.assertFalse(channel1.isValid());

    cache.invalidateChannelIfSequencersMatch(
        channel2.getDBName(),
        channel2.getSchemaName(),
        channel2.getTableName(),
        channel2.getName(),
        channel2.getChannelSequencer(),
        "Invalidated by test");
    Assert.assertFalse(channel2.isValid());

    cache.invalidateChannelIfSequencersMatch(
        channel3.getDBName(),
        channel3.getSchemaName(),
        channel3.getTableName(),
        channel3.getName(),
        channel3.getChannelSequencer(),
        "Invalidated by test");
    Assert.assertFalse(channel3.isValid());
  }
}

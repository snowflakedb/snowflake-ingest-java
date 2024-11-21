/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import static net.snowflake.ingest.utils.Constants.REGISTER_BLOB_ENDPOINT;
import static net.snowflake.ingest.utils.ParameterProvider.ENABLE_ICEBERG_STREAMING;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_CLIENT_LAG;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.snowflake.ingest.IcebergIT;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.streaming.internal.datatypes.IcebergNumericTypesIT;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IcebergIT.class)
@RunWith(Parameterized.class)
public class IcebergBigFilesIT {
  private static final Logger logger = LoggerFactory.getLogger(IcebergNumericTypesIT.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Parameterized.Parameters(name = "icebergSerializationPolicy={0}")
  public static Object[] parameters() {
    return new Object[] {
      Constants.IcebergSerializationPolicy.COMPATIBLE,
      Constants.IcebergSerializationPolicy.OPTIMIZED
    };
  }

  @Parameterized.Parameter
  public static Constants.IcebergSerializationPolicy icebergSerializationPolicy;

  private String database;
  private String schema;
  private Connection conn;
  private SnowflakeStreamingIngestClientInternal<?> client;
  private RequestBuilder requestBuilder;
  private Random generator;
  private RandomStringGenerator randomStringGenerator;

  @Before
  public void before() throws Exception {
    database = String.format("SDK_ICEBERG_BIG_FILES_IT_DB_%d", System.nanoTime());
    schema = "PUBLIC";

    conn = TestUtils.getConnection(true);

    conn.createStatement().execute(String.format("create or replace database %s;", database));
    conn.createStatement().execute(String.format("use database %s;", database));
    conn.createStatement().execute(String.format("use schema %s;", schema));
    conn.createStatement().execute(String.format("use warehouse %s;", TestUtils.getWarehouse()));

    SnowflakeURL url = new SnowflakeURL(TestUtils.getAccountURL());
    Properties properties = TestUtils.getProperties(Constants.BdecVersion.THREE, false);
    properties.setProperty(ENABLE_ICEBERG_STREAMING, "true");
    properties.setProperty(MAX_CLIENT_LAG, "30000");
    requestBuilder =
        Mockito.spy(
            new RequestBuilder(
                url,
                TestUtils.getUser(),
                TestUtils.getKeyPair(),
                HttpUtil.getHttpClient(url.getAccount()),
                true /* enableIcebergStreaming */,
                "client1"));
    this.client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client1",
            url,
            Utils.createProperties(properties),
            HttpUtil.getHttpClient(url.getAccount()),
            false /* isTestMode */,
            requestBuilder,
            null /* parameterOverrides */);

    long seed = System.currentTimeMillis();
    logger.info("Random seed: {}", seed);
    generator = new Random(seed);
    randomStringGenerator =
        new RandomStringGenerator.Builder()
            .usingRandom(generator::nextInt)
            .withinRange(0, 255)
            .build();
  }

  @After
  public void after() throws Exception {
    conn.createStatement().execute(String.format("drop database if exists %s;", database));
  }

  @Test
  public void testMultiplePartUpload() throws Exception {
    int rows = 1024 * 1024;
    int channelCount = 4;
    int rowsPerChannel = rows / channelCount;
    int batchSize = 1024;

    AtomicInteger expectedCount = new AtomicInteger();

    String tableName = "test_multiple_part_upload_table";
    conn.createStatement()
        .execute(
            String.format(
                "create or replace iceberg table %s(string_col string)"
                    + "catalog = 'SNOWFLAKE' "
                    + "external_volume = 'streaming_ingest' "
                    + "base_location = 'SDK_IT/%s/%s'"
                    + "storage_serialization_policy = %s;",
                tableName, database, tableName, icebergSerializationPolicy.name()));

    ExecutorService testThreadPool = Executors.newFixedThreadPool(channelCount);
    CompletableFuture[] futures = new CompletableFuture[channelCount];
    List<SnowflakeStreamingIngestChannel> channelList =
        Collections.synchronizedList(new ArrayList<>());
    for (int i = 0; i < channelCount; i++) {
      final String channelName = "CHANNEL" + i;
      final int first = i * rowsPerChannel;
      futures[i] =
          CompletableFuture.runAsync(
              () -> {
                SnowflakeStreamingIngestChannel channel =
                    client.openChannel(
                        OpenChannelRequest.builder(channelName)
                            .setDBName(database)
                            .setSchemaName(schema)
                            .setTableName(tableName)
                            .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
                            .build());
                channelList.add(channel);
                for (int val = first; val < first + rowsPerChannel; val += batchSize) {
                  List<Map<String, Object>> values = new ArrayList<>(batchSize);
                  for (int j = 0; j < batchSize; j++) {
                    String randomString = randomStringGenerator.generate(1, 512);
                    if (randomString.length() > 256) {
                      expectedCount.getAndIncrement();
                    }
                    values.add(ImmutableMap.of("string_col", randomString));
                  }
                  TestUtils.verifyInsertValidationResponse(
                      channel.insertRows(
                          values,
                          Integer.toString(
                              val == first + rowsPerChannel - batchSize ? rows : val)));
                }
              },
              testThreadPool);
    }
    CompletableFuture joined = CompletableFuture.allOf(futures);
    joined.get();
    for (SnowflakeStreamingIngestChannel channel : channelList) {
      TestUtils.waitForOffset(channel, Integer.toString(rows), 600);
    }
    testThreadPool.shutdown();

    ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(requestBuilder, Mockito.atLeastOnce())
        .generateStreamingIngestPostRequest(
            payloadCaptor.capture(),
            Mockito.eq(REGISTER_BLOB_ENDPOINT),
            Mockito.eq("register blob"));

    /* Check if any chunk is larger than 128MB */
    Pattern pattern = Pattern.compile("\"chunk_length\":(\\d+)");
    assertThat(payloadCaptor.getAllValues())
        .anyMatch(
            payload -> {
              Matcher matcher = pattern.matcher(payload);
              if (matcher.find()) {
                return Integer.parseInt(matcher.group(1)) > 128 * 1024 * 1024;
              }
              return false;
            });

    ResultSet rs =
        conn.createStatement()
            .executeQuery(
                String.format("select count(*) from %s where length(string_col) > 256", tableName));
    assertThat(rs.next()).isTrue();
    assertThat(rs.getLong(1)).isEqualTo(expectedCount.get());
    assertThat(rs.next()).isFalse();

    ResultSet rs2 =
        conn.createStatement().executeQuery(String.format("select hash_agg(*) from %s", tableName));
    assertThat(rs2.next()).isTrue();
    assertThat(rs2.getLong(1)).isNotNull();
    assertThat(rs2.next()).isFalse();
  }
}

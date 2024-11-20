/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import static net.snowflake.ingest.utils.Constants.REFRESH_TABLE_INFORMATION_ENDPOINT;
import static net.snowflake.ingest.utils.ParameterProvider.ENABLE_ICEBERG_STREAMING;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_CLIENT_LAG;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.util.Properties;
import net.snowflake.ingest.IcebergIT;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(IcebergIT.class)
public class SubscopedTokenRefreshIT {

  private String database;
  private String schema;
  private Connection conn;
  private SnowflakeStreamingIngestClientInternal<?> client;
  private RequestBuilder requestBuilder;

  @Before
  public void before() throws Exception {
    database = String.format("SDK_TOKEN_EXPIRE_IT_DB_%d", System.nanoTime());
    schema = "PUBLIC";

    conn = TestUtils.getConnection(true);

    conn.createStatement().execute(String.format("create or replace database %s;", database));
    conn.createStatement().execute(String.format("use database %s;", database));
    conn.createStatement().execute(String.format("use schema %s;", schema));
    conn.createStatement().execute(String.format("use warehouse %s;", TestUtils.getWarehouse()));

    SnowflakeURL url = new SnowflakeURL(TestUtils.getAccountURL());
    Properties properties = TestUtils.getProperties(Constants.BdecVersion.THREE, false);
    properties.setProperty(ENABLE_ICEBERG_STREAMING, "true");
    properties.setProperty(MAX_CLIENT_LAG, "1000");
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
  }

  @After
  public void after() throws Exception {
    conn.createStatement().execute(String.format("drop database if exists %s;", database));
  }

  @Test
  public void testTokenExpire() throws Exception {
    String tableName = "test_token_expire_table";

    /*
     * Minimum duration of token for each cloud storage:
     *  - S3: 900 seconds
     *  - GCS: 600 seconds
     *  - Azure: 300 seconds
     */
    int duration = 900;
    createIcebergTable(tableName);
    conn.createStatement()
        .execute(
            String.format(
                "alter iceberg table %s set"
                    + " STREAMING_ICEBERG_INGESTION_SUBSCOPED_TOKEN_DURATION_SECONDS_S3=%s",
                tableName, duration));
    conn.createStatement()
        .execute(
            String.format(
                "alter iceberg table %s set"
                    + " STREAMING_ICEBERG_INGESTION_SUBSCOPED_TOKEN_DURATION_SECONDS_GCS=%s",
                tableName, duration));
    conn.createStatement()
        .execute(
            String.format(
                "alter iceberg table %s set"
                    + " STREAMING_ICEBERG_INGESTION_SUBSCOPED_TOKEN_DURATION_SECONDS_AZURE=%s",
                tableName, duration));
    conn.createStatement()
        .execute(
            String.format(
                "alter iceberg table %s set"
                    + " STREAMING_ICEBERG_INGESTION_SUBSCOPED_TOKEN_DURATION_SECONDS_DEFAULT=%s",
                tableName, duration));

    SnowflakeStreamingIngestChannel channel =
        client.openChannel(
            OpenChannelRequest.builder("CHANNEL")
                .setDBName(database)
                .setSchemaName(schema)
                .setTableName(tableName)
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
                .build());

    /* Refresh table information should be called once channel is opened */
    Mockito.verify(requestBuilder, Mockito.times(1))
        .generateStreamingIngestPostRequest(
            Mockito.anyString(),
            Mockito.eq(REFRESH_TABLE_INFORMATION_ENDPOINT),
            Mockito.eq("refresh table information"));

    channel.insertRow(ImmutableMap.of("int_col", 1), "1");
    TestUtils.waitForOffset(channel, "1");

    /* Wait for token to expire */

    Thread.sleep((duration + 1) * 1000);

    /* Insert a row to trigger token generation */
    channel.insertRow(ImmutableMap.of("int_col", 2), "2");
    TestUtils.waitForOffset(channel, "2");
    Mockito.verify(requestBuilder, Mockito.times(2))
        .generateStreamingIngestPostRequest(
            Mockito.anyString(),
            Mockito.eq(REFRESH_TABLE_INFORMATION_ENDPOINT),
            Mockito.eq("refresh table information"));
  }

  private void createIcebergTable(String tableName) throws Exception {
    conn.createStatement()
        .execute(
            String.format(
                "create or replace iceberg table %s(int_col int)"
                    + "catalog = 'SNOWFLAKE' "
                    + "external_volume = 'streaming_ingest' "
                    + "base_location = 'SDK_IT/%s/%s'",
                tableName, database, tableName));
  }
}

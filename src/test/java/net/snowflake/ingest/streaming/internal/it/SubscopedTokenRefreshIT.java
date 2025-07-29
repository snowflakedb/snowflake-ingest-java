/*
 * Copyright (c) 2024-2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import static net.snowflake.ingest.utils.Constants.REFRESH_TABLE_INFORMATION_ENDPOINT;
import static net.snowflake.ingest.utils.ParameterProvider.ENABLE_ICEBERG_STREAMING;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_CLIENT_LAG;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import net.snowflake.ingest.IcebergIT;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.internal.FileLocationInfo;
import net.snowflake.ingest.streaming.internal.IStorageManager;
import net.snowflake.ingest.streaming.internal.InternalStage;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.streaming.internal.TableRef;
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
    String tableName = "TEST_TOKEN_EXPIRE_TABLE";
    int rowCount = 10;

    createIcebergTable(tableName);

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

    /* Invalidate the token */
    ((InternalStage)
            client
                .getStorageManager()
                .getStorage(Utils.getFullyQualifiedTableName(database, schema, tableName)))
        .setEmptyIcebergFileTransferMetadataWithAge();

    /* Insert rows to trigger token generation */
    for (int i = 2; i <= rowCount; i++) {
      channel.insertRow(ImmutableMap.of("int_col", i), String.valueOf(i));
    }
    TestUtils.waitForOffset(channel, String.valueOf(rowCount));
    Mockito.verify(requestBuilder, Mockito.times(2))
        .generateStreamingIngestPostRequest(
            Mockito.anyString(),
            Mockito.eq(REFRESH_TABLE_INFORMATION_ENDPOINT),
            Mockito.eq("refresh table information"));

    /* Verify data is inserted */
    ResultSet result =
        conn.createStatement()
            .executeQuery(String.format("select * from %s order by int_col;", tableName));
    for (int i = 1; i <= rowCount; i++) {
      assertThat(result.next()).isTrue();
      assertThat(result.getInt(1)).isEqualTo(i);
    }
  }

  @Test
  public void testMetadataRefreshAfterBlobPathGeneration() throws Exception {
    String tableName = "TEST_METADATA_REFRESH_AFTER_BLOB_PATH_GENERATION";

    createIcebergTable(tableName);

    SnowflakeStreamingIngestChannel channel =
        client.openChannel(
            OpenChannelRequest.builder("CHANNEL")
                .setDBName(database)
                .setSchemaName(schema)
                .setTableName(tableName)
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
                .build());

    IStorageManager storageManager = client.getStorageManager();

    InternalStage stage =
        (InternalStage)
            storageManager.getStorage(
                Utils.getFullyQualifiedTableName(database, schema, tableName));

    /*
     * Create a race condition test with two threads:
     * Thread 1: Continuously sets new file location info - simulates concurrent metadata refresh
     * Thread 2: Performs insertions that trigger putRemote calls
     * This tests the race condition where metadata is refreshed by another thread
     * after blob path is generated and passed to putRemote. The possibility of no refresh happens is
     * 1 / numFileLocationInfos^rowCount which is 1 / 5^10 ~ 1e-7.
     */
    final int rowCount = 10;
    final int numFileLocationInfos = 5;
    final AtomicBoolean testRunning = new AtomicBoolean(true);
    final AtomicReference<Exception> threadException = new AtomicReference<>();

    /* Pre-fetch FileLocationInfo objects using getRefreshedLocation logic */
    final List<FileLocationInfo> fileLocationInfos = new ArrayList<>();
    final TableRef tableRef = new TableRef(database, schema, tableName);
    for (int i = 0; i < numFileLocationInfos; i++) {
      try {
        fileLocationInfos.add(storageManager.getRefreshedLocation(tableRef, Optional.empty()));
      } catch (Exception e) {
        throw new RuntimeException("Failed to pre-fetch FileLocationInfo objects", e);
      }
    }

    /* Thread 1: Cycle through the valid FileLocationInfo objects using setFileLocationInfo to mimic concurrent metadata refresh */
    Thread refreshThread =
        new Thread(
            () -> {
              try {
                int index = 0;
                while (testRunning.get()) {
                  stage.setMetadataRef(fileLocationInfos.get(index++ % fileLocationInfos.size()));

                  Thread.sleep(5);
                }
              } catch (Exception e) {
                threadException.set(e);
              }
            });

    /* Thread 2: Upload thread that triggers putRemote calls */
    Thread uploadThread =
        new Thread(
            () -> {
              try {
                for (int i = 1; i <= rowCount; i++) {
                  channel.insertRow(ImmutableMap.of("int_col", i), String.valueOf(i));
                  TestUtils.waitForOffset(channel, String.valueOf(i));
                }
              } catch (Exception e) {
                threadException.set(e);
              }
            });

    refreshThread.start();
    uploadThread.start();

    uploadThread.join();

    testRunning.set(false);
    refreshThread.join();

    if (threadException.get() != null) {
      throw new RuntimeException("Thread failed during race condition test", threadException.get());
    }

    /*
     * Verify there's at least one blob path refresh (rowCount + 1),
     * possibility of no refresh happens is 1 / numFileLocationInfos^rowCount
     */
    long finalCount = storageManager.getGenerateBlobPathCount();
    assertThat(finalCount).isGreaterThan(rowCount);

    /* Verify data integrity */
    ResultSet result =
        conn.createStatement()
            .executeQuery(String.format("select int_col from %s order by int_col;", tableName));
    for (int i = 1; i <= rowCount; i++) {
      assertThat(result.next()).isTrue();
      assertThat(result.getInt(1)).isEqualTo(i);
    }
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

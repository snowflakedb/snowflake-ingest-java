/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.ROLE;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Verified that ingestion work when we ingest into large number of tables from the same client and
 * blobs and registration requests have to be cut, so they don't contain large number of chunks
 */
public class ManyTablesIT {

  private static final int TABLES_COUNT = 20;
  private static final int TOTAL_ROWS_COUNT = 200_000;
  private String dbName;
  private SnowflakeStreamingIngestClient client;
  private Connection connection;
  private SnowflakeStreamingIngestChannel[] channels;
  private String[] offsetTokensPerChannel;

  @Before
  public void setUp() throws Exception {
    Properties props = TestUtils.getProperties(Constants.BdecVersion.THREE, false);
    props.put(ParameterProvider.MAX_CHUNKS_IN_BLOB, 2);
    props.put(ParameterProvider.MAX_CHUNKS_IN_REGISTRATION_REQUEST, 2);
    if (props.getProperty(ROLE).equals("DEFAULT_ROLE")) {
      props.setProperty(ROLE, "ACCOUNTADMIN");
    }
    client = SnowflakeStreamingIngestClientFactory.builder("client1").setProperties(props).build();
    connection = TestUtils.getConnection(true);
    dbName = String.format("sdk_it_many_tables_db_%d", System.nanoTime());

    channels = new SnowflakeStreamingIngestChannel[TABLES_COUNT];
    offsetTokensPerChannel = new String[TABLES_COUNT];
    connection.createStatement().execute(String.format("create database %s;", dbName));

    String[] tableNames = new String[TABLES_COUNT];
    for (int i = 0; i < tableNames.length; i++) {
      tableNames[i] = String.format("table_%d", i);
      connection.createStatement().execute(String.format("create table table_%d(c int);", i));
      channels[i] =
          client.openChannel(
              OpenChannelRequest.builder(String.format("channel-%d", i))
                  .setDBName(dbName)
                  .setSchemaName("public")
                  .setTableName(tableNames[i])
                  .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
                  .build());
    }
  }

  @After
  public void tearDown() throws Exception {
    connection.createStatement().execute(String.format("drop database %s;", dbName));
    client.close();
    connection.close();
  }

  @Test
  public void testIngestionIntoManyTables() throws InterruptedException, SQLException {
    for (int i = 0; i < TOTAL_ROWS_COUNT; i++) {
      Map<String, Object> row = Collections.singletonMap("c", i);
      String offset = String.valueOf(i);
      int channelId = i % channels.length;
      channels[channelId].insertRow(row, offset);
      offsetTokensPerChannel[channelId] = offset;
    }

    for (int i = 0; i < channels.length; i++) {
      TestUtils.waitForOffset(channels[i], offsetTokensPerChannel[i]);
    }

    int totalRowsCount = 0;
    ResultSet rs =
        connection
            .createStatement()
            .executeQuery(String.format("show tables in database %s;", dbName));
    while (rs.next()) {
      totalRowsCount += rs.getInt("rows");
    }
    Assert.assertEquals(TOTAL_ROWS_COUNT, totalRowsCount);
  }
}

/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import static net.snowflake.ingest.utils.Constants.ROLE;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test that validates Snowpipe streaming ingestion into dynamic tables and verifies
 * that there are no duplicate primary partition names in the dynamic table.
 *
 * <p>This test performs the following steps: 1. Creates a base table for streaming ingestion 2.
 * Creates a dynamic table that aggregates data from the base table 3. Uses Snowpipe streaming to
 * ingest data into the base table 4. Waits for the dynamic table to refresh and populate 5.
 * Validates that the dynamic table contains expected aggregated data 6. Uses Snowflake metadata
 * functions to verify no duplicate partition names exist
 *
 * <p>To run this test: mvn test -Dtest=DynamicTablePartitionIT
 *
 * <p>Prerequisites: - Valid Snowflake connection profile (profile.json) - ACCOUNTADMIN role or
 * sufficient privileges to create dynamic tables - A warehouse available for dynamic table
 * operations
 */
public class DynamicTablePartitionIT {

  private static final String BASE_TABLE_NAME = "base_table_for_dynamic";
  private static final String DYNAMIC_TABLE_NAME = "dynamic_table_partition_test";
  private static final String DB_NAME = "sdk_dynamic_table_test_db";
  private static final int TEST_ROWS_COUNT = 1000;

  private SnowflakeStreamingIngestClient client;
  private Connection connection;
  private SnowflakeStreamingIngestChannel channel;

  @Before
  public void setUp() throws Exception {
    Properties props = TestUtils.getProperties(Constants.BdecVersion.THREE, false);
    if (props.getProperty(ROLE).equals("DEFAULT_ROLE")) {
      props.setProperty(ROLE, "ACCOUNTADMIN");
    }
    client =
        SnowflakeStreamingIngestClientFactory.builder("dynamic_table_test_client")
            .setProperties(props)
            .build();
    connection = TestUtils.getConnection(true);

    // Create database and schema
    connection.createStatement().execute(String.format("create or replace database %s;", DB_NAME));
    connection.createStatement().execute(String.format("use database %s;", DB_NAME));
    connection.createStatement().execute("create schema if not exists public;");
    connection.createStatement().execute("use schema public;");

    // Create base table for streaming ingestion
    connection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s ("
                    + "id int, "
                    + "name varchar(100), "
                    + "category varchar(50), "
                    + "value decimal(10,2)"
                    + ");",
                BASE_TABLE_NAME));

    // Create dynamic table based on the base table - simple select *
    connection
        .createStatement()
        .execute(
            String.format(
                "create or replace dynamic table %s "
                    + "target_lag = '1 minute' "
                    + "warehouse = %s "
                    + "as "
                    + "select * from %s;",
                DYNAMIC_TABLE_NAME, TestUtils.getWarehouse(), BASE_TABLE_NAME));

    // Open streaming channel for the base table
    channel =
        client.openChannel(
            OpenChannelRequest.builder("dynamic_table_test_channel")
                .setDBName(DB_NAME)
                .setSchemaName("public")
                .setTableName(BASE_TABLE_NAME)
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
                .build());
  }

  @After
  public void tearDown() throws Exception {
    if (channel != null) {
      channel.close();
    }
    if (connection != null) {
      connection.createStatement().execute(String.format("drop database if exists %s;", DB_NAME));
      connection.close();
    }
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testStreamingIngestToDynamicTableWithPartitionValidation() throws Exception {
    // Categories for partitioning
    String[] categories = {"Electronics", "Books", "Clothing", "Home", "Sports"};

    // Ingest test data using streaming
    for (int i = 0; i < TEST_ROWS_COUNT; i++) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", i);
      row.put("name", "Product_" + i);
      row.put("category", categories[i % categories.length]);
      row.put("value", (i % 100) + 10.50);

      String offsetToken = String.valueOf(i);
      TestUtils.verifyInsertValidationResponse(channel.insertRow(row, offsetToken));
    }

    // Wait for all data to be flushed
    TestUtils.waitForOffset(channel, String.valueOf(TEST_ROWS_COUNT - 1));

    // Verify base table has the expected number of rows
    TestUtils.verifyTableRowCount(TEST_ROWS_COUNT, connection, DB_NAME, "public", BASE_TABLE_NAME);

    // Try to manually refresh the dynamic table to ensure it's up to date
    connection
        .createStatement()
        .execute(String.format("alter dynamic table %s refresh;", DYNAMIC_TABLE_NAME));
    Thread.sleep(2000); // Wait for refresh to complete

    // Verify dynamic table has been populated with the same data as base table
    verifyDynamicTableContent();

    // Validate no duplicate partition names using metadata columns
    validateNoDuplicatePartitionNames();
  }

  @Test
  public void testDirectStreamingToDynamicTableShouldFail() throws Exception {
    // This test verifies that direct streaming to a dynamic table should fail
    // as dynamic tables should only be populated through their source tables
    try {
      client.openChannel(
          OpenChannelRequest.builder("dynamic_table_direct_channel")
              .setDBName(DB_NAME)
              .setSchemaName("public")
              .setTableName(DYNAMIC_TABLE_NAME)
              .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
              .build());

      Assert.fail("open channel on dynamic table should fail.");
    } catch (Exception e) {
      // Expected behavior - dynamic tables should not support direct streaming
      System.out.println(
          "✓ Direct streaming to dynamic table failed as expected: " + e.getMessage());
      // This is the expected behavior, so we don't fail the test
    }
  }

  /** Verifies that the dynamic table contains the same data as the base table */
  private void verifyDynamicTableContent() throws SQLException {
    // Verify dynamic table has the same number of rows as base table
    TestUtils.verifyTableRowCount(
        TEST_ROWS_COUNT, connection, DB_NAME, "public", DYNAMIC_TABLE_NAME);

    // Do a basic sanity check on the data
    ResultSet rs =
        connection
            .createStatement()
            .executeQuery(
                String.format("select count(*), min(id), max(id) from %s;", DYNAMIC_TABLE_NAME));

    if (rs.next()) {
      int rowCount = rs.getInt(1);
      int minId = rs.getInt(2);
      int maxId = rs.getInt(3);

      System.out.printf(
          "Dynamic table contains %d rows with ID range %d to %d%n", rowCount, minId, maxId);

      Assert.assertEquals(
          "Dynamic table should have same row count as base table", TEST_ROWS_COUNT, rowCount);
      Assert.assertEquals("Min ID should be 0", 0, minId);
      Assert.assertEquals("Max ID should be TEST_ROWS_COUNT-1", TEST_ROWS_COUNT - 1, maxId);
    }
  }

  /**
   * Validates that there are no duplicate partition names in the dynamic table. Directly queries
   * the dynamic table's metadata columns.
   */
  private void validateNoDuplicatePartitionNames() throws SQLException {
    // Query the dynamic table directly for duplicate PRIMARY_PARTITION_NAME values
    String partitionValidationQuery =
        String.format(
            "select "
                + "  IFNULL(METADATA$ORIGINAL_PARTITION_NAME, "
                + "    IFNULL(METADATA$PRIMARY_PARTITION_NAME, "
                + "      METADATA$PARTITION_NAME)) as files, "
                + "  IFNULL(METADATA$ORIGINAL_PARTITION_ROW_NUMBER, "
                + "    METADATA$PARTITION_ROW_NUMBER) as row_num, "
                + "  count(*) as num "
                + "from %s "
                + "where true "
                + "group by all "
                + "having num > 1;",
            DYNAMIC_TABLE_NAME);

    ResultSet rs = connection.createStatement().executeQuery(partitionValidationQuery);

    // If any results are returned, it means we have duplicate partition names
    if (rs.next()) {
      String duplicatePartition = rs.getString("files");
      int duplicateRowNum = rs.getInt("row_num");
      int duplicateCount = rs.getInt("num");

      Assert.fail(
          String.format(
              "Found duplicate partition name '%s' with row number %d and count %d in dynamic table"
                  + " %s",
              duplicatePartition, duplicateRowNum, duplicateCount, DYNAMIC_TABLE_NAME));
    }

    System.out.println("✓ No duplicate PRIMARY_PARTITION_NAME found in dynamic table");
  }
}

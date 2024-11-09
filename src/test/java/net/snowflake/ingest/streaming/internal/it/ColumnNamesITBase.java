/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.internal.datatypes.AbstractDataTypeTest;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;

public abstract class ColumnNamesITBase extends AbstractDataTypeTest {
  private static final int INGEST_VALUE = 1;

  @Test
  public void testColumnNamesSupport() throws Exception {
    // Test simple case
    testColumnNameSupported("FOO", "FOO");
    testColumnNameSupported("FOO", "FoO");
    testColumnNameSupported("FOO", "\"FOO\"");
    testColumnNameUnsupported("FOO", "\"Foo\"");

    // Test quoted identifier
    testColumnNameSupported("\"FOO\"", "\"FOO\"");
    testColumnNameSupported("\"FOO\"", "FOO");
    testColumnNameSupported("\"FOO\"", "Foo");
    testColumnNameUnsupported("\"FOO\"", "\"Foo\"");

    testColumnNameSupported("\"Foo\"", "\"Foo\"");
    testColumnNameUnsupported("\"Foo\"", "Foo");

    // Test keyword
    testColumnNameSupported("\"CReATE\"", "\"CReATE\"");
    testColumnNameSupported("\"CREATE\"", "CReATE");
    testColumnNameUnsupported("\"CReATE\"", "\"CREATE\"");
    testColumnNameUnsupported("\"CReATE\"", "CReATE");

    // Test escaped space
    testColumnNameSupported("fO\\ O", "fO\\ O");
    testColumnNameSupported("fO\\ O", "fO\\ o");
    testColumnNameSupported("fO\\ O", "fO O");
    testColumnNameSupported("fO\\ O", "fO o");
    testColumnNameSupported("fO\\ O", "\"FO O\"");
    testColumnNameUnsupported("fO\\ O", "\"FO\\ O\"");

    // Test double quotes
    testColumnNameSupported("\"foo\"\"bar\"", "\"foo\"\"bar\"");
    testColumnNameSupported("\"FOO\"\"BAR\"", "foo\"bar");
    testColumnNameSupported("\"\"\"\"", "\"");
    testColumnNameSupported("\"\"\"\"", "\"\"\"\"");
    testColumnNameUnsupported("\"\"\"\"", "\"\"\"\"\"\"");

    // Test quoted column with spaces
    testColumnNameSupported("\"FO O\"", "FO O");
    testColumnNameSupported("\"FO O\"", "\"FO O\"");
    testColumnNameUnsupported("\"FO O\"", "\"FO\\ O\"");
  }

  @Test
  public void testNonstandardTableAndColumnNames() throws Exception {
    testColumnNameSupported("fo\\ o");
    testColumnNameSupported("foo");
    testColumnNameSupported("\"foo\"");
    testColumnNameSupported("\"fo o\"");
    testColumnNameSupported("\"alter\"");
    testColumnNameSupported("\"table\"");
    testColumnNameSupported("\"  \"");
    testColumnNameSupported("\"\"");
    testColumnNameSupported("\"a\"\"b\"");
    testColumnNameSupported("\"\"\"\"");
    testColumnNameSupported("\"  \"\"  \"\"  \"");
    testColumnNameSupported("\"  \"\" Ť \"\"  \"");
  }

  /** Tests that quoted columns are correctly resolved for null-backfill */
  @Test
  public void testNullableResolution() throws Exception {
    String tableName =
        createTableWithColumns(
            "AbC int", "\"AbC\" int", "\"abC\" int", "ab\\ c int", "\"Ab c\" int");
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    String offsetToken = "token1";
    channel.insertRow(new HashMap<>(), offsetToken);
    TestUtils.waitForOffset(channel, offsetToken);

    ResultSet rs =
        conn.createStatement().executeQuery(String.format("select * from %s", tableName));
    rs.next();
    Assert.assertNull(rs.getObject(1));
    Assert.assertNull(rs.getObject(2));
    Assert.assertNull(rs.getObject(3));
    Assert.assertNull(rs.getObject(4));
    Assert.assertNull(rs.getObject(5));
  }

  /**
   * Test that original user input is used in extra column names validation response (required by
   * KC)
   */
  @Test
  public void testExtraColNames() throws Exception {
    String tableName = createTableWithColumns("\"create\" int");
    SnowflakeStreamingIngestChannel channel =
        openChannel(tableName, OpenChannelRequest.OnErrorOption.CONTINUE);

    // Test simple input
    Map<String, Object> row = new HashMap<>();
    row.put("\"create\"", 4);
    row.put("abc", 11);
    InsertValidationResponse insertValidationResponse =
        channel.insertRow(row, UUID.randomUUID().toString());
    Assert.assertEquals(1, insertValidationResponse.getInsertErrors().size());
    Assert.assertEquals(
        Collections.singletonList("abc"),
        insertValidationResponse.getInsertErrors().get(0).getExtraColNames());

    // Test quoted input
    row = new HashMap<>();
    row.put("\"create\"", 4);
    row.put("\"CrEaTe\"", 11);
    insertValidationResponse = channel.insertRow(row, UUID.randomUUID().toString());
    Assert.assertEquals(1, insertValidationResponse.getInsertErrors().size());
    Assert.assertEquals(
        Collections.singletonList("\"CrEaTe\""),
        insertValidationResponse.getInsertErrors().get(0).getExtraColNames());
  }

  /** Test that display names are shown in missing not null columns validation response */
  @Test
  public void testMissingNotNullColNames() throws Exception {
    String tableName =
        createTableWithColumns(
            "\"CrEaTe\" int not null", "a int not null", "\"a\" int not null", "\"create\" int");
    SnowflakeStreamingIngestChannel channel =
        openChannel(tableName, OpenChannelRequest.OnErrorOption.CONTINUE);

    InsertValidationResponse insertValidationResponse =
        channel.insertRow(new HashMap<>(), UUID.randomUUID().toString());
    Assert.assertEquals(1, insertValidationResponse.getInsertErrors().size());
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("\"CrEaTe\"", "A", "\"a\"")),
        new HashSet<>(
            insertValidationResponse.getInsertErrors().get(0).getMissingNotNullColNames()));
  }

  /** Test that display names are shown in null values for not null columns validation response */
  @Test
  public void testNullValuesForNotNullColNames() throws Exception {
    String tableName =
        createTableWithColumns(
            "col1 int not null",
            "a int not null",
            "\"a\" int not null",
            "col2 int not null",
            "\"col3\" int");
    SnowflakeStreamingIngestChannel channel =
        openChannel(tableName, OpenChannelRequest.OnErrorOption.CONTINUE);

    HashMap<String, Object> row = new HashMap<>();
    row.put("col1", null);
    row.put("col2", null);
    row.put("a", "null");
    row.put("\"a\"", "someValue");
    InsertValidationResponse insertValidationResponse =
        channel.insertRow(row, UUID.randomUUID().toString());
    Assert.assertEquals(1, insertValidationResponse.getInsertErrors().size());
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("COL1", "COL2")),
        new HashSet<>(
            insertValidationResponse.getInsertErrors().get(0).getNullValueForNotNullColNames()));
  }

  /**
   * Tests that data can be ingested for specific row key into a specific column
   *
   * @param createTableColumnName Column name used in CREATE TABLE
   * @param ingestColumnName Column name of used in ingestion
   */
  private void testColumnNameSupported(String createTableColumnName, String ingestColumnName)
      throws SQLException, InterruptedException {

    String tableName = createTableWithColumns(createTableColumnName + " int");
    String offsetToken = UUID.randomUUID().toString();
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    Map<String, Object> row = new HashMap<>();
    row.put(ingestColumnName, INGEST_VALUE);
    channel.insertRow(row, offsetToken);

    TestUtils.waitForOffset(channel, offsetToken);

    // Verify that supported columns work
    ResultSet rs =
        conn.createStatement().executeQuery(String.format("select * from %s;", tableName));
    int count = 0;
    while (rs.next()) {
      count++;
      Assert.assertEquals(INGEST_VALUE, rs.getInt(1));
    }
    Assert.assertEquals(1, count);

    if (!enableIcebergStreaming) {
      conn.createStatement().execute(String.format("alter table %s migrate;", tableName));
    }
  }

  private void testColumnNameSupported(String column) throws SQLException, InterruptedException {
    testColumnNameSupported(column, column);
  }

  /** Verifies that column names that are not support to work, does not work */
  private void testColumnNameUnsupported(String createTableColumnName, String ingestColumnName)
      throws SQLException {

    Map<String, Object> row = new HashMap<>();
    row.put(ingestColumnName, INGEST_VALUE);
    SnowflakeStreamingIngestChannel channel =
        openChannel(createTableWithColumns(createTableColumnName + " int"));
    testInsertRowFails(channel, row);
  }

  private void testInsertRowFails(
      SnowflakeStreamingIngestChannel channel, Map<String, Object> row) {
    try {
      channel.insertRow(row, UUID.randomUUID().toString());
      Assert.fail("Ingest row should not succeed");
    } catch (SFException e) {
      // all good, expected exception has been thrown
    }
  }

  private String createTableWithColumns(String... columns) throws SQLException {
    String tableName = "a" + UUID.randomUUID().toString().replace("-", "_");
    String createTableSql =
        String.format(
            "create %s table %s (%s) %s",
            enableIcebergStreaming ? "iceberg" : "",
            tableName,
            String.join(",", columns),
            enableIcebergStreaming ? getIcebergTableConfig(tableName) : "");
    conn.createStatement().execute(createTableSql);
    return tableName;
  }
}

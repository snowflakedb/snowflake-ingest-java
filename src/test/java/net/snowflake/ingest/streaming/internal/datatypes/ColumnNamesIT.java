package net.snowflake.ingest.streaming.internal.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;

public class ColumnNamesIT extends AbstractDataTypeTest {
  private static final int INGEST_VALUE = 1;

  public ColumnNamesIT(String name, Constants.BdecVersion bdecVersion) {
    super(name, bdecVersion);
  }

  @Test
  public void testColumnNamesSupport() throws Exception {
    // Test simple case
    testWorks("FOO", "FOO");
    testWorks("FOO", "FoO");
    testWorks("FOO", "\"FOO\"");
    testDoesNotWork("FOO", "\"Foo\"");

    // Test quoted identifier
    testWorks("\"FOO\"", "\"FOO\"");
    testWorks("\"FOO\"", "FOO");
    testWorks("\"FOO\"", "Foo");
    testDoesNotWork("\"FOO\"", "\"Foo\"");

    testWorks("\"Foo\"", "\"Foo\"");
    testDoesNotWork("\"Foo\"", "Foo");

    // Test keyword
    testWorks("\"CReATE\"", "\"CReATE\"");
    testWorks("\"CREATE\"", "CReATE");
    testDoesNotWork("\"CReATE\"", "\"CREATE\"");
    testDoesNotWork("\"CReATE\"", "CReATE");

    // Test escaped space
    testWorks("fO\\ O", "fO\\ O");
    testWorks("fO\\ O", "fO\\ o");
    testWorks("fO\\ O", "fO O");
    testWorks("fO\\ O", "fO o");
    testWorks("fO\\ O", "\"FO O\"");
    testDoesNotWork("fO\\ O", "\"FO\\ O\"");

    // Test double quotes
    testWorks("\"\"\"\"", "\"");
    testWorks("\"\"\"\"", "\"\"\"\"");
    testDoesNotWork("\"\"\"\"", "\"\"\"\"\"\"");

    // Test quoted column with spaces
    testWorks("\"FO O\"", "FO O");
    testWorks("\"FO O\"", "\"FO O\"");
    testDoesNotWork("\"FO O\"", "\"FO\\ O\"");
  }

  @Test
  public void testNonstandardTableAndColumnNames() throws Exception {
    testWorks("fo\\ o");
    testWorks("foo");
    testWorks("\"foo\"");
    testWorks("\"fo o\"");
    testWorks("\"alter\"");
    testWorks("\"table\"");
    testWorks("\"  \"");
    testWorks("\"\"");
    testWorks("\"a\"\"b\"");
    testWorks("\"\"\"\"");
    testWorks("\"  \"\"  \"\"  \"");
    testWorks("\"  \"\" Ť \"\"  \"");
  }

  /** Tests that quoted columns are correctly resolved for null-backfill */
  @Test
  public void testNullableResolution() throws Exception {
    String tableName = "t1";
    conn.createStatement()
        .execute(
            String.format(
                "create or replace table %s (\"AbC\" int, \"abC\" int, ab\\ c int, \"Ab c\" int);",
                tableName));
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
  }

  /** Tests that quoted columns are correctly resolved for not-null checks */
  @Test
  public void testNonNullableResolution() throws Exception {
    String tableName = "t1";
    conn.createStatement()
        .execute(
            String.format(
                "create or replace table %s (AbC int not null, \"AbC\" int not null);", tableName));
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    testInsertRowFails(channel, Collections.singletonMap("AbC", 1));
    testInsertRowFails(channel, Collections.singletonMap("\"AbC\"", 1));
  }

  private void testWorks(String column) throws SQLException, InterruptedException {
    testWorks(column, column);
  }

  /** // Verifies that column names that are not support to work, does not work */
  private void testDoesNotWork(String createTableColumnName, String ingestColumnName)
      throws SQLException {

    Map<String, Object> row = new HashMap<>();
    row.put(ingestColumnName, INGEST_VALUE);

    SnowflakeStreamingIngestChannel channel = openChannel(createSimpleTable(createTableColumnName));
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

  private void testWorks(String createTableColumnName, String ingestColumnName)
      throws SQLException, InterruptedException {

    String tableName = createSimpleTable(createTableColumnName);
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
  }

  private String createSimpleTable(String createTableColumnName) throws SQLException {
    String tableName = "a" + UUID.randomUUID().toString().replace("-", "_");
    String createTableSql =
        String.format("create table %s (%s int);", tableName, createTableColumnName);
    conn.createStatement().execute(createTableSql);
    return tableName;
  }
}

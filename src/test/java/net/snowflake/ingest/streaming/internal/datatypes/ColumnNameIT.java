package net.snowflake.ingest.streaming.internal.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that it is possible to ingest into tables and columns, which have special and non-standard
 * characters in their names.
 */
public class ColumnNameIT extends AbstractDataTypeTest {

  @Test
  public void testNonstandardTableAneColumnNames() throws Exception {
    testName("foo");
    testName("\"foo\"");
    testName("\"fo o\"");
    testName("\"alter\"");
    testName("\"table\"");
    testName("\"  \"");
    testName("\"\"");
    testName("\"a\"\"b\"");
    testName("\"\"\"\"");
    testName("\"  \"\"  \"\"  \"");
    testName("\"  \"\" Ť \"\"  \"");
  }

  private void testName(String name) throws Exception {

    conn.createStatement().execute(String.format("create table %s (%s int)", name, name));

    String offset = UUID.randomUUID().toString();
    int ingestedValue = 1;
    SnowflakeStreamingIngestChannel channel = openChannel(name);

    Map<String, Object> row = new HashMap<>();
    row.put(name, ingestedValue);
    channel.insertRow(row, offset);

    TestUtils.waitForOffset(channel, offset);
    assertSingleRow(name, ingestedValue);

    migrateTable(name);
    assertSingleRow(name, ingestedValue);

    channel.close().get();
  }

  private void assertSingleRow(String name, int value) throws SQLException {
    ResultSet resultSet =
        conn.createStatement()
            .executeQuery(
                String.format("select count(*) from %s where %s = %s", name, name, value));
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(1, resultSet.getInt(1));
  }
}

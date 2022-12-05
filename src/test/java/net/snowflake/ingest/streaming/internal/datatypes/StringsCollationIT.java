package net.snowflake.ingest.streaming.internal.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Collated columns are currently not supported, only NULL values can be ingested into such columns.
 */
public class StringsCollationIT extends AbstractDataTypeTest {

  public StringsCollationIT(String name, Constants.BdecVersion bdecVersion) {
    super(name, bdecVersion);
  }

  /** Verifies that non-NULL values cannot be ingested into collated columns. */
  @Test
  public void testNotNullValuesNotSupported() throws SQLException {
    String tableName = getRandomIdentifier();
    conn.createStatement()
        .execute(
            String.format(
                "create or replace table %s (\"create\" string collate 'en_US')", tableName));
    SnowflakeStreamingIngestChannel channel =
        openChannel(tableName, OpenChannelRequest.OnErrorOption.CONTINUE);
    InsertValidationResponse insertValidationResponse =
        channel.insertRow(Collections.singletonMap("\"create\"", "abc"), getRandomIdentifier());
    Assert.assertEquals(1, insertValidationResponse.getErrorRowCount());
    Assert.assertEquals(
        ErrorCode.DATA_TYPE_NOT_SUPPORTED.getMessageCode(),
        insertValidationResponse.getInsertErrors().get(0).getException().getVendorCode());
  }

  /**
   * Verifies that NULL values can be (implicitly) ingested into collated columns by omitting the
   * column.
   */
  @Test
  public void testImplicitNullsSupported() throws SQLException, InterruptedException {
    String tableName = getRandomIdentifier();
    conn.createStatement()
        .execute(
            String.format(
                "create or replace table %s (\"create\" string collate 'en_US')", tableName));
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    String offsetToken = getRandomIdentifier();
    channel.insertRow(new HashMap<>(), offsetToken);
    TestUtils.waitForOffset(channel, offsetToken);

    // Query without WHERE
    ResultSet rs1 =
        conn.createStatement().executeQuery(String.format("select * from %s", tableName));
    rs1.next();
    Assert.assertNull(rs1.getString(1));

    // Query with WHERE
    rs1 =
        conn.createStatement()
            .executeQuery(String.format("select * from %s where \"create\" is null", tableName));
    rs1.next();
    Assert.assertNull(rs1.getString(1));
    migrateTable(tableName);
  }

  /**
   * Verifies that NULL values can be ingested into collated columns by explicitly putting them into
   * the row hash map.
   */
  @Test
  public void testExplicitNullsSupported() throws SQLException, InterruptedException {
    String tableName = getRandomIdentifier();
    conn.createStatement()
        .execute(
            String.format(
                "create or replace table %s (\"create\" string collate 'en_US')", tableName));
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    String offsetToken = getRandomIdentifier();
    channel.insertRow(Collections.singletonMap("\"create\"", null), offsetToken);
    TestUtils.waitForOffset(channel, offsetToken);

    // Query without WHERE
    ResultSet rs1 =
        conn.createStatement().executeQuery(String.format("select * from %s", tableName));
    rs1.next();
    Assert.assertNull(rs1.getString(1));

    // Query with WHERE
    rs1 =
        conn.createStatement()
            .executeQuery(String.format("select * from %s where \"create\" is null", tableName));
    rs1.next();
    Assert.assertNull(rs1.getString(1));

    migrateTable(tableName);
  }

  /**
   * Verifies that non-nullable collated columns are not supported at all and an exception is thrown
   * already while creating the channel.
   */
  @Test
  public void testCollatedNotNullColumnsNotSupported() throws SQLException {
    String tableName = getRandomIdentifier();
    conn.createStatement()
        .execute(
            String.format(
                "create or replace table %s (\"create\" string collate 'en_US' not null)",
                tableName));
    try {
      openChannel(tableName);
      Assert.fail("Opening a channel shouldn't have succeeded");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.DATA_TYPE_NOT_SUPPORTED.getMessageCode(), e.getVendorCode());
    }
  }
}

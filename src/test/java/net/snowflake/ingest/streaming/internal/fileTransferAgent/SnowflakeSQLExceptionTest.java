/*
 * Tests for SnowflakeSQLException — replicated from snowflake-jdbc.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SnowflakeSQLExceptionTest {

  @Test
  public void testConstructorWithSqlStateAndVendorCode() {
    SnowflakeSQLException ex =
        new SnowflakeSQLException(
            SqlState.INTERNAL_ERROR,
            StorageErrorCode.INTERNAL_ERROR.getMessageCode(),
            "test detail");
    assertEquals(SqlState.INTERNAL_ERROR, ex.getSQLState());
    assertEquals(200001, ex.getErrorCode());
    assertNotNull(ex.getMessage());
  }

  @Test
  public void testConstructorWithThrowable() {
    RuntimeException cause = new RuntimeException("root cause");
    SnowflakeSQLException ex =
        new SnowflakeSQLException(
            cause,
            SqlState.SYSTEM_ERROR,
            StorageErrorCode.IO_ERROR.getMessageCode(),
            "upload failed");
    assertEquals(SqlState.SYSTEM_ERROR, ex.getSQLState());
    assertEquals(200016, ex.getErrorCode());
    assertEquals(cause, ex.getCause());
  }

  @Test
  public void testConstructorWithStorageErrorCode() {
    SnowflakeSQLException ex =
        new SnowflakeSQLException(StorageErrorCode.INVALID_PROXY_PROPERTIES, "bad port");
    assertEquals("08000", ex.getSQLState());
    assertEquals(200051, ex.getErrorCode());
  }

  @Test
  public void testMessageFromResourceBundle() {
    // ErrorCode 200001 maps to "JDBC driver internal error: {0}."
    SnowflakeSQLException ex =
        new SnowflakeSQLException(SqlState.INTERNAL_ERROR, 200001, "something broke");
    assertTrue(ex.getMessage().contains("something broke"));
  }

  @Test
  public void testMissingResourceBundleKey() {
    // Non-existent code should produce a fallback message like "!999999!"
    SnowflakeSQLException ex = new SnowflakeSQLException(SqlState.INTERNAL_ERROR, 999999);
    assertTrue(ex.getMessage().contains("999999"));
  }

  @Test
  public void testQueryId() {
    SnowflakeSQLException ex = new SnowflakeSQLException(SqlState.INTERNAL_ERROR, 200001);
    assertEquals("unknown", ex.getQueryId());
  }
}

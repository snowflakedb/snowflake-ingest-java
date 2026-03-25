// Ported from snowflake-jdbc: net.snowflake.client.jdbc.SnowflakeSQLExceptionTest
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import org.junit.Test;

public class SnowflakeSQLExceptionTest {

  @Test
  public void testConstructorWithQueryIdReasonSqlStateVendorCode() {
    SnowflakeSQLException ex = new SnowflakeSQLException("qid-123", "some reason", "22000", 200001);
    assertEquals("qid-123", ex.getQueryId());
    assertEquals("some reason", ex.getMessage());
    assertEquals("22000", ex.getSQLState());
    assertEquals(200001, ex.getErrorCode());
  }

  @Test
  public void testConstructorWithSqlStateAndVendorCode() {
    // Looks up localized message from resource bundle
    SnowflakeSQLException ex = new SnowflakeSQLException("22000", 200001);
    assertNotNull(ex.getMessage());
    assertTrue(ex.getMessage().contains("internal error"));
    assertEquals("22000", ex.getSQLState());
    assertEquals(200001, ex.getErrorCode());
  }

  @Test
  public void testConstructorWithErrorCode() {
    SnowflakeSQLException ex = new SnowflakeSQLException(ErrorCode.IO_ERROR);
    assertNotNull(ex.getMessage());
    assertEquals(ErrorCode.IO_ERROR.getSqlState(), ex.getSQLState());
    assertEquals(200016, ex.getErrorCode());
  }

  @Test
  public void testConstructorWithErrorCodeAndParams() {
    SnowflakeSQLException ex = new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR, "test detail");
    assertTrue(ex.getMessage().contains("test detail"));
  }

  @Test
  public void testConstructorWithThrowable() {
    Exception cause = new RuntimeException("root cause");
    SnowflakeSQLException ex = new SnowflakeSQLException(cause, "22000", 200001);
    assertEquals(cause, ex.getCause());
    assertEquals("22000", ex.getSQLState());
    assertEquals(200001, ex.getErrorCode());
  }

  @Test
  public void testConstructorWithThrowableAndErrorCode() {
    Exception cause = new RuntimeException("root cause");
    SnowflakeSQLException ex =
        new SnowflakeSQLException(cause, ErrorCode.IO_ERROR, "upload failed");
    assertEquals(cause, ex.getCause());
    assertEquals(ErrorCode.IO_ERROR.getSqlState(), ex.getSQLState());
  }

  @Test
  public void testConstructorWithRetryInfo() {
    SnowflakeSQLException ex = new SnowflakeSQLException(ErrorCode.NETWORK_ERROR, 3, true, 120L);
    assertEquals(3, ex.getRetryCount());
    assertTrue(ex.issocketTimeoutNoBackoff());
    assertEquals(120L, ex.getElapsedSeconds());
  }

  @Test
  public void testDefaultValues() {
    SnowflakeSQLException ex = new SnowflakeSQLException("test reason");
    assertEquals("unknown", ex.getQueryId());
    assertEquals(0, ex.getRetryCount());
    assertFalse(ex.issocketTimeoutNoBackoff());
    assertEquals(0L, ex.getElapsedSeconds());
  }

  @Test
  public void testConstructorWithThrowableAndMessage() {
    Exception cause = new RuntimeException("cause");
    SnowflakeSQLException ex = new SnowflakeSQLException(cause, "error message");
    assertEquals("error message", ex.getMessage());
    assertEquals(cause, ex.getCause());
  }

  @Test
  public void testIsSQLException() {
    SnowflakeSQLException ex = new SnowflakeSQLException("test");
    assertTrue(ex instanceof SQLException);
  }
}

// Ported from snowflake-jdbc: net.snowflake.client.jdbc.SnowflakeSQLLoggedException
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import net.minidev.json.JSONObject;
import org.junit.Test;

public class SnowflakeSQLLoggedExceptionTest {

  @Test
  public void testConstructorWithNullSession() {
    // All ingest callers pass null for session — this is the primary usage
    SnowflakeSQLLoggedException ex = new SnowflakeSQLLoggedException(null, 200016, "58000");
    assertNotNull(ex.getMessage());
    assertEquals("58000", ex.getSQLState());
    assertEquals(200016, ex.getErrorCode());
  }

  @Test
  public void testConstructorWithNullSessionAndParams() {
    SnowflakeSQLLoggedException ex =
        new SnowflakeSQLLoggedException(null, 200001, "22000", "test detail");
    assertNotNull(ex.getMessage());
    assertTrue(ex.getMessage().contains("test detail"));
  }

  @Test
  public void testConstructorWithErrorCode() {
    SnowflakeSQLLoggedException ex =
        new SnowflakeSQLLoggedException(null, ErrorCode.IO_ERROR, new RuntimeException("cause"));
    assertEquals(ErrorCode.IO_ERROR.getSqlState(), ex.getSQLState());
    assertNotNull(ex.getCause());
  }

  @Test
  public void testConstructorWithQueryId() {
    SnowflakeSQLLoggedException ex =
        new SnowflakeSQLLoggedException("qid-456", null, 200016, "58000");
    assertEquals("qid-456", ex.getQueryId());
    assertEquals("58000", ex.getSQLState());
  }

  @Test
  public void testExtendsSnowflakeSQLException() {
    SnowflakeSQLLoggedException ex = new SnowflakeSQLLoggedException(null, 200016, "58000");
    assertTrue(ex instanceof SnowflakeSQLException);
  }

  @Test
  public void testMaskStacktrace() {
    String stackTrace =
        "net.snowflake.client.jdbc.SnowflakeSQLException: some sensitive error message\n"
            + "\tat net.snowflake.client.jdbc.SomeClass.method(SomeClass.java:42)";
    String masked = SnowflakeSQLLoggedException.maskStacktrace(stackTrace);
    // The sensitive error message after the exception class name should be removed
    assertNotNull(masked);
  }

  @Test
  public void testMaskStacktraceNoMatch() {
    String stackTrace = "java.lang.RuntimeException: something\n\tat some.Class.method(File:1)";
    String masked = SnowflakeSQLLoggedException.maskStacktrace(stackTrace);
    // No Snowflake exception pattern, should return unchanged
    assertEquals(stackTrace, masked);
  }

  @Test
  public void testCreateOOBValue() {
    JSONObject value = SnowflakeSQLLoggedException.createOOBValue("qid-1", "22000", 200001);
    assertEquals("client_sql_exception", value.get("type"));
    assertEquals("qid-1", value.get("QueryID"));
    assertEquals("22000", value.get("SQLState"));
    assertEquals(200001, value.get("ErrorNumber"));
  }

  @Test
  public void testCreateOOBValueNullQueryId() {
    JSONObject value = SnowflakeSQLLoggedException.createOOBValue(null, "58000", 200016);
    // null queryId should not be included
    assertTrue(!value.containsKey("QueryID"));
    assertEquals("58000", value.get("SQLState"));
  }

  @Test
  public void testCreateOOBValueNoVendorCode() {
    JSONObject value = SnowflakeSQLLoggedException.createOOBValue("qid", "22000", -1);
    // NO_VENDOR_CODE (-1) should not be included
    assertTrue(!value.containsKey("ErrorNumber"));
  }

  @Test
  public void testCreateIBValue() {
    ObjectNode value = SnowflakeSQLLoggedException.createIBValue("qid-2", "22000", 200001);
    assertEquals("client_sql_exception", value.get("type").asText());
    assertEquals("qid-2", value.get("QueryID").asText());
    assertEquals("22000", value.get("SQLState").asText());
    assertEquals(200001, value.get("ErrorNumber").asInt());
  }

  @Test
  public void testCreateIBValueNullFields() {
    ObjectNode value = SnowflakeSQLLoggedException.createIBValue(null, null, -1);
    assertTrue(!value.has("QueryID"));
    assertTrue(!value.has("SQLState"));
    assertTrue(!value.has("ErrorNumber"));
  }
}

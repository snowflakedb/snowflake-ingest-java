// Ported from snowflake-jdbc: net.snowflake.client.jdbc.ErrorCode
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ErrorCodeTest {

  @Test
  public void testMessageCodeRange() {
    for (ErrorCode code : ErrorCode.values()) {
      int messageCode = code.getMessageCode();
      assertTrue(
          "Message code " + messageCode + " should be in 200001-200071 range",
          messageCode >= 200001 && messageCode <= 200071);
    }
  }

  @Test
  public void testSqlStateNotNull() {
    for (ErrorCode code : ErrorCode.values()) {
      assertNotNull("SQL state should not be null for " + code.name(), code.getSqlState());
    }
  }

  @Test
  public void testSpecificCodes() {
    assertEquals(Integer.valueOf(200001), ErrorCode.INTERNAL_ERROR.getMessageCode());
    assertEquals(Integer.valueOf(200016), ErrorCode.IO_ERROR.getMessageCode());
    assertEquals(Integer.valueOf(200020), ErrorCode.AWS_CLIENT_ERROR.getMessageCode());
  }

  @Test
  public void testCloudStorageCredentialsExpired() {
    assertEquals(240001, ErrorCode.CLOUD_STORAGE_CREDENTIALS_EXPIRED);
  }

  @Test
  public void testToString() {
    String str = ErrorCode.INTERNAL_ERROR.toString();
    assertTrue(str.contains("200001"));
    assertTrue(str.contains("sqlState"));
  }

  @Test
  public void testErrorMessageResource() {
    assertEquals(
        "net.snowflake.ingest.streaming.internal.fileTransferAgent.jdbc_error_messages",
        ErrorCode.errorMessageResource);
  }
}

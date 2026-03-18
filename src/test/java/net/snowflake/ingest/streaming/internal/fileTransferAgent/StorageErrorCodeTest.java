/*
 * Tests for StorageErrorCode — replicated from snowflake-jdbc ErrorCode.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class StorageErrorCodeTest {

  @Test
  public void testMessageCodes() {
    assertEquals(Integer.valueOf(200001), StorageErrorCode.INTERNAL_ERROR.getMessageCode());
    assertEquals(Integer.valueOf(200013), StorageErrorCode.S3_OPERATION_ERROR.getMessageCode());
    assertEquals(Integer.valueOf(200016), StorageErrorCode.IO_ERROR.getMessageCode());
    assertEquals(Integer.valueOf(200020), StorageErrorCode.AWS_CLIENT_ERROR.getMessageCode());
    assertEquals(Integer.valueOf(200044), StorageErrorCode.AZURE_SERVICE_ERROR.getMessageCode());
    assertEquals(Integer.valueOf(200051), StorageErrorCode.INVALID_PROXY_PROPERTIES.getMessageCode());
    assertEquals(Integer.valueOf(200061), StorageErrorCode.GCP_SERVICE_ERROR.getMessageCode());
  }

  @Test
  public void testSqlStates() {
    assertEquals("XX000", StorageErrorCode.INTERNAL_ERROR.getSqlState());
    assertEquals("58000", StorageErrorCode.S3_OPERATION_ERROR.getSqlState());
    assertEquals("58030", StorageErrorCode.IO_ERROR.getSqlState());
  }

  @Test
  public void testCloudStorageCredentialsExpired() {
    assertEquals(240001, StorageErrorCode.CLOUD_STORAGE_CREDENTIALS_EXPIRED);
  }
}

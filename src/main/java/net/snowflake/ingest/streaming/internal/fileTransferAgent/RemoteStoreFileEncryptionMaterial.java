/*
 * Replicated from snowflake-jdbc-thin (v3.25.1):
 *   net.snowflake.client.jdbc.internal.snowflake.common.core.RemoteStoreFileEncryptionMaterial
 * Originally from net.snowflake:snowflake-common (no public source URL).
 * Decompiled from the JDBC thin jar.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

public class RemoteStoreFileEncryptionMaterial {
  private String queryStageMasterKey;
  private String queryId;
  private Long smkId;

  public RemoteStoreFileEncryptionMaterial(String queryStageMasterKey, String queryId, Long smkId) {
    this.queryStageMasterKey = queryStageMasterKey;
    this.queryId = queryId;
    this.smkId = smkId;
  }

  public RemoteStoreFileEncryptionMaterial() {}

  public String getQueryStageMasterKey() {
    return queryStageMasterKey;
  }

  public void setQueryStageMasterKey(String queryStageMasterKey) {
    this.queryStageMasterKey = queryStageMasterKey;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public Long getSmkId() {
    return smkId;
  }

  public void setSmkId(long smkId) {
    this.smkId = smkId;
  }
}

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.utils.Utils;

/** Represents an encryption key for a table */
public class EncryptionKey {
  // Database name
  private final String databaseName;

  // Schema name
  private final String schemaName;

  // Table Name
  private final String tableName;

  String blobTableMasterKey;

  long encryptionKeyId;

  public EncryptionKey(
      @JsonProperty("database") String databaseName,
      @JsonProperty("schema") String schemaName,
      @JsonProperty("table") String tableName,
      @JsonProperty("encryption_key") String blobTableMasterKey,
      @JsonProperty("encryption_key_id") long encryptionKeyId) {
    this.databaseName = databaseName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.blobTableMasterKey = blobTableMasterKey;
    this.encryptionKeyId = encryptionKeyId;
  }

  public EncryptionKey(EncryptionKey encryptionKey) {
    this.databaseName = encryptionKey.databaseName;
    this.schemaName = encryptionKey.schemaName;
    this.tableName = encryptionKey.tableName;
    this.blobTableMasterKey = encryptionKey.blobTableMasterKey;
    this.encryptionKeyId = encryptionKey.encryptionKeyId;
  }

  public String getFullyQualifiedTableName() {
    return Utils.getFullyQualifiedTableName(databaseName, schemaName, tableName);
  }

  @JsonProperty("database")
  public String getDatabaseName() {
    return databaseName;
  }

  @JsonProperty("schema")
  public String getSchemaName() {
    return schemaName;
  }

  @JsonProperty("table")
  public String getTableName() {
    return tableName;
  }

  @JsonProperty("encryption_key")
  public String getEncryptionKey() {
    return blobTableMasterKey;
  }

  @JsonProperty("encryption_key_id")
  public long getEncryptionKeyId() {
    return encryptionKeyId;
  }
}

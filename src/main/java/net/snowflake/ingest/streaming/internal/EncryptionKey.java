package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

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
            String databaseName,
            String schemaName,
            String tableName,
            String blobTableMasterKey,
            long encryptionKeyId) {
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.blobTableMasterKey = blobTableMasterKey;
        this.encryptionKeyId = encryptionKeyId;
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

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

class GeneratePresignedUrlsRequest implements IStreamingIngestRequest {
    // TODO: Add requestId and log somewhere

    private String dbName;
    private String schemaName;
    private String tableName;
    private Integer count;
    private String encodedFigsId;

    public GeneratePresignedUrlsRequest(TableRef tableRef, int count) {
        this(tableRef, count, null);
    }

    public GeneratePresignedUrlsRequest(TableRef tableRef, int count, String encodedFigsId) {
        this.dbName = tableRef.dbName;
        this.schemaName = tableRef.schemaName;
        this.tableName = tableRef.tableName;
        this.count = count;
        this.encodedFigsId = encodedFigsId;

    }

    @JsonProperty("database")
    void setDBName(String dbName) {
        this.dbName = dbName;
    }

    String getDBName() {
        return this.dbName;
    }

    @JsonProperty("schema")
    void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    String getSchemaName() {
        return this.schemaName;
    }

    @JsonProperty("table")
    void setTableName(String tableName) {
        this.tableName = tableName;
    }

    String getTableName() {
        return this.tableName;
    }

    @JsonProperty("encoded_figs_id")
    void setEncodedFigsId(String encodedFigsId) {
        this.encodedFigsId = encodedFigsId;
    }

    String getEncodedFigsId() {
        return this.encodedFigsId;
    }

    @JsonProperty("count")
    void setCount(Integer count) {
        this.count = count;
    }

    Integer getCount() {
        return this.count;
    }

    @Override
    public String getStringForLogging() {
        return String.format(
                "GetPresignedUrlsRequest(db=%s, schema=%s, table=%s, count=%s, encodedFigsId=%s)",
                dbName, schemaName, tableName, count, encodedFigsId);
    }
}

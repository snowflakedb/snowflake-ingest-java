package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RefreshTableInformationRequest implements IStreamingIngestRequest {
  @JsonProperty("database")
  private String dbName;

  @JsonProperty("schema")
  private String schemaName;

  @JsonProperty("table")
  private String tableName;

  @JsonProperty("role")
  private String role;

  @JsonProperty("is_iceberg")
  private boolean isIceberg;

  public RefreshTableInformationRequest(TableRef tableRef, String role, boolean isIceberg) {
    this.dbName = tableRef.dbName;
    this.schemaName = tableRef.schemaName;
    this.tableName = tableRef.tableName;
    this.role = role;
    this.isIceberg = isIceberg;
  }

  String getDBName() {
    return this.dbName;
  }

  String getSchemaName() {
    return this.schemaName;
  }

  String getTableName() {
    return this.tableName;
  }

  String getRole() {
    return this.role;
  }

  boolean getIsIceberg() {
    return this.isIceberg;
  }

  @Override
  public String getStringForLogging() {
    return String.format(
        "RefreshTableInformation(db=%s, schema=%s, table=%s, role=%s, isIceberg=%s)",
        dbName, schemaName, tableName, role, isIceberg);
  }
}

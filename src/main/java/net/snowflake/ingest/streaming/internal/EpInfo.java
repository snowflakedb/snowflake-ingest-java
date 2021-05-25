package net.snowflake.ingest.streaming.internal;

import java.util.Map;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonProperty;

/** Class used to serialize/deserialize EP information */
class EpInfo {

  private long rowCount;

  private Map<String, FileColumnProperties> columnEps;

  /** Default constructor, needed for Jackson */
  EpInfo() {}

  EpInfo(long rowCount, Map<String, FileColumnProperties> columnEps) {
    this.rowCount = rowCount;
    this.columnEps = columnEps;
  }

  @JsonProperty("rows")
  long getRowCount() {
    return rowCount;
  }

  @JsonProperty("rows")
  void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  @JsonProperty("columns")
  Map<String, FileColumnProperties> getColumnEps() {
    return columnEps;
  }

  @JsonProperty("columns")
  void setColumnEps(Map<String, FileColumnProperties> columnEps) {
    this.columnEps = columnEps;
  }
}

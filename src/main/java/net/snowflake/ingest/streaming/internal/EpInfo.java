package net.snowflake.ingest.streaming.internal;

import java.util.Map;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonProperty;

/** Class used to serialize/deserialize EP information */
public class EpInfo {

  private long rowCount;

  private Map<String, FileColumnProperties> columnEps;

  /** Default constructor, needed for Jackson */
  public EpInfo() {}

  public EpInfo(long rowCount, Map<String, FileColumnProperties> columnEps) {
    this.rowCount = rowCount;
    this.columnEps = columnEps;
  }

  @JsonProperty("rows")
  public long getRowCount() {
    return rowCount;
  }

  @JsonProperty("rows")
  public void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  @JsonProperty("columns")
  public Map<String, FileColumnProperties> getColumnEps() {
    return columnEps;
  }

  @JsonProperty("columns")
  public void setColumnEps(Map<String, FileColumnProperties> columnEps) {
    this.columnEps = columnEps;
  }
}

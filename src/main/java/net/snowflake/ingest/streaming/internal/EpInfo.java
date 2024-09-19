package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

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

  /** Some basic verification logic to make sure the EP info is correct */
  public void verifyEpInfo() {
    for (Map.Entry<String, FileColumnProperties> entry : columnEps.entrySet()) {
      String colName = entry.getKey();
      FileColumnProperties colEp = entry.getValue();
      // Make sure the null count should always smaller than the total row count
      if (colEp.getNullCount() > rowCount) {
        throw new SFException(
            ErrorCode.INTERNAL_ERROR,
            String.format(
                "Null count bigger than total row count on col=%s, nullCount=%s, rowCount=%s",
                colName, colEp.getNullCount(), rowCount));
      }
    }
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

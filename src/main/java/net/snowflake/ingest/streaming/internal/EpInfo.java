package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.EP_NDV_UNKNOWN;

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

      // Make sure the NDV should always be -1
      if (colEp.getDistinctValues() != EP_NDV_UNKNOWN) {
        throw new SFException(
            ErrorCode.INTERNAL_ERROR,
            String.format(
                "Unexpected NDV on col=%s, value=%d", colName, colEp.getDistinctValues()));
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

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

  private boolean enableDistinctValuesCount;

  /** Default constructor, needed for Jackson */
  EpInfo() {}

  EpInfo(
      long rowCount,
      Map<String, FileColumnProperties> columnEps,
      boolean enableDistinctValuesCount) {
    this.rowCount = rowCount;
    this.columnEps = columnEps;
    this.enableDistinctValuesCount = enableDistinctValuesCount;
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

      // Make sure the NDV should always be -1 when the NDV set to default
      if (!enableDistinctValuesCount && colEp.getDistinctValues() != EP_NDV_UNKNOWN) {
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

/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/util/TimeMeasurement.java
 *
 * Permitted differences: package. @SnowflakeJdbcInternalApi removed.
 * SnowflakeUtil.getEpochTimeInMicroSeconds → StorageClientUtil.getEpochTimeInMicroSeconds.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

/** Class keeping the start and stop time in epoch microseconds. */
public class TimeMeasurement {
  private long start;
  private long end;

  /**
   * Get the start time as epoch time in microseconds.
   *
   * @return the start time as epoch time in microseconds.
   */
  public long getStart() {
    return start;
  }

  /** Set the start time as current epoch time in microseconds. */
  public void setStart() {
    this.start = StorageClientUtil.getEpochTimeInMicroSeconds();
  }

  /**
   * Get the stop time as epoch time in microseconds.
   *
   * @return the stop time as epoch time in microseconds.
   */
  public long getEnd() {
    return end;
  }

  /** Set the stop time as current epoch time in microseconds. */
  public void setEnd() {
    this.end = StorageClientUtil.getEpochTimeInMicroSeconds();
  }

  /**
   * Get the microseconds between the stop and start time.
   *
   * @return difference between stop and start in microseconds. If one of the variables is not
   *     initialized, it returns -1
   */
  public long getTime() {
    if (start == 0 || end == 0) {
      return -1;
    }

    return end - start;
  }
}

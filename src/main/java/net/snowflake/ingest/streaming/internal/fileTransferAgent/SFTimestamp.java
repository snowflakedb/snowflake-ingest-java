/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/util/SFTimestamp.java
 *
 * Permitted differences: package.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class SFTimestamp {
  /**
   * Get current time in UTC in the following format
   *
   * @return String representation in this format: yyyy-MM-dd HH:mm:ss
   */
  public static String getUTCNow() {
    SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT"));

    // Time in GMT
    return dateFormatGmt.format(new Date());
  }
}

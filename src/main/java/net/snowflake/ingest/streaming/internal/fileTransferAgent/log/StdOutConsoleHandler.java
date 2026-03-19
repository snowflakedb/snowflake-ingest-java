/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/log/StdOutConsoleHandler.java
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent.log;

import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

class StdOutConsoleHandler extends StreamHandler {
  public StdOutConsoleHandler() {
    // configure with specific defaults for ConsoleHandler
    super(System.out, new SimpleFormatter());
  }

  @Override
  public void publish(LogRecord record) {
    super.publish(record);
    flush();
  }

  @Override
  public void close() {
    flush();
  }
}

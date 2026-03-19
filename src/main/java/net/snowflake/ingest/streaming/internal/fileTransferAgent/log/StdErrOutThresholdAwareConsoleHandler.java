/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/log/StdErrOutThresholdAwareConsoleHandler.java
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent.log;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

class StdErrOutThresholdAwareConsoleHandler extends StreamHandler {
  private final ConsoleHandler stdErrConsoleHandler = new ConsoleHandler();
  private final Level threshold;

  public StdErrOutThresholdAwareConsoleHandler(Level threshold) {
    super(System.out, new SimpleFormatter());
    this.threshold = threshold;
  }

  @Override
  public void publish(LogRecord record) {
    if (record.getLevel().intValue() > threshold.intValue()) {
      stdErrConsoleHandler.publish(record);
    } else {
      super.publish(record);
      flush();
    }
  }

  @Override
  public void close() {
    flush();
    stdErrConsoleHandler.close();
  }

  Level getThreshold() {
    return threshold;
  }
}

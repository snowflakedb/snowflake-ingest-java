/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/log/UnknownJavaUtilLoggingLevelException.java
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent.log;

import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class UnknownJavaUtilLoggingLevelException extends RuntimeException {
  private static final String AVAILABLE_LEVELS =
      Stream.of(
              Level.OFF,
              Level.SEVERE,
              Level.WARNING,
              Level.INFO,
              Level.CONFIG,
              Level.FINE,
              Level.FINER,
              Level.FINEST,
              Level.ALL)
          .map(Level::getName)
          .collect(Collectors.joining(", "));

  UnknownJavaUtilLoggingLevelException(String threshold) {
    super(
        "Unknown java util logging level: " + threshold + ", expected one of: " + AVAILABLE_LEVELS);
  }
}

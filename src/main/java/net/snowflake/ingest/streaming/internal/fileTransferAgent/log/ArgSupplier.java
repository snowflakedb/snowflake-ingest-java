/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/log/ArgSupplier.java
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent.log;

/**
 * An interface for representing lambda expressions that supply values to placeholders in message
 * formats.
 *
 * <p>E.g., {@code Logger.debug("Value: {}", (ArgSupplier) () -> getValue());}
 */
@FunctionalInterface
public interface ArgSupplier {
  /**
   * Get value
   *
   * @return Object value.
   */
  Object get();
}

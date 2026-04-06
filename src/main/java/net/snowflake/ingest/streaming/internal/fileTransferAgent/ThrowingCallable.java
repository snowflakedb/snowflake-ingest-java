/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/util/ThrowingCallable.java
 *
 * Permitted differences: package. @SnowflakeJdbcInternalApi removed.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

@FunctionalInterface
public interface ThrowingCallable<A, T extends Throwable> {
  A call() throws T;
}

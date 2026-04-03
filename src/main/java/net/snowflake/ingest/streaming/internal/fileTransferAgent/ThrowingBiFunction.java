/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/util/ThrowingBiFunction.java
 *
 * Permitted differences: package declaration, @SnowflakeJdbcInternalApi annotation removed.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

@FunctionalInterface
public interface ThrowingBiFunction<A, B, R, T extends Throwable> {
  R apply(A a, B b) throws T;
}

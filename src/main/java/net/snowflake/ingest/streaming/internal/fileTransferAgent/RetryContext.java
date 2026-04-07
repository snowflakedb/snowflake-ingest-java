/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/RetryContext.java
 *
 * Permitted differences: package declaration, @SnowflakeJdbcInternalApi annotation removed.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

/** RetryContext stores information about an ongoing request's retrying process. */
public class RetryContext {
  static final int SECONDS_TO_MILLIS_FACTOR = 1000;
  private long elapsedTimeInMillis;
  private long retryTimeoutInMillis;
  private long retryCount;

  public RetryContext() {}

  public RetryContext setElapsedTimeInMillis(long elapsedTimeInMillis) {
    this.elapsedTimeInMillis = elapsedTimeInMillis;
    return this;
  }

  public RetryContext setRetryTimeoutInMillis(long retryTimeoutInMillis) {
    this.retryTimeoutInMillis = retryTimeoutInMillis;
    return this;
  }

  public RetryContext setRetryCount(long retryCount) {
    this.retryCount = retryCount;
    return this;
  }

  private long getRemainingRetryTimeoutInMillis() {
    return retryTimeoutInMillis - elapsedTimeInMillis;
  }

  public long getRemainingRetryTimeoutInSeconds() {
    return (getRemainingRetryTimeoutInMillis()) / SECONDS_TO_MILLIS_FACTOR;
  }

  public long getRetryCount() {
    return retryCount;
  }
}

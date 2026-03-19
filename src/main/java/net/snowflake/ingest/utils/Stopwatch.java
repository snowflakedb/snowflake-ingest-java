/*
 * Replicated from snowflake-jdbc: net.snowflake.client.util.Stopwatch
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/util/Stopwatch.java
 */

package net.snowflake.ingest.utils;

public class Stopwatch {
  private boolean isStarted = false;
  private long startTime;
  private long elapsedTime;

  public void start() {
    if (this.isStarted) {
      throw new IllegalStateException("Stopwatch is already running");
    }
    this.isStarted = true;
    this.startTime = System.nanoTime();
  }

  public void stop() {
    if (!this.isStarted) {
      if (this.startTime == 0L) {
        throw new IllegalStateException("Stopwatch has not been started");
      }
      throw new IllegalStateException("Stopwatch is already stopped");
    }
    this.isStarted = false;
    this.elapsedTime = System.nanoTime() - this.startTime;
  }

  public void reset() {
    this.isStarted = false;
    this.startTime = 0L;
    this.elapsedTime = 0L;
  }

  public void restart() {
    this.isStarted = true;
    this.startTime = System.nanoTime();
    this.elapsedTime = 0L;
  }

  public long elapsedMillis() {
    return this.elapsedNanos() / 1_000_000L;
  }

  public long elapsedNanos() {
    if (this.isStarted) {
      return System.nanoTime() - this.startTime;
    }
    if (this.startTime == 0L) {
      throw new IllegalStateException("Stopwatch has not been started");
    }
    return this.elapsedTime;
  }

  public boolean isStarted() {
    return this.isStarted;
  }
}

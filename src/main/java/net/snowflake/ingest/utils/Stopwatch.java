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
    if (isStarted) {
      throw new IllegalStateException("Stopwatch is already running");
    }

    isStarted = true;
    startTime = System.nanoTime();
  }

  public void stop() {
    if (!isStarted) {
      if (startTime == 0) {
        throw new IllegalStateException("Stopwatch has not been started");
      }
      throw new IllegalStateException("Stopwatch is already stopped");
    }

    isStarted = false;
    elapsedTime = System.nanoTime() - startTime;
  }

  public void reset() {
    isStarted = false;
    startTime = 0;
    elapsedTime = 0;
  }

  public void restart() {
    isStarted = true;
    startTime = System.nanoTime();
    elapsedTime = 0;
  }

  public long elapsedMillis() {
    return elapsedNanos() / 1_000_000;
  }

  public long elapsedNanos() {
    if (isStarted) {
      return (System.nanoTime() - startTime);
    }
    if (startTime == 0) {
      throw new IllegalStateException("Stopwatch has not been started");
    }
    return elapsedTime;
  }

  public boolean isStarted() {
    return isStarted;
  }
}

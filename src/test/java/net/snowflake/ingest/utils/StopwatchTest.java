/*
 * Ported from snowflake-jdbc: net.snowflake.client.util.StopwatchTest
 */

package net.snowflake.ingest.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class StopwatchTest {

  @Test
  public void testStartStop() {
    Stopwatch sw = new Stopwatch();
    assertFalse(sw.isStarted());
    sw.start();
    assertTrue(sw.isStarted());
    sw.stop();
    assertFalse(sw.isStarted());
    assertTrue(sw.elapsedNanos() >= 0);
    assertTrue(sw.elapsedMillis() >= 0);
  }

  @Test
  public void testReset() {
    Stopwatch sw = new Stopwatch();
    sw.start();
    sw.stop();
    sw.reset();
    assertFalse(sw.isStarted());
  }

  @Test
  public void testRestart() {
    Stopwatch sw = new Stopwatch();
    sw.start();
    sw.stop();
    sw.restart();
    assertTrue(sw.isStarted());
    sw.stop();
    assertTrue(sw.elapsedNanos() >= 0);
  }

  @Test(expected = IllegalStateException.class)
  public void testDoubleStart() {
    Stopwatch sw = new Stopwatch();
    sw.start();
    sw.start();
  }

  @Test(expected = IllegalStateException.class)
  public void testStopWithoutStart() {
    Stopwatch sw = new Stopwatch();
    sw.stop();
  }

  @Test(expected = IllegalStateException.class)
  public void testElapsedWithoutStart() {
    Stopwatch sw = new Stopwatch();
    sw.elapsedNanos();
  }
}

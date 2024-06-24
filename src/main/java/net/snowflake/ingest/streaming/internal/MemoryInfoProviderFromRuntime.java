/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Reads memory information from JVM runtime */
public class MemoryInfoProviderFromRuntime implements MemoryInfoProvider {
  private final long maxMemory;
  private volatile long totalFreeMemory;
  private final ScheduledExecutorService executorService;

  public MemoryInfoProviderFromRuntime(long freeMemoryUpdateIntervalMs) {
    maxMemory = Runtime.getRuntime().maxMemory();
    totalFreeMemory = Runtime.getRuntime().freeMemory() + (maxMemory - Runtime.getRuntime().totalMemory());
    executorService = new ScheduledThreadPoolExecutor(1, r -> {
        Thread th = new Thread(r, "MemoryInfoProviderFromRuntime");
        th.setDaemon(true);
        return th;
    });
    executorService.scheduleAtFixedRate(this::updateFreeMemory, 0, freeMemoryUpdateIntervalMs, TimeUnit.MILLISECONDS);
  }

  private void updateFreeMemory() {
    totalFreeMemory = Runtime.getRuntime().freeMemory() + (maxMemory - Runtime.getRuntime().totalMemory());
  }

  @Override
  public long getMaxMemory() {
    return maxMemory;
  }

  @Override
  public long getFreeMemory() {
    return totalFreeMemory;
  }
}

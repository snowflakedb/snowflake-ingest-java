/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/** Reads memory information from JVM runtime */
public class MemoryInfoProviderFromRuntime implements MemoryInfoProvider {
  @Override
  public long getMaxMemory() {
    return Runtime.getRuntime().maxMemory();
  }

  @Override
  public long getTotalMemory() {
    return Runtime.getRuntime().totalMemory();
  }

  @Override
  public long getFreeMemory() {
    return Runtime.getRuntime().freeMemory();
  }
}

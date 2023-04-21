/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/** Provider information about available system memory */
public interface MemoryInfoProvider {
  long getMaxMemory();

  long getTotalMemory();

  long getFreeMemory();
}

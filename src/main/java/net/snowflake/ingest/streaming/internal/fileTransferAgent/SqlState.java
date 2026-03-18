/*
 * Copyright (c) 2024-2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

/** Standard SQL state code constants used by the storage clients. */
public final class SqlState {
  public static final String SYSTEM_ERROR = "58000";
  public static final String INTERNAL_ERROR = "XX000";

  private SqlState() {}
}

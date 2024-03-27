/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

abstract class StreamingIngestResponse {
  abstract String getMessage();

  abstract Long getStatusCode();
}

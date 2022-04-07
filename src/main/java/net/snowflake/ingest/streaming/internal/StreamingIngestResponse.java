/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/** Response to the OpenChannelRequest */
abstract class StreamingIngestResponse {
  abstract Long getStatusCode();
}

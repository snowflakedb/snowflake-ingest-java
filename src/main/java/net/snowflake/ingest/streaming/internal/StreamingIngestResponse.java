/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/**
 * The StreamingIngestResponse class is an abstract class that represents a response from the
 * Snowflake streaming ingest API. This class provides a common structure for all types of responses
 * that can be received from the {@link SnowflakeServiceClient}.
 */
abstract class StreamingIngestResponse {
  abstract Long getStatusCode();

  abstract String getMessage();
}

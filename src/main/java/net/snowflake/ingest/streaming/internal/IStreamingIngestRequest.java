/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * The StreamingIngestRequest interface is a marker interface used for type safety in the {@link
 * SnowflakeServiceClient} for streaming ingest API request.
 */
interface IStreamingIngestRequest {
  @JsonIgnore
  String getStringForLogging();
}

/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.TimeUnit;

/** Latency stats for a blob */
class BlobStats {
  // flush duration cannot be calculated in the client sdk, so we pass just the start time
  private long flushStartMs;

  private long buildDurationMs;
  private long uploadDurationMs;

  @JsonProperty("build_duration_ms")
  long getBuildDurationMs() {
    return this.buildDurationMs;
  }

  @JsonProperty("build_duration_ms")
  void setBuildDurationMs(Timer.Context buildLatencyContext) {
    if (buildLatencyContext != null) {
      this.buildDurationMs = TimeUnit.NANOSECONDS.toMillis(buildLatencyContext.stop());
    }
  }

  @JsonProperty("upload_duration_ms")
  long getUploadDurationMs() {
    return this.uploadDurationMs;
  }

  @JsonProperty("upload_duration_ms")
  void setUploadDurationMs(Timer.Context uploadLatencyContext) {
    if (uploadLatencyContext != null) {
      this.uploadDurationMs = TimeUnit.NANOSECONDS.toMillis(uploadLatencyContext.stop());
    }
  }

  @JsonProperty("flush_start_ms")
  long getFlushStartMs() {
    return this.flushStartMs;
  }

  @JsonProperty("flush_start_ms")
  void setFlushStartMs(long flushStartMs) {
    this.flushStartMs = flushStartMs;
  }
}

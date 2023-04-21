/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.TimeUnit;

/** Latency information for a blob */
class BlobStats {
  private long buildDurationMs;
  private long uploadDurationMs;

  // flush and register duration cannot be calculated in the client sdk we pass the start time
  // because the end time is when the request hits the server
  private long flushStartMs;
  private long registerStartMs;

  @JsonProperty("build_duration_ms")
  long getBuildDurationMs() {
    return this.buildDurationMs;
  }

  @JsonProperty("upload_duration_ms")
  long getUploadDurationMs() {
    return this.uploadDurationMs;
  }

  @JsonProperty("flush_start_ms")
  long getFlushStartMs() {
    return this.flushStartMs;
  }

  @JsonProperty("register_start_ms")
  long getRegisterStartMs() {
    return this.registerStartMs;
  }

  void setBuildDurationMs(Timer.Context buildLatencyContext) {
    if (buildLatencyContext != null) {
      this.buildDurationMs = TimeUnit.NANOSECONDS.toMillis(buildLatencyContext.stop());
    }
  }

  void setUploadDurationMs(Timer.Context uploadLatencyContext) {
    if (uploadLatencyContext != null) {
      this.uploadDurationMs = TimeUnit.NANOSECONDS.toMillis(uploadLatencyContext.stop());
    }
  }

  void setFlushStartMs(long flushStartMs) {
    this.flushStartMs = flushStartMs;
  }

  void setRegisterStartMs(long registerStartMs) {
    this.registerStartMs = registerStartMs;
  }
}

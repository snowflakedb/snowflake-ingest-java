/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.TimeUnit;

/** Latency information for a blob */
class BlobLatencies {
  public static final Long DEFAULT_BLOB_LATENCY = null;

  private Long buildDurationMs;
  private Long uploadDurationMs;

  private Long flushStartMs;
  private Long registerStartMs;

  public BlobLatencies() {
    this.buildDurationMs = DEFAULT_BLOB_LATENCY;
    this.uploadDurationMs = DEFAULT_BLOB_LATENCY;

    this.flushStartMs = DEFAULT_BLOB_LATENCY;
    this.registerStartMs = DEFAULT_BLOB_LATENCY;
  }

  @JsonProperty("build_latency_ms")
  long getBuildDurationMs() {
    return this.buildDurationMs;
  }

  @JsonProperty("upload_latency_ms")
  long getUploadDurationMs() {
    return this.uploadDurationMs;
  }

  @JsonProperty("flush_start_timestamp")
  long getFlushStartMs() {
    return this.flushStartMs;
  }

  @JsonProperty("register_start_timestamp")
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

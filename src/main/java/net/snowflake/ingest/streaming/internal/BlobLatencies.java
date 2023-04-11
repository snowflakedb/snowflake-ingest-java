/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Latency information for a blob
 */
class BlobLatencies {
    public static final long DEFAULT_BLOB_LATENCY = -1;

    private long buildLatencyMs;
    private long uploadLatencyMs;

    private long flushStartTimestamp;
    private long registerStartTimestamp;

    public BlobLatencies() {
        this.buildLatencyMs = DEFAULT_BLOB_LATENCY;
        this.uploadLatencyMs = DEFAULT_BLOB_LATENCY;

        this.flushStartTimestamp = DEFAULT_BLOB_LATENCY;
        this.registerStartTimestamp = DEFAULT_BLOB_LATENCY;
    }

    @JsonProperty("build_latency_ms")
    long getBuildLatencyMs() {
        return this.buildLatencyMs;
    }

    @JsonProperty("upload_latency_ms")
    long getUploadLatencyMs() {
        return this.uploadLatencyMs;
    }

    @JsonProperty("flush_start_timestamp")
    long getFlushStartTimestamp() {
        return this.flushStartTimestamp;
    }

    @JsonProperty("register_start_timestamp")
    long getRegisterStartTimestamp() {
        return this.registerStartTimestamp;
    }

    void setBuildLatencyMs(Timer.Context buildLatencyContext) {
        if (buildLatencyContext != null) {
            this.buildLatencyMs = buildLatencyContext.stop();
        }
    }

    void setUploadLatencyMs(Timer.Context uploadLatencyContext) {
        if (uploadLatencyContext != null) {
            this.uploadLatencyMs = uploadLatencyContext.stop();
        }
    }

    void setFlushStartTimestamp(long flushStartTimestamp) {
        this.flushStartTimestamp = flushStartTimestamp;
    }

    void setRegisterStartTimestamp(long registerStartTimestamp) {
        this.registerStartTimestamp = registerStartTimestamp;
    }
}

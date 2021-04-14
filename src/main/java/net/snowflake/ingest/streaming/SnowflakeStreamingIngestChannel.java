/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

/**
 * A logical partition that represents a connection to a single Snowflake table, data will be
 * ingested into the channel, and then flush to Snowflake table periodically in the background. Note
 * that only one client (or thread) could write to a channel at a given time, so if there are
 * multiple clients (or multiple threads in the same client) try to ingest using the same channel,
 * the latest client (or thread) that opens the channel will win and all the other opened channels
 * will be invalid
 */
public interface SnowflakeStreamingIngestChannel {}

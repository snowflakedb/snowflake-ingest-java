/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

/**
 * The first version of implementation for SnowflakeStreamingIngestClient. The client internally
 * manages a few things:
 * <li>the channel cache, which contains all the channels that belong to this account
 * <li>the flush service, which schedules and coordinates the flush to Snowflake tables
 */
public class SnowflakeStreamingIngestClientV1 implements SnowflakeStreamingIngestClient {}

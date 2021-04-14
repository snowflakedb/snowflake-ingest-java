/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/**
 * Responsible for flushing data from client to Snowflake tables. When a flush is triggered, it will
 * do the following in order:
 * <li>take the data from all the channel row buffers
 * <li>build the blob using the ChannelData returned from previous step
 * <li>upload the blob to stage
 * <li>register the blob to the targeted Snowflake table
 */
public class FlushService {}

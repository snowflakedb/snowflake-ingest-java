/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/**
 * In-memory cache that stores the active channels for a given Streaming Ingest client, and the
 * channels belong to the same table will be stored together in order to combine the channel data
 * during flush. The key is a fully qualified table name and the value is a set of channels that
 * belongs to this table
 */
public class ChannelCache {}

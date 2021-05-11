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
public interface SnowflakeStreamingIngestChannel {
  /**
   * Get the fully qualified channel name, the format will be
   * dbName.schemaName.tableName.channelName
   *
   * @return fully qualified name of the channel
   */
  String getFullyQualifiedName();

  /**
   * Get the name of the channel
   *
   * @return name of the channel
   */
  String getName();

  /**
   * Get the database name
   *
   * @return
   */
  String getDBName();

  /**
   * Get the schema name
   *
   * @return
   */
  String getSchemaName();

  /**
   * Get the table name
   *
   * @return
   */
  String getTableName();

  /**
   * Get the fully qualified table name that the channel belongs to
   *
   * @return fully qualified table name
   */
  String getFullyQualifiedTableName();

  /** @return a boolean to indicate whether the channel is valid or not */
  boolean isValid();

  /** @return a boolean to indicate whether the channel is closed or not */
  boolean isClosed();
}

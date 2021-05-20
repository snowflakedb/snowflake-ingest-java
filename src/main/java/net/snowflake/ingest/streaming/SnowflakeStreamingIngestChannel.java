/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

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

  /**
   * Flush all data in memory to persistent storage and register with a Snowflake table
   *
   * @return future which will be complete when the flush the data is registered
   */
  CompletableFuture<Void> flush();

  /**
   * Close the channel (this will flush in-flight buffered data)
   *
   * @return future which will be complete when the channel is closed
   */
  CompletableFuture<Void> close();

  /**
   * --------------------------------------------------------------------------------------------
   * Insert one row into the channel
   * --------------------------------------------------------------------------------------------
   */

  /**
   * The row is represented using Map where the key is column name and the value is data row
   *
   * @param row object data to write
   * @param offsetToken offset of given row, used for replay in case of failures. It could be null
   *     if you don't plan on replaying or can't replay
   */
  void insertRow(Map<String, Object> row, @Nullable String offsetToken);

  /**
   * --------------------------------------------------------------------------------------------
   * Insert a batch of rows into the channel
   * --------------------------------------------------------------------------------------------
   */

  /**
   * Each row is represented using Map where the key is column name and the value is data row
   *
   * @param rows object data to write
   * @param offsetToken offset of last row in the row-set, used for replay in case of failures, It
   *     could be null * if you don't plan on replaying or can't replay
   */
  void insertRows(Iterable<Map<String, Object>> rows, @Nullable String offsetToken);
}

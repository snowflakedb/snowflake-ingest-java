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
   * Get the fully qualified channel name
   *
   * @return fully qualified name of the channel, in the format of
   *     dbName.schemaName.tableName.channelName
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
   * @return name of the database
   */
  String getDBName();

  /**
   * Get the schema name
   *
   * @return name of the schema
   */
  String getSchemaName();

  /**
   * Get the table name
   *
   * @return name of the table
   */
  String getTableName();

  /**
   * Get the fully qualified table name that the channel belongs to
   *
   * @return fully qualified table name, in the format of dbName.schemaName.tableName
   */
  String getFullyQualifiedTableName();

  /** @return a boolean which indicates whether the channel is valid */
  boolean isValid();

  /** @return a boolean which indicates whether the channel is closed */
  boolean isClosed();

  /**
   * Close the channel, this function will make sure all the data in this channel is committed
   *
   * @return a completable future which will be completed when the channel is closed
   */
  CompletableFuture<Void> close();

  /**
   * Insert one row into the channel, the row is represented using Map where the key is column name
   * and the value is a row of data
   *
   * @param row object data to write
   * @param offsetToken offset of given row, used for replay in case of failures. It could be null
   *     if you don't plan on replaying or can't replay
   * @return insert response that possibly contains errors because of insertion failures
   */
  InsertValidationResponse insertRow(Map<String, Object> row, @Nullable String offsetToken);

  /**
   * Insert a batch of rows into the channel, each row is represented using Map where the key is
   * column name and the value is a row of data
   *
   * @param rows object data to write
   * @param offsetToken offset of last row in the row-set, used for replay in case of failures, It
   *     could be null if you don't plan on replaying or can't replay
   * @return insert response that possibly contains errors because of insertion failures
   */
  InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, @Nullable String offsetToken);

  /**
   * Get the latest committed offset token from Snowflake
   *
   * @return the latest committed offset token
   */
  String getLatestCommittedOffsetToken();
}

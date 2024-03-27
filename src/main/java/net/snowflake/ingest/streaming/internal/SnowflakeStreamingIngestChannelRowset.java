/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Logging;

/** The implementation for Rowset API channel */
public class SnowflakeStreamingIngestChannelRowset implements SnowflakeStreamingIngestChannel {

  private static final Logging logger = new Logging(SnowflakeStreamingIngestChannelRowset.class);

  private final String dbName;

  private final String schemaName;

  private final String pipeName;

  private final String tableName;

  private final String name;

  private final String fullyQualifiedName;

  private boolean isClosed;

  private boolean isValid;

  private String offsetToken;

  private String continuationToken;

  SnowflakeStreamingIngestChannelRowset(
      String name,
      String dbName,
      String schemaName,
      String pipeName,
      String tableName,
      String offsetToken,
      String continuationToken) {
    this.dbName = dbName;
    this.schemaName = schemaName;
    this.pipeName = pipeName;
    this.tableName = tableName;
    this.name = name;
    this.fullyQualifiedName =
        String.format(
            "%s.%s.%s.%s", dbName, schemaName, pipeName == null ? tableName : pipeName, name);
    this.offsetToken = offsetToken;
    this.continuationToken = continuationToken;
  }

  /**
   * Get the fully qualified channel name
   *
   * @return fully qualified name of the channel, in the format of
   *     dbName.schemaName.pipeName.channelName or dbName.schemaName.tableName.channelName
   */
  @Override
  public String getFullyQualifiedName() {
    return this.fullyQualifiedName;
  }

  /**
   * Get the name of the channel
   *
   * @return name of the channel
   */
  @Override
  public String getName() {
    return this.name;
  }

  /**
   * Get the database name
   *
   * @return name of the database
   */
  @Override
  public String getDBName() {
    return this.dbName;
  }

  /**
   * Get the schema name
   *
   * @return name of the schema
   */
  @Override
  public String getSchemaName() {
    return this.schemaName;
  }

  /**
   * Get the table name
   *
   * @return name of the table
   */
  @Override
  public String getTableName() {
    return this.tableName;
  }

  /**
   * Get the name of the table or pipe based on the ownership of the channel (either a table or
   * pipe)
   *
   * @return name of the table or pipe
   */
  @Override
  public String getTableOrPipeName() {
    return this.pipeName != null ? this.pipeName : this.tableName;
  }

  /**
   * Get the fully qualified table name that the channel belongs to
   *
   * @return fully qualified table name, in the format of dbName.schemaName.tableName
   */
  @Override
  public String getFullyQualifiedTableName() {
    return this.fullyQualifiedName;
  }

  /**
   * Get the fully qualified table or pipe name that the channel belongs to
   *
   * @return fully qualified table or pipe name
   */
  @Override
  public String getFullyQualifiedTableOrPipeName() {
    return String.format(
        "%s.%s.%s", this.getDBName(), this.getSchemaName(), this.getTableOrPipeName());
  }

  /** @return a boolean which indicates whether the channel is valid */
  @Override
  public boolean isValid() {
    return this.isValid;
  }

  /** @return a boolean which indicates whether the channel is closed */
  @Override
  public boolean isClosed() {
    return this.isClosed;
  }

  /**
   * Close the channel, this function will make sure all the data in this channel is committed
   *
   * @return a completable future which will be completed when the channel is closed
   */
  @Override
  public CompletableFuture<Void> close() {
    this.isClosed = true;
    return null;
  }

  /**
   * Close the channel, this function will make sure all the data in this channel is committed
   *
   * <p>Note that this call with drop=true will delete <a
   * href=https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#offset-tokens>Offset
   * Token</a> and other state from Snowflake servers unless the channel has already been opened in
   * another client. So only use it if you are completely done ingesting data for this channel. If
   * you open a channel with the same name in the future, it will behave like a new channel.
   *
   * @param drop if true, the channel will be dropped after all data is successfully committed.
   * @return a completable future which will be completed when the channel is closed
   */
  @Override
  public CompletableFuture<Void> close(boolean drop) {
    this.isClosed = true;
    return null;
  }

  /**
   * Insert one row into the channel, the row is represented using Map where the key is column name
   * and the value is a row of data.
   *
   * @param row object data to write. For predictable results, we recommend not to concurrently
   *     modify the input row data.
   * @param offsetToken offset of given row, used for replay in case of failures. It could be null
   *     if you don't plan on replaying or can't replay
   * @return insert response that possibly contains errors because of insertion failures
   */
  @Override
  public InsertValidationResponse insertRow(Map<String, Object> row, @Nullable String offsetToken) {
    return null;
  }

  @Override
  public InsertValidationResponse insertRow(Map<String, Object> row) {
    return null;
  }

  /**
   * Insert a batch of rows into the channel, each row is represented using Map where the key is
   * column name and the value is a row of data. See {@link
   * SnowflakeStreamingIngestChannel#insertRow(Map, String)} for more information about accepted
   * values.
   *
   * @param rows object data to write
   * @param startOffsetToken start offset of the batch/row-set
   * @param endOffsetToken end offset of the batch/row-set, used for replay in case of failures, *
   *     It could be null if you don't plan on replaying or can't replay
   * @return insert response that possibly contains errors because of insertion failures
   */
  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows,
      @Nullable String startOffsetToken,
      @Nullable String endOffsetToken) {
    throw new UnsupportedOperationException();
  }

  /**
   * Insert a batch of rows into the channel with the end offset token only, please see {@link
   * SnowflakeStreamingIngestChannel#insertRows(Iterable, String, String)} for more information.
   *
   * @param rows
   * @param offsetToken
   */
  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, @Nullable String offsetToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InsertValidationResponse insertRows(Iterable<Map<String, Object>> rows) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the latest committed offset token from Snowflake
   *
   * @return the latest committed offset token
   */
  @Nullable
  @Override
  public String getLatestCommittedOffsetToken() {
    throw new UnsupportedOperationException();
  }

  /**
   * Gets the table schema associated with this channel. Note that this is the table schema at the
   * time of a channel open event. The schema may be changed on the Snowflake side in which case
   * this will continue to show an old schema version until the channel is re-opened.
   *
   * @return map representing Column Name to Column Properties
   */
  @Override
  public Map<String, ColumnProperties> getTableSchema() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the type of channel, please referring to {@link
   * net.snowflake.ingest.streaming.OpenChannelRequest.ChannelType} for the supported channel type
   *
   * @return type of the channel
   */
  @Override
  public OpenChannelRequest.ChannelType getType() {
    return OpenChannelRequest.ChannelType.ROWSET_API;
  }

  /** Get the latest continuation token */
  public String getContinuationToken() {
    return this.continuationToken;
  }
}

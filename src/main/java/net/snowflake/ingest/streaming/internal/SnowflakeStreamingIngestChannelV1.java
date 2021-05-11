/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.RootAllocator;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Logging;

/** The first version of implementation for SnowflakeStreamingIngestChannel */
public class SnowflakeStreamingIngestChannelV1 extends Logging
    implements SnowflakeStreamingIngestChannel {

  private final String channelName;
  private final String dbName;
  private final String schemaName;
  private final String tableName;
  private volatile String offsetToken;
  private final AtomicLong rowSequencer;

  // Sequencer for this channel, corresponding to client sequencer at server side because each
  // connection to a channel at server side will be seen as a connection from a new client
  private final Long channelSequencer;

  // Reference to the row buffer
  private final ArrowRowBuffer arrowBuffer;

  // Indicates whether the channel is still valid
  private volatile boolean isValid;

  // Indicates whether the channel is closed
  private volatile boolean isClosed;

  // Reference to the client that owns this channel
  private final SnowflakeStreamingIngestClientV1 owningClient;

  // Memory allocator
  private final BufferAllocator allocator;

  // Indicates whether we're using it as of the any tests
  private boolean isTestMode;

  /**
   * Constructor for TESTING ONLY which allows us to set the test mode
   *
   * @param name
   * @param dbName
   * @param schemaName
   * @param tableName
   * @param offsetToken
   * @param channelSequencer
   * @param rowSequencer
   * @param client
   * @param isTestMode
   */
  SnowflakeStreamingIngestChannelV1(
      String name,
      String dbName,
      String schemaName,
      String tableName,
      String offsetToken,
      Long channelSequencer,
      Long rowSequencer,
      SnowflakeStreamingIngestClientV1 client,
      boolean isTestMode) {
    this.channelName = name;
    this.dbName = dbName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.offsetToken = offsetToken;
    this.channelSequencer = channelSequencer;
    this.rowSequencer = new AtomicLong(rowSequencer);
    this.isValid = true;
    this.isClosed = false;
    this.owningClient = client;
    this.isTestMode = isTestMode;
    this.allocator =
        isTestMode || owningClient.isTestMode()
            ? new RootAllocator()
            : this.owningClient
                .getAllocator()
                .newChildAllocator(name, 0, this.owningClient.getAllocator().getLimit());
    this.arrowBuffer = new ArrowRowBuffer(this);
    logDebug("Channel {} created for table: {}", this.channelName, this.tableName);
  }

  /**
   * Default Constructor
   *
   * @param name
   * @param dbName
   * @param schemaName
   * @param tableName
   * @param offsetToken
   * @param channelSequencer
   * @param rowSequencer
   * @param client
   */
  public SnowflakeStreamingIngestChannelV1(
      String name,
      String dbName,
      String schemaName,
      String tableName,
      String offsetToken,
      Long channelSequencer,
      Long rowSequencer,
      SnowflakeStreamingIngestClientV1 client) {
    this(
        name,
        dbName,
        schemaName,
        tableName,
        offsetToken,
        channelSequencer,
        rowSequencer,
        client,
        false);
  }

  /**
   * Get the fully qualified channel name, the format will be
   * dbName.schemaName.tableName.channelName
   *
   * @return fully qualified name of the channel
   */
  @Override
  public String getFullyQualifiedName() {
    return String.format(
        "%s.%s.%s.%s", this.dbName, this.schemaName, this.tableName, this.channelName);
  }

  /**
   * Get the name of the channel
   *
   * @return name of the channel
   */
  @Override
  public String getName() {
    return this.channelName;
  }

  @Override
  public String getDBName() {
    return this.dbName;
  }

  @Override
  public String getSchemaName() {
    return this.schemaName;
  }

  @Override
  public String getTableName() {
    return this.tableName;
  }

  public String getOffsetToken() {
    return this.offsetToken;
  }

  public void setOffsetToken(String offsetToken) {
    this.offsetToken = offsetToken;
  }

  public Long getChannelSequencer() {
    return this.channelSequencer;
  }

  public long incrementAndGetRowSequencer() {
    return this.rowSequencer.incrementAndGet();
  }

  /**
   * Get the fully qualified table name that the channel belongs to
   *
   * @return fully qualified table name
   */
  @Override
  public String getFullyQualifiedTableName() {
    return String.format("%s.%s.%s", this.dbName, this.schemaName, this.tableName);
  }

  /**
   * Get all the data needed to build the blob during flush
   *
   * @return a ChannelData object
   */
  public ChannelData getData() {
    return this.arrowBuffer.flush();
  }

  /** @return a boolean to indicate whether the channel is valid or not */
  @Override
  public boolean isValid() {
    return this.isValid;
  }

  /** Mark the channel as invalid, and release resources */
  public void invalidate() {
    this.isValid = false;
    this.arrowBuffer.close();
    this.owningClient.removeChannel(this);
  }

  /** @return a boolean to indicate whether the channel is closed or not */
  @Override
  public boolean isClosed() {
    return this.isClosed;
  }

  /** Mark the channel as closed */
  public void markClosed() {
    this.isClosed = true;
  }

  /**
   * Get the buffer allocator
   *
   * @return the buffer allocator
   */
  public BufferAllocator getAllocator() {
    return this.allocator;
  }

  /**
   * Setup the column fields and vectors using the column metadata from the server
   *
   * @param columns
   */
  // TODO: need to verify with the table schema when supporting sub-columns
  public void setupSchema(List<ColumnMetadata> columns) {
    logDebug("Setup schema for channel: {}, schema: {}", getFullyQualifiedName(), columns);
    this.arrowBuffer.setupSchema(columns);
  }
}

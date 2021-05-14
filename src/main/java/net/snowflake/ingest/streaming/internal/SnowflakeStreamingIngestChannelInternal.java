/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.Constants.MAX_CHUNK_SIZE_IN_BYTES;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/** The first version of implementation for SnowflakeStreamingIngestChannel */
public class SnowflakeStreamingIngestChannelInternal implements SnowflakeStreamingIngestChannel {

  private static final Logging logger = new Logging(SnowflakeStreamingIngestChannelInternal.class);

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
  private final SnowflakeStreamingIngestClientInternal owningClient;

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
  SnowflakeStreamingIngestChannelInternal(
      String name,
      String dbName,
      String schemaName,
      String tableName,
      String offsetToken,
      Long channelSequencer,
      Long rowSequencer,
      SnowflakeStreamingIngestClientInternal client,
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
    logger.logDebug("Channel {} created for table: {}", this.channelName, this.tableName);
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
  public SnowflakeStreamingIngestChannelInternal(
      String name,
      String dbName,
      String schemaName,
      String tableName,
      String offsetToken,
      Long channelSequencer,
      Long rowSequencer,
      SnowflakeStreamingIngestClientInternal client) {
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
    this.owningClient.removeChannelIfSequencersMatch(this);
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
    logger.logDebug("Setup schema for channel: {}, schema: {}", getFullyQualifiedName(), columns);
    this.arrowBuffer.setupSchema(columns);
  }

  /**
   * --------------------------------------------------------------------------------------------
   * Insert one row into the channel
   * --------------------------------------------------------------------------------------------
   */

  /**
   * The row is represented using Map where the key is column name and the value is data row
   *
   * @param row object data to write
   * @param offsetToken offset of given row, used for replay in case of failures
   * @throws SFException when the channel is invalid or closed
   */
  @Override
  public void insertRow(Map<String, Object> row, String offsetToken) {
    insertRows(Collections.singletonList(row), offsetToken);
  }

  /**
   * --------------------------------------------------------------------------------------------
   * Insert a batch of rows into the channel
   * --------------------------------------------------------------------------------------------
   */

  /**
   * Each row is represented using Map where the key is column name and the value is data row
   *
   * @param rows object data to write
   * @param offsetToken offset of last row in the row-set, used for replay in case of failures
   * @throws SFException when the channel is invalid or closed
   */
  @Override
  public void insertRows(Iterable<Map<String, Object>> rows, String offsetToken) {
    if (!isValid()) {
      throw new SFException(ErrorCode.INVALID_CHANNEL);
    }

    if (isClosed()) {
      throw new SFException(ErrorCode.CLOSED_CHANNEL);
    }

    this.arrowBuffer.insertRows(rows, offsetToken);

    // Start flush task if the chunk size reaches a certain size
    // TODO: Checking table/chunk level size reduces throughput a lot, we may want to check it only
    // if a large number of rows are inserted
    if (this.arrowBuffer.getSize() >= MAX_CHUNK_SIZE_IN_BYTES) {
      this.owningClient.setNeedFlush();
    }
  }
}

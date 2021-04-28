/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.BufferAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.RootAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.*;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.types.Types;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.types.pojo.ArrowType;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.types.pojo.Field;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.types.pojo.FieldType;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.Text;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.TransferPair;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.StreamingUtils;

/**
 * The buffer in the Streaming Ingest channel that holds the un-flushed rows, these rows will be
 * converted to Arrow format for faster processing
 */
public class ArrowRowBuffer extends Logging {

  // Constants for column fields
  private static final String FIELD_EPOCH_IN_SECONDS = "epoch"; // seconds since epoch
  private static final String FIELD_TIME_ZONE = "timezone"; // time zone index
  private static final String FIELD_FRACTION_IN_NANOSECONDS = "fraction"; // fraction in nanoseconds

  // Column metadata that will send back to server as part of the blob, and will be used by the
  // Arrow reader
  private static final String COLUMN_PHYSICAL_TYPE = "physicalType";
  private static final String COLUMN_LOGICAL_TYPE = "logicalType";
  private static final String COLUMN_SCALE = "scale";
  private static final String COLUMN_PRECISION = "precision";
  private static final String COLUMN_CHAR_LENGTH = "charLength";
  private static final String COLUMN_BYTE_LENGTH = "byteLength";

  // Snowflake table column logical type
  private static enum ColumnLogicalType {
    ANY,
    BOOLEAN,
    ROWINDEX,
    NULL,
    REAL,
    FIXED,
    TEXT,
    CHAR,
    BINARY,
    DATE,
    TIME,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    INTERVAL,
    RAW,
    ARRAY,
    OBJECT,
    VARIANT,
    ROW,
    SEQUENCE,
    FUNCTION,
    USER_DEFINED_TYPE,
  }

  // Snowflake table column physical type
  private static enum ColumnPhysicalType {
    ROWINDEX,
    DOUBLE,
    SB1,
    SB2,
    SB4,
    SB8,
    SB16,
    LOB,
    BINARY,
    ROW,
  }

  // Map the column name to the Arrow vector (buffer)
  private final Map<String, FieldVector> vectors;

  // Map the column name to Arrow column field
  private final Map<String, Field> fields;

  // Lock used to protect the buffers from concurrent read/write
  private final Lock flushLock;

  // Current row count
  private volatile long rowCount;

  // Allocator used to allocate the buffers
  private final BufferAllocator allocator;

  // Current buffer size
  private volatile float bufferSize;

  // Current row index in the buffer
  private volatile int curRowIndex;

  // Reference to the Streaming Ingest channel that owns this buffer
  private final SnowflakeStreamingIngestChannel owningChannel;

  /**
   * Construct a ArrowRowBuffer object
   *
   * @param channel
   */
  public ArrowRowBuffer(SnowflakeStreamingIngestChannel channel) {
    this.owningChannel = channel;
    this.allocator = channel == null ? new RootAllocator() : channel.getAllocator();
    this.vectors = new HashMap<>();
    this.fields = new HashMap<>();
    this.flushLock = new ReentrantLock();
    this.rowCount = 0L;
    this.bufferSize = 0F;
    this.curRowIndex = 0;
  }

  /**
   * Setup the column fields and vectors using the column metadata from the server
   *
   * @param columns list of column metadata
   */
  public void setupSchema(List<ColumnMetadata> columns) {
    for (ColumnMetadata column : columns) {
      Field field = buildField(column);
      FieldVector vector = field.createVector(this.allocator);
      this.fields.put(column.getName(), field);
      this.vectors.put(column.getName(), vector);
    }
  }

  /**
   * Close the row buffer and release resources. Note that the caller needs to handle
   * synchronization
   */
  public void close() {
    this.vectors.values().forEach(vector -> vector.close());
    this.vectors.clear();
    this.fields.clear();
    this.allocator.close();
  }

  /** Reset the variables after each flush. Note that the caller needs to handle synchronization */
  private void reset() {
    this.vectors.values().forEach(vector -> vector.clear());
    this.rowCount = 0L;
    this.bufferSize = 0F;
    this.curRowIndex = 0;
  }

  /**
   * Get the current buffer size
   *
   * @return the current buffer size
   */
  public float getSize() {
    return this.bufferSize;
  }

  /**
   * Insert a row into the row buffer
   *
   * @param row
   * @param offsetToken
   */
  public void insertRow(Map<String, Object> row, String offsetToken) {
    insertRows(Collections.singletonList(row), offsetToken);
  }

  /**
   * Insert a batch of rows into the row buffer
   *
   * @param rows
   * @param offsetToken offset token of the latest row in the batch
   */
  public void insertRows(Collection<Map<String, Object>> rows, String offsetToken) {
    this.flushLock.lock();
    try {
      for (Map<String, Object> row : rows) {
        convertRowToArrow(row);
        this.rowCount++;
      }
      this.owningChannel.setOffsetToken(offsetToken);
    } catch (Exception e) {
      // TODO SNOW-348857: Return a response instead in case customer wants to skip error rows
      // TODO SNOW-348857: What offset token to return if the latest row failed?
      throw new SFException(e, ErrorCode.INVALID_ROW);
    } finally {
      this.flushLock.unlock();
    }
  }

  /**
   * Flush the data in the row buffer by taking the ownership of the old vectors and pass all the
   * required info back to the flush service to build the blob
   *
   * @return A ChannelData object that contains the info needed by the flush service to build a blob
   */
  public ChannelData flush() {
    if (this.rowCount > 0) {
      List<FieldVector> oldVectors = new ArrayList<>();
      long rowCount = 0L;
      float bufferSize = 0F;
      int curRowIndex = 0;
      long rowSequencer = 0;
      String offsetToken = null;

      logDebug(
          "Arrow buffer flush about to take lock on channel: {}",
          this.owningChannel.getFullyQualifiedName());
      this.flushLock.lock();
      try {
        if (this.rowCount > 0) {
          // Transfer the ownership of the vectors
          for (FieldVector vector : this.vectors.values()) {
            vector.setValueCount(this.curRowIndex);
            TransferPair t = vector.getTransferPair(this.allocator);
            t.transfer();
            oldVectors.add((FieldVector) t.getTo());
          }

          rowCount = this.rowCount;
          bufferSize = this.bufferSize;
          curRowIndex = this.curRowIndex;
          rowSequencer = this.owningChannel.getAndIncrementRowSequencer();
          offsetToken = this.owningChannel.getOffsetToken();
          // Reset everything in the buffer once we save all the info
          reset();
        }
      } finally {
        this.flushLock.unlock();
      }
      logDebug(
          "Arrow buffer flush released lock on channel: {}, rowCount: {}, bufferSize: {}",
          this.owningChannel.getFullyQualifiedName(),
          rowCount,
          bufferSize);

      if (!oldVectors.isEmpty()) {
        ChannelData data = new ChannelData();
        data.setVectors(oldVectors);
        data.setRowCount(rowCount);
        data.setBufferSize(bufferSize);
        data.setChannel(this.owningChannel);
        data.setRowSequencer(rowSequencer);
        data.setOffsetToken(offsetToken);
        return data;
      }
    }
    return null;
  }

  /**
   * Build the column field from the column metadata
   *
   * @param column column metadata
   * @return Column field object
   */
  private Field buildField(ColumnMetadata column) {
    ArrowType arrowType;
    FieldType fieldType;
    List<Field> children = null;

    // Put info into the metadata, which will be used by the Arrow reader later
    Map<String, String> metadata = new HashMap<>();
    metadata.put(COLUMN_LOGICAL_TYPE, column.getLogicalType());
    metadata.put(COLUMN_PHYSICAL_TYPE, column.getPhysicalType());
    if (column.getPrecision() != null) {
      metadata.put(COLUMN_PRECISION, column.getPrecision().toString());
    }
    if (column.getScale() != null) {
      metadata.put(COLUMN_SCALE, column.getScale().toString());
    }
    if (column.getByteLength() != null) {
      metadata.put(COLUMN_BYTE_LENGTH, column.getByteLength().toString());
    }
    if (column.getLength() != null) {
      metadata.put(COLUMN_CHAR_LENGTH, column.getLength().toString());
    }

    // Handle differently depends on the column logical and physical types
    switch (ColumnLogicalType.valueOf(column.getLogicalType())) {
      case FIXED:
        switch (ColumnPhysicalType.valueOf(column.getPhysicalType())) {
          case SB1:
            arrowType =
                column.getScale() == 0
                    ? Types.MinorType.TINYINT.getType()
                    : new ArrowType.Decimal(column.getPrecision(), column.getScale());
            break;
          case SB2:
            arrowType =
                column.getScale() == 0
                    ? Types.MinorType.SMALLINT.getType()
                    : new ArrowType.Decimal(column.getPrecision(), column.getScale());
            break;
          case SB4:
            arrowType =
                column.getScale() == 0
                    ? Types.MinorType.INT.getType()
                    : new ArrowType.Decimal(column.getPrecision(), column.getScale());
            break;
          case SB8:
            arrowType =
                column.getScale() == 0
                    ? Types.MinorType.BIGINT.getType()
                    : new ArrowType.Decimal(column.getPrecision(), column.getScale());
            break;
          case SB16:
            arrowType = new ArrowType.Decimal(column.getPrecision(), column.getScale());
            break;
          default:
            throw new SFException(
                ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
        }
        break;
      case ANY:
      case ARRAY:
      case CHAR:
      case TEXT:
      case OBJECT:
      case VARIANT:
        arrowType = Types.MinorType.VARCHAR.getType();
        break;
      default:
        throw new SFException(
            ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
    }

    // Create the corresponding column field base on the column data type
    fieldType = new FieldType(column.getNullable(), arrowType, null, metadata);
    return new Field(column.getName(), fieldType, children);
  }

  /**
   * Convert the input row to the correct Arrow format
   *
   * @param row input row
   */
  // TODO: need to verify each row with the table schema
  private void convertRowToArrow(Map<String, Object> row) {
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      this.bufferSize += 0.125; // 1/8 for null value bitmap
      String columnName = entry.getKey();
      StreamingUtils.assertStringNotNullOrEmpty("invalid column name", columnName);
      columnName =
          (columnName.charAt(0) == '"' && columnName.charAt(columnName.length() - 1) == '"')
              ? columnName.substring(1, columnName.length() - 1)
              : columnName.toUpperCase();
      Object value = entry.getValue();
      Field field = this.fields.get(columnName);
      StreamingUtils.assertNotNull("Arrow column field", field);
      FieldVector vector = this.vectors.get(columnName);
      StreamingUtils.assertNotNull("Arrow column vector", vector);
      ColumnLogicalType logicalType =
          ColumnLogicalType.valueOf(field.getMetadata().get(COLUMN_LOGICAL_TYPE));
      ColumnPhysicalType physicalType =
          ColumnPhysicalType.valueOf(field.getMetadata().get(COLUMN_PHYSICAL_TYPE));
      switch (logicalType) {
        case FIXED:
          switch (physicalType) {
            case SB1:
              if (value == null) {
                ((TinyIntVector) vector).setNull(this.curRowIndex);
              } else {
                ((TinyIntVector) vector).setSafe(this.curRowIndex, (byte) value);
                this.bufferSize += 1;
              }
              break;
            case SB2:
              if (value == null) {
                ((SmallIntVector) vector).setNull(this.curRowIndex);
              } else {
                ((SmallIntVector) vector).setSafe(this.curRowIndex, (short) value);
                this.bufferSize += 2;
              }
              break;
            case SB4:
              if (value == null) {
                ((IntVector) vector).setNull(this.curRowIndex);
              } else {
                ((IntVector) vector).setSafe(this.curRowIndex, (int) value);
                this.bufferSize += 4;
              }
              break;
            case SB8:
              if (value == null) {
                ((BigIntVector) vector).setNull(this.curRowIndex);
              } else {
                ((BigIntVector) vector).setSafe(this.curRowIndex, (long) value);
                this.bufferSize += 8;
              }
              break;
            case SB16:
              if (value == null) {
                ((DecimalVector) vector).setNull(this.curRowIndex);
              } else {
                ((DecimalVector) vector)
                    .setSafe(this.curRowIndex, new BigDecimal(value.toString()));
                this.bufferSize += 16;
              }
              break;
            default:
              throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
          }
          break;
        case ANY:
        case ARRAY:
        case CHAR:
        case TEXT:
        case OBJECT:
        case VARIANT:
          if (value == null) {
            ((VarCharVector) vector).setNull(this.curRowIndex);
          } else {
            Text text = new Text(value.toString());
            ((VarCharVector) vector).setSafe(this.curRowIndex, text);
            this.bufferSize += text.getBytes().length;
          }
          break;
        default:
          throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
      }
    }
    this.curRowIndex++;
  }
}

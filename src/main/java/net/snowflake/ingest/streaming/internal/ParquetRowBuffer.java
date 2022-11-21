/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * The buffer in the Streaming Ingest channel that holds the un-flushed rows, these rows will be
 * converted to Parquet format for faster processing
 */
public class ParquetRowBuffer extends AbstractRowBuffer<ParquetChunkData> {
  private static final Logging logger = new Logging(ParquetRowBuffer.class);

  private static final String PARQUET_MESSAGE_TYPE_NAME = "bdec";

  private final Map<String, Pair<ColumnMetadata, Integer>> fieldIndex;
  private final Map<String, String> metadata;
  private final List<List<Object>> data;
  private final List<List<Object>> tempData;

  private MessageType schema;

  /**
   * Construct a ParquetRowBuffer object
   *
   * @param channel client channel
   */
  ParquetRowBuffer(SnowflakeStreamingIngestChannelInternal<ParquetChunkData> channel) {
    super(channel);
    fieldIndex = new HashMap<>();
    metadata = new HashMap<>();
    data = new ArrayList<>();
    tempData = new ArrayList<>();
  }

  @Override
  public void setupSchema(List<ColumnMetadata> columns) {
    fieldIndex.clear();
    metadata.clear();
    metadata.put("sfVer", "1,1");
    List<Type> parquetTypes = new ArrayList<>();
    // Snowflake column id that corresponds to the order in 'columns' received from server
    // id is required to pack column metadata for the server scanner, e.g. decimal scale and
    // precision
    int id = 1;
    for (ColumnMetadata column : columns) {
      ParquetTypeGenerator.ParquetTypeInfo typeInfo =
          ParquetTypeGenerator.generateColumnParquetTypeInfo(column, id);
      parquetTypes.add(typeInfo.getParquetType());
      this.metadata.putAll(typeInfo.getMetadata());
      fieldIndex.put(column.getName(), new Pair<>(column, parquetTypes.size() - 1));
      if (!column.getNullable()) {
        addNonNullableFieldName(column.getName());
      }
      this.statsMap.put(column.getName(), new RowBufferStats(column.getCollation()));

      if (this.owningChannel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.ABORT) {
        this.tempStatsMap.put(column.getName(), new RowBufferStats(column.getCollation()));
      }

      id++;
    }
    schema = new MessageType(PARQUET_MESSAGE_TYPE_NAME, parquetTypes);
  }

  @Override
  boolean hasColumn(String name) {
    return fieldIndex.containsKey(name);
  }

  @Override
  float addRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames) {
    return addRow(row, data, statsMap, formattedInputColumnNames);
  }

  @Override
  float addTempRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames) {
    return addRow(row, tempData, statsMap, formattedInputColumnNames);
  }

  /**
   * Adds a row to the parquet buffer.
   *
   * @param row row to add
   * @param out internal buffer to add to
   * @param statsMap column stats map
   * @param inputColumnNames list of input column names after formatting
   * @return row size
   */
  private float addRow(
      Map<String, Object> row,
      List<List<Object>> out,
      Map<String, RowBufferStats> statsMap,
      Set<String> inputColumnNames) {
    Object[] indexedRow = new Object[fieldIndex.size()];
    float size = 0F;
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      String columnName = LiteralQuoteUtils.formatColumnName(key);
      int colIndex = fieldIndex.get(columnName).getSecond();
      RowBufferStats stats = statsMap.get(columnName);
      ColumnMetadata column = fieldIndex.get(columnName).getFirst();
      ColumnDescriptor columnDescriptor = schema.getColumns().get(colIndex);
      PrimitiveType.PrimitiveTypeName typeName =
          columnDescriptor.getPrimitiveType().getPrimitiveTypeName();
      ParquetValueParser.ParquetBufferValue valueWithSize =
          ParquetValueParser.parseColumnValueToParquet(value, column, typeName, stats);
      indexedRow[colIndex] = valueWithSize.getValue();
      size += valueWithSize.getSize();
    }
    out.add(Arrays.asList(indexedRow));

    for (String columnName : Sets.difference(this.fieldIndex.keySet(), inputColumnNames)) {
      statsMap.get(columnName).incCurrentNullCount();
    }
    return size;
  }

  @Override
  void moveTempRowsToActualBuffer(int tempRowCount) {
    data.addAll(tempData);
  }

  @Override
  void clearTempRows() {
    tempData.clear();
  }

  @Override
  boolean hasColumns() {
    return !fieldIndex.isEmpty();
  }

  @Override
  Optional<ParquetChunkData> getSnapshot() {
    List<List<Object>> oldData = new ArrayList<>();
    data.forEach(r -> oldData.add(new ArrayList<>(r)));
    return oldData.isEmpty()
        ? Optional.empty()
        : Optional.of(new ParquetChunkData(oldData, metadata));
  }

  /** Used only for testing. */
  @Override
  Object getVectorValueAt(String column, int index) {
    int colIndex = schema.getFieldIndex(column);
    Object value = data.get(index).get(colIndex);
    ColumnMetadata columnMetadata =
        fieldIndex.get(LiteralQuoteUtils.formatColumnName(column)).getFirst();
    String physicalTypeStr = columnMetadata.getPhysicalType();
    ColumnPhysicalType physicalType = ColumnPhysicalType.valueOf(physicalTypeStr);
    String logicalTypeStr = columnMetadata.getLogicalType();
    ColumnLogicalType logicalType = ColumnLogicalType.valueOf(logicalTypeStr);
    if (logicalType == ColumnLogicalType.FIXED) {
      if (physicalType == ColumnPhysicalType.SB1) {
        value = ((Integer) value).byteValue();
      }
      if (physicalType == ColumnPhysicalType.SB2) {
        value = ((Integer) value).shortValue();
      }
      if (physicalType == ColumnPhysicalType.SB16) {
        value = new BigDecimal(new BigInteger((byte[]) value), columnMetadata.getScale());
      }
    }
    if (logicalType == ColumnLogicalType.BINARY && value != null) {
      value = ((String) value).getBytes(StandardCharsets.UTF_8);
    }
    return value;
  }

  @Override
  int getTempRowCount() {
    return tempData.size();
  }

  @Override
  void reset() {
    super.reset();
    data.clear();
  }

  @Override
  public void close(String name) {
    this.fieldIndex.clear();
    logger.logInfo(
        "Trying to close parquet buffer for channel={} from function={}",
        this.owningChannel.getName(),
        name);
  }

  @Override
  public Flusher<ParquetChunkData> createFlusher() {
    return new ParquetFlusher(schema);
  }
}

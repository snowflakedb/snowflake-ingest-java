/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
import net.snowflake.ingest.connection.TelemetryService;
import net.snowflake.ingest.streaming.OffsetTokenVerificationFunction;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.parquet.hadoop.BdecParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * The buffer in the Streaming Ingest channel that holds the un-flushed rows, these rows will be
 * converted to Parquet format for faster processing
 */
public class ParquetRowBuffer extends AbstractRowBuffer<ParquetChunkData> {
  private static final String PARQUET_MESSAGE_TYPE_NAME = "bdec";

  private final Map<String, ParquetColumn> fieldIndex;

  /* map that contains metadata like typeinfo for columns and other information needed by the server scanner */
  private final Map<String, String> metadata;

  /* Unflushed rows as Java objects. Needed for the Parquet w/o memory optimization. */
  private final List<List<Object>> data;

  /* BDEC Parquet writer. It is used to buffer unflushed data in Parquet internal buffers instead of using Java objects */
  private BdecParquetWriter bdecParquetWriter;

  private ByteArrayOutputStream fileOutput;
  private final List<List<Object>> tempData;

  private MessageType schema;

  /** Construct a ParquetRowBuffer object. */
  ParquetRowBuffer(
      OpenChannelRequest.OnErrorOption onErrorOption,
      ZoneId defaultTimezone,
      String fullyQualifiedChannelName,
      Consumer<Float> rowSizeMetric,
      ChannelRuntimeState channelRuntimeState,
      ClientBufferParameters clientBufferParameters,
      OffsetTokenVerificationFunction offsetTokenVerificationFunction,
      TelemetryService telemetryService) {
    super(
        onErrorOption,
        defaultTimezone,
        fullyQualifiedChannelName,
        rowSizeMetric,
        channelRuntimeState,
        clientBufferParameters,
        offsetTokenVerificationFunction,
        telemetryService);
    this.fieldIndex = new HashMap<>();
    this.metadata = new HashMap<>();
    this.data = new ArrayList<>();
    this.tempData = new ArrayList<>();
  }

  @Override
  public void setupSchema(List<ColumnMetadata> columns) {
    fieldIndex.clear();
    metadata.clear();
    metadata.put("sfVer", "1,1");
    List<Type> parquetTypes = new ArrayList<>();
    int id = 1;
    for (ColumnMetadata column : columns) {
      validateColumnCollation(column);
      ParquetTypeGenerator.ParquetTypeInfo typeInfo =
          ParquetTypeGenerator.generateColumnParquetTypeInfo(column, id);
      parquetTypes.add(typeInfo.getParquetType());
      this.metadata.putAll(typeInfo.getMetadata());
      int columnIndex = parquetTypes.size() - 1;
      fieldIndex.put(
          column.getInternalName(),
          new ParquetColumn(column, columnIndex, typeInfo.getPrimitiveTypeName()));
      if (!column.getNullable()) {
        addNonNullableFieldName(column.getInternalName());
      }
      this.statsMap.put(
          column.getInternalName(),
          new RowBufferStats(column.getName(), column.getCollation(), column.getOrdinal()));

      if (onErrorOption == OpenChannelRequest.OnErrorOption.ABORT
          || onErrorOption == OpenChannelRequest.OnErrorOption.SKIP_BATCH) {
        this.tempStatsMap.put(
            column.getInternalName(),
            new RowBufferStats(column.getName(), column.getCollation(), column.getOrdinal()));
      }

      id++;
    }
    schema = new MessageType(PARQUET_MESSAGE_TYPE_NAME, parquetTypes);
    createFileWriter();
    tempData.clear();
    data.clear();
  }

  /** Create BDEC file writer if Parquet memory optimization is enabled. */
  private void createFileWriter() {
    fileOutput = new ByteArrayOutputStream();
    try {
      if (clientBufferParameters.getEnableParquetInternalBuffering()) {
        bdecParquetWriter =
            new BdecParquetWriter(
                fileOutput,
                schema,
                metadata,
                channelFullyQualifiedName,
                clientBufferParameters.getMaxChunkSizeInBytes(),
                clientBufferParameters.getBdecParquetCompression());
      } else {
        this.bdecParquetWriter = null;
      }
      data.clear();
    } catch (IOException e) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "cannot create parquet writer", e);
    }
  }

  @Override
  boolean hasColumn(String name) {
    return fieldIndex.containsKey(name);
  }

  @Override
  float addRow(
      Map<String, Object> row,
      int bufferedRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames,
      final long insertRowIndex) {
    return addRow(row, this::writeRow, statsMap, formattedInputColumnNames, insertRowIndex);
  }

  void writeRow(List<Object> row) {
    if (clientBufferParameters.getEnableParquetInternalBuffering()) {
      bdecParquetWriter.writeRow(row);
    } else {
      data.add(row);
    }
  }

  @Override
  float addTempRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames,
      long insertRowIndex) {
    return addRow(row, tempData::add, statsMap, formattedInputColumnNames, insertRowIndex);
  }

  /**
   * Adds a row to the parquet buffer.
   *
   * @param row row to add
   * @param out internal buffer to add to
   * @param statsMap column stats map
   * @param inputColumnNames list of input column names after formatting
   * @param insertRowsCurrIndex Row index of the input Rows passed in {@link
   *     net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel#insertRows(Iterable,
   *     String)}
   * @return row size
   */
  private float addRow(
      Map<String, Object> row,
      Consumer<List<Object>> out,
      Map<String, RowBufferStats> statsMap,
      Set<String> inputColumnNames,
      long insertRowsCurrIndex) {
    Object[] indexedRow = new Object[fieldIndex.size()];
    float size = 0F;

    // Create new empty stats just for the current row.
    Map<String, RowBufferStats> forkedStatsMap = new HashMap<>();

    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      String columnName = LiteralQuoteUtils.unquoteColumnName(key);
      ParquetColumn parquetColumn = fieldIndex.get(columnName);
      int colIndex = parquetColumn.index;
      RowBufferStats forkedStats = statsMap.get(columnName).forkEmpty();
      forkedStatsMap.put(columnName, forkedStats);
      ColumnMetadata column = parquetColumn.columnMetadata;
      ParquetValueParser.ParquetBufferValue valueWithSize =
          ParquetValueParser.parseColumnValueToParquet(
              value,
              column,
              parquetColumn.type,
              forkedStats,
              defaultTimezone,
              insertRowsCurrIndex,
              clientBufferParameters.isEnableNewJsonParsingLogic());
      indexedRow[colIndex] = valueWithSize.getValue();
      size += valueWithSize.getSize();
    }

    long rowSizeRoundedUp = Double.valueOf(Math.ceil(size)).longValue();

    if (rowSizeRoundedUp > clientBufferParameters.getMaxAllowedRowSizeInBytes()) {
      throw new SFException(
          ErrorCode.MAX_ROW_SIZE_EXCEEDED,
          String.format(
              "rowSizeInBytes:%.3f, maxAllowedRowSizeInBytes:%d, rowIndex:%d",
              size, clientBufferParameters.getMaxAllowedRowSizeInBytes(), insertRowsCurrIndex));
    }

    out.accept(Arrays.asList(indexedRow));

    // All input values passed validation, iterate over the columns again and combine their existing
    // statistics with the forked statistics for the current row.
    for (Map.Entry<String, RowBufferStats> forkedColStats : forkedStatsMap.entrySet()) {
      String columnName = forkedColStats.getKey();
      statsMap.put(
          columnName,
          RowBufferStats.getCombinedStats(statsMap.get(columnName), forkedColStats.getValue()));
    }

    // Increment null count for column missing in the input map
    for (String columnName : Sets.difference(this.fieldIndex.keySet(), inputColumnNames)) {
      statsMap.get(columnName).incCurrentNullCount();
    }
    return size;
  }

  @Override
  void moveTempRowsToActualBuffer(int tempRowCount) {
    tempData.forEach(this::writeRow);
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
    if (!clientBufferParameters.getEnableParquetInternalBuffering()) {
      data.forEach(r -> oldData.add(new ArrayList<>(r)));
    }
    return bufferedRowCount <= 0
        ? Optional.empty()
        : Optional.of(new ParquetChunkData(oldData, bdecParquetWriter, fileOutput, metadata));
  }

  /** Used only for testing. */
  @Override
  Object getVectorValueAt(String column, int index) {
    if (data == null) {
      return null;
    }
    int colIndex = fieldIndex.get(column).index;
    Object value = data.get(index).get(colIndex);
    ColumnMetadata columnMetadata = fieldIndex.get(column).columnMetadata;
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
      value = value instanceof String ? ((String) value).getBytes(StandardCharsets.UTF_8) : value;
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
    createFileWriter();
    data.clear();
  }

  /** Close the row buffer by releasing its internal resources. */
  @Override
  void closeInternal() {
    if (bdecParquetWriter != null) {
      try {
        bdecParquetWriter.close();
      } catch (IOException e) {
        throw new SFException(ErrorCode.INTERNAL_ERROR, "Failed to close parquet writer", e);
      }
    }
  }

  @Override
  public Flusher<ParquetChunkData> createFlusher() {
    return new ParquetFlusher(
        schema,
        clientBufferParameters.getEnableParquetInternalBuffering(),
        clientBufferParameters.getMaxChunkSizeInBytes(),
        clientBufferParameters.getBdecParquetCompression());
  }

  private static class ParquetColumn {
    final ColumnMetadata columnMetadata;
    final int index;
    final PrimitiveType.PrimitiveTypeName type;

    private ParquetColumn(
        ColumnMetadata columnMetadata, int index, PrimitiveType.PrimitiveTypeName type) {
      this.columnMetadata = columnMetadata;
      this.index = index;
      this.type = type;
    }
  }
}

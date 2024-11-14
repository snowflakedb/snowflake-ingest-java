/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Utils.concatDotPath;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.TelemetryService;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OffsetTokenVerificationFunction;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.IcebergDataTypeParser;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SubColumnFinder;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * The buffer in the Streaming Ingest channel that holds the un-flushed rows, these rows will be
 * converted to Parquet format for faster processing
 */
public class ParquetRowBuffer extends AbstractRowBuffer<ParquetChunkData> {

  /* map that contains metadata like typeinfo for columns and other information needed by the server scanner */
  private final Map<String, String> metadata;

  /* Unflushed rows as Java objects. Needed for the Parquet w/o memory optimization. */
  private final List<List<Object>> data;
  private final List<List<Object>> tempData;

  private final ParquetProperties.WriterVersion parquetWriterVersion;

  private MessageType schema;
  private SubColumnFinder subColumnFinder;

  /** Construct a ParquetRowBuffer object. */
  ParquetRowBuffer(
      OpenChannelRequest.OnErrorOption onErrorOption,
      ZoneId defaultTimezone,
      String fullyQualifiedChannelName,
      Consumer<Float> rowSizeMetric,
      ChannelRuntimeState channelRuntimeState,
      ClientBufferParameters clientBufferParameters,
      OffsetTokenVerificationFunction offsetTokenVerificationFunction,
      ParquetProperties.WriterVersion parquetWriterVersion,
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
    this.metadata = new HashMap<>();
    this.data = new ArrayList<>();
    this.tempData = new ArrayList<>();
    this.parquetWriterVersion = parquetWriterVersion;
  }

  /**
   * Set up the parquet schema.
   *
   * @param columns top level columns list of column metadata
   */
  @Override
  public void setupSchema(List<ColumnMetadata> columns) {
    fieldIndex.clear();
    metadata.clear();
    if (!clientBufferParameters.isEnableIcebergStreaming()) {
      metadata.put("sfVer", "1,1");
    }
    metadata.put(Constants.SDK_VERSION_KEY, RequestBuilder.DEFAULT_VERSION);
    List<Type> parquetTypes = new ArrayList<>();
    int id = 1;

    for (ColumnMetadata column : columns) {
      /* Set up fields using top level column information */
      validateColumnCollation(column);
      ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(column, id);
      Type parquetType = typeInfo.getParquetType();
      parquetTypes.add(parquetType);
      this.metadata.putAll(typeInfo.getMetadata());
      int columnIndex = parquetTypes.size() - 1;
      fieldIndex.put(column.getInternalName(), new ParquetColumn(column, columnIndex, parquetType));

      if (!column.getNullable()) {
        addNonNullableFieldName(column.getInternalName());
      }

      if (!clientBufferParameters.isEnableIcebergStreaming()) {
        /* Streaming to FDN table doesn't support sub-columns, set up the stats here. */
        this.statsMap.put(
            column.getInternalName(),
            new RowBufferStats(
                column.getName(),
                column.getCollation(),
                column.getOrdinal(),
                null /* fieldId */,
                parquetType.isPrimitive() ? parquetType.asPrimitiveType() : null,
                false /* enableDistinctValuesCount */,
                false /* enableValuesCount */));

        if (onErrorOption == OpenChannelRequest.OnErrorOption.ABORT
            || onErrorOption == OpenChannelRequest.OnErrorOption.SKIP_BATCH) {
          /*
           * tempStatsMap is used to store stats for the current batch,
           * create a separate stats in case current batch has invalid rows which ruins the original stats.
           */
          this.tempStatsMap.put(
              column.getInternalName(),
              new RowBufferStats(
                  column.getName(),
                  column.getCollation(),
                  column.getOrdinal(),
                  null /* fieldId */,
                  parquetType.isPrimitive() ? parquetType.asPrimitiveType() : null,
                  false /* enableDistinctValuesCount */,
                  false /* enableValuesCount */));
        }
      }

      id++;
    }
    schema = new MessageType(clientBufferParameters.getParquetMessageTypeName(), parquetTypes);

    /*
     * Iceberg mode requires stats for all primitive columns and sub-columns, set them up here.
     *
     * There are two values that are used to identify a column in the stats map:
     *   1. ordinal - The ordinal is the index of the top level column in the schema.
     *   2. fieldId - The fieldId is the id of all sub-columns in the schema.
     *                It's indexed by the level and order of the column in the schema.
     *                Note that the fieldId is set to 0 for non-structured columns.
     *
     * For example, consider the following schema:
     *   F1 INT,
     *   F2 STRUCT(F21 STRUCT(F211 INT), F22 INT),
     *   F3 INT,
     *   F4 MAP(INT, MAP(INT, INT)),
     *   F5 INT,
     *   F6 ARRAY(INT),
     *   F7 INT
     *
     * The ordinal and fieldId  will look like this:
     *   F1:             ordinal=1, fieldId=1
     *   F2:             ordinal=2, fieldId=2
     *   F2.F21:         ordinal=2, fieldId=8
     *   F2.F21.F211:    ordinal=2, fieldId=13
     *   F2.F22:         ordinal=2, fieldId=9
     *   F3:             ordinal=3, fieldId=3
     *   F4:             ordinal=4, fieldId=4
     *   F4.key:         ordinal=4, fieldId=10
     *   F4.value:       ordinal=4, fieldId=11
     *   F4.value.key:   ordinal=4, fieldId=14
     *   F4.value.value: ordinal=4, fieldId=15
     *   F5:             ordinal=5, fieldId=5
     *   F6:             ordinal=6, fieldId=6
     *   F6.element:     ordinal=6, fieldId=12
     *   F7:             ordinal=7, fieldId=7
     *
     * The stats map will contain the following entries:
     *   F1:             ordinal=1, fieldId=0
     *   F2:             ordinal=2, fieldId=0
     *   F2.F21.F211:    ordinal=2, fieldId=13
     *   F2.F22:         ordinal=2, fieldId=9
     *   F3:             ordinal=3, fieldId=0
     *   F4.key:         ordinal=4, fieldId=10
     *   F4.value.key:   ordinal=4, fieldId=14
     *   F4.value.value: ordinal=4, fieldId=15
     *   F5:             ordinal=5, fieldId=0
     *   F6.element:     ordinal=6, fieldId=12
     *   F7:             ordinal=7, fieldId=0
     */
    if (clientBufferParameters.isEnableIcebergStreaming()) {
      Map<Integer, ColumnMetadata> ordinalToColumn = new HashMap<>();
      for (ColumnMetadata column : columns) {
        ordinalToColumn.put(column.getOrdinal(), column);
      }

      for (ColumnDescriptor columnDescriptor : schema.getColumns()) {
        String[] path = columnDescriptor.getPath();
        String columnDotPath = concatDotPath(path);
        PrimitiveType primitiveType = columnDescriptor.getPrimitiveType();
        boolean isInRepeatedGroup = false;

        if (path.length > 1
            && schema
                .getType(Arrays.copyOf(path, path.length - 1))
                .isRepetition(Type.Repetition.REPEATED)) {
          if (!primitiveType.getName().equals(IcebergDataTypeParser.ELEMENT)
              && !primitiveType.getName().equals(IcebergDataTypeParser.KEY)
              && !primitiveType.getName().equals(IcebergDataTypeParser.VALUE)) {
            throw new SFException(
                ErrorCode.INTERNAL_ERROR,
                String.format(
                    "Invalid repeated column %s, column name must be %s, %s or %s",
                    columnDotPath,
                    IcebergDataTypeParser.ELEMENT,
                    IcebergDataTypeParser.KEY,
                    IcebergDataTypeParser.VALUE));
          }
          isInRepeatedGroup = true;
        }

        boolean isPrimitiveColumn = path.length == 1;

        /* set fieldId to 0 for non-structured columns */
        int fieldId = isPrimitiveColumn ? 0 : primitiveType.getId().intValue();
        int ordinal = schema.getType(columnDescriptor.getPath()[0]).getId().intValue();

        /**
         * For non-structured columns, server side performs EP metadata check by columnsDisplayName,
         * set it to the original column name to avoid EP validation error. Structured datatype is
         * checked by fieldId and ordinal where columnDisplayName doesn't matter.
         */
        String columnDisplayName =
            isPrimitiveColumn
                ? Optional.ofNullable(ordinalToColumn.get(ordinal))
                    .orElseThrow(
                        () ->
                            new SFException(
                                ErrorCode.INTERNAL_ERROR,
                                String.format("Column not found. ordinal=%d.", ordinal)))
                    .getName()
                : columnDotPath;

        this.statsMap.put(
            primitiveType.getId().toString(),
            new RowBufferStats(
                columnDisplayName,
                null /* collationDefinitionString */,
                ordinal,
                fieldId,
                primitiveType,
                clientBufferParameters.isEnableDistinctValuesCount(),
                clientBufferParameters.isEnableValuesCount() && isInRepeatedGroup));

        if (onErrorOption == OpenChannelRequest.OnErrorOption.ABORT
            || onErrorOption == OpenChannelRequest.OnErrorOption.SKIP_BATCH) {
          this.tempStatsMap.put(
              primitiveType.getId().toString(),
              new RowBufferStats(
                  columnDisplayName,
                  null /* collationDefinitionString */,
                  ordinal,
                  fieldId,
                  primitiveType,
                  clientBufferParameters.isEnableDistinctValuesCount(),
                  clientBufferParameters.isEnableValuesCount() && isInRepeatedGroup));
        }
      }
      subColumnFinder = new SubColumnFinder(schema);
    }
    tempData.clear();
    data.clear();
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
      final long insertRowIndex,
      InsertValidationResponse.InsertError error) {
    return addRow(row, this::writeRow, statsMap, formattedInputColumnNames, insertRowIndex, error);
  }

  void writeRow(List<Object> row) {
    data.add(row);
  }

  @Override
  float addTempRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames,
      long insertRowIndex,
      InsertValidationResponse.InsertError error) {
    return addRow(row, tempData::add, statsMap, formattedInputColumnNames, insertRowIndex, error);
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
   * @param error insertion error object to populate if there is an error
   * @return row size
   */
  private float addRow(
      Map<String, Object> row,
      Consumer<List<Object>> out,
      Map<String, RowBufferStats> statsMap,
      Set<String> inputColumnNames,
      long insertRowsCurrIndex,
      InsertValidationResponse.InsertError error) {
    Object[] indexedRow = new Object[fieldIndex.size()];
    float size = 0F;

    // Create new empty stats just for the current row.
    Map<String, RowBufferStats> forkedStatsMap = new HashMap<>();
    statsMap.forEach((columnPath, stats) -> forkedStatsMap.put(columnPath, stats.forkEmpty()));

    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      String columnName = LiteralQuoteUtils.unquoteColumnName(key);
      ParquetColumn parquetColumn = fieldIndex.get(columnName);
      int colIndex = parquetColumn.index;
      ColumnMetadata column = parquetColumn.columnMetadata;
      ParquetBufferValue valueWithSize =
          (clientBufferParameters.isEnableIcebergStreaming()
              ? IcebergParquetValueParser.parseColumnValueToParquet(
                  value,
                  parquetColumn.type,
                  forkedStatsMap,
                  subColumnFinder,
                  defaultTimezone,
                  insertRowsCurrIndex,
                  error)
              : SnowflakeParquetValueParser.parseColumnValueToParquet(
                  value,
                  column,
                  parquetColumn.type.asPrimitiveType().getPrimitiveTypeName(),
                  forkedStatsMap.get(columnName),
                  defaultTimezone,
                  insertRowsCurrIndex,
                  clientBufferParameters.isEnableNewJsonParsingLogic()));
      indexedRow[colIndex] = valueWithSize.getValue();
      size += valueWithSize.getSize();
    }

    if (error.getMissingNotNullColNames() != null
        || error.getExtraColNames() != null
        || error.getNullValueForNotNullColNames() != null) {
      throw new SFException(
          ErrorCode.INVALID_FORMAT_ROW,
          String.format("Invalid row %d", insertRowsCurrIndex),
          String.format(
              "missingNotNullColNames=%s, extraColNames=%s, nullValueForNotNullColNames=%s",
              error.getMissingNotNullColNames(),
              error.getExtraColNames(),
              error.getNullValueForNotNullColNames()));
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

    // Increment null count for column and its sub-columns missing in the input map
    for (String columnName : Sets.difference(this.fieldIndex.keySet(), inputColumnNames)) {
      if (clientBufferParameters.isEnableIcebergStreaming()) {
        if (subColumnFinder == null) {
          throw new SFException(ErrorCode.INTERNAL_ERROR, "SubColumnFinder is not initialized.");
        }

        for (String subColumnId :
            subColumnFinder.getSubColumns(fieldIndex.get(columnName).type.getId())) {
          RowBufferStats stats = statsMap.get(subColumnId);
          if (stats == null) {
            throw new SFException(
                ErrorCode.INTERNAL_ERROR,
                String.format(
                    "Entry not found in stats map. fieldId=%s, column=%s.",
                    subColumnId,
                    subColumnFinder.getDotPath(new Type.ID(Integer.parseInt(subColumnId)))));
          }
          stats.incCurrentNullCount();
        }
      } else {
        statsMap.get(columnName).incCurrentNullCount();
      }
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
    data.forEach(r -> oldData.add(new ArrayList<>(r)));
    return bufferedRowCount <= 0
        ? Optional.empty()
        : Optional.of(new ParquetChunkData(oldData, metadata));
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
    /* Mismatch between Iceberg string & FDN String */
    if (Objects.equals(columnMetadata.getSourceIcebergDataType(), "\"string\"")) {
      value = value instanceof byte[] ? new String((byte[]) value, StandardCharsets.UTF_8) : value;
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

  /** Close the row buffer by releasing its internal resources. */
  @Override
  void closeInternal() {}

  @Override
  public Flusher<ParquetChunkData> createFlusher() {
    return new ParquetFlusher(
        schema,
        clientBufferParameters.getMaxChunkSizeInBytes(),
        clientBufferParameters.getMaxRowGroups(),
        clientBufferParameters.getBdecParquetCompression(),
        parquetWriterVersion,
        clientBufferParameters.isEnableDictionaryEncoding(),
        clientBufferParameters.isEnableIcebergStreaming());
  }
}

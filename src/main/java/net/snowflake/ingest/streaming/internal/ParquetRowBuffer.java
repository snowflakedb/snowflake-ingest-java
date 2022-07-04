package net.snowflake.ingest.streaming.internal;

import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.*;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.*;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ParquetRowBuffer implements RowBuffer<ParquetRowBuffer.ParquetChunkData> {
    // Snowflake table column logical type
    enum ColumnLogicalType {
        ANY,
        BOOLEAN(1),
        ROWINDEX,
        NULL(15),
        REAL(8),
        FIXED(2),
        TEXT(9),
        CHAR,
        BINARY(10),
        DATE(7),
        TIME(6),
        TIMESTAMP_LTZ(3),
        TIMESTAMP_NTZ(4),
        TIMESTAMP_TZ(5),
        INTERVAL,
        RAW,
        ARRAY(13, true),
        OBJECT(12, true),
        VARIANT(11, true),
        ROW,
        SEQUENCE,
        FUNCTION,
        USER_DEFINED_TYPE,
        ;

        private final int ordinal;
        private final boolean object;

        ColumnLogicalType() {
            this(-1);
        }

        ColumnLogicalType(int ordinal) {
            this(ordinal, false);
        }

        ColumnLogicalType(int ordinal, boolean object) {
            this.ordinal = ordinal;
            this.object = object;
        }

        public int getOrdinal() {
            return ordinal;
        }

        public boolean isObject() {
            return object;
        }
    }

    // Snowflake table column physical type
    private enum ColumnPhysicalType {
        ROWINDEX(9),
        DOUBLE(7),
        SB1(1),
        SB2(2),
        SB4(3),
        SB8(4),
        SB16(5),
        LOB(8),
        BINARY,
        ROW(10),
        ;

        private final int ordinal;

        ColumnPhysicalType() {
            this.ordinal = -1;
        }

        ColumnPhysicalType(int ordinal) {
            this.ordinal = ordinal;
        }

        public int getOrdinal() {
            return ordinal;
        }
    }

    private static final Logging logger = new Logging(ArrowRowBuffer.class);

    // Constants for column fields
    private static final String FIELD_EPOCH_IN_SECONDS = "epoch"; // seconds since epoch
    private static final String FIELD_TIME_ZONE = "timezone"; // time zone index
    private static final String FIELD_FRACTION_IN_NANOSECONDS = "fraction"; // fraction in nanoseconds

    // Column metadata that will send back to server as part of the blob, and will be used by the
    // Arrow reader
    private static final String COLUMN_PHYSICAL_TYPE = "physicalType";
    private static final String COLUMN_LOGICAL_TYPE = "logicalType";
    private static final String COLUMN_NULLABLE = "nullable";
    static final String COLUMN_SCALE = "scale";
    private static final String COLUMN_PRECISION = "precision";
    private static final String COLUMN_CHAR_LENGTH = "charLength";
    private static final String COLUMN_BYTE_LENGTH = "byteLength";
    @VisibleForTesting
    static final int DECIMAL_BIT_WIDTH = 128;

    private final List<ColumnMetadata> columns = new ArrayList<>();
    private final Map<String, Pair<ColumnMetadata, Integer>> fieldIndex = new HashMap<>();
    private final Map<String, String> metadata = new HashMap<>();

    List<List<Object>> data = new ArrayList<>();

    List<List<Object>> tempData = new ArrayList<>();

    // Map the column name to the stats
    @VisibleForTesting Map<String, RowBufferStats> statsMap;

    @VisibleForTesting Map<String, RowBufferStats> tempStatsMap;

    // Lock used to protect the buffers from concurrent read/write
    private final Lock flushLock;

    // Current row count
    @VisibleForTesting volatile int rowCount;

    // Current buffer size
    private volatile float bufferSize;

    // Reference to the Streaming Ingest channel that owns this buffer
    @VisibleForTesting final SnowflakeStreamingIngestChannelInternal owningChannel;

    // Names of non-nullable columns
    private final Set<String> nonNullableFieldNames = new HashSet<>();

    private MessageType schema;

    ParquetRowBuffer(SnowflakeStreamingIngestChannelInternal channel) {
        this.owningChannel = channel;
        this.flushLock = new ReentrantLock();
        this.rowCount = 0;
        this.bufferSize = 0F;

        // Initialize empty stats
        this.statsMap = new HashMap<>();
        this.tempStatsMap = new HashMap<>();
    }

    @Override
    public void setupSchema(List<ColumnMetadata> columns) {
        this.columns.clear();
        fieldIndex.clear();
        metadata.clear();
        List<Type> parquetTypes = new ArrayList<>();
        int id = 1;
        for (ColumnMetadata column : columns) {
            this.columns.add(column);
            Type type = buildField(column, id);
            parquetTypes.add(type);
            fieldIndex.put(column.getName(), new Pair<>(column, parquetTypes.size() - 1));
            if (!column.getNullable()) {
                this.nonNullableFieldNames.add(column.getName());
            }
            this.statsMap.put(column.getName(), new RowBufferStats(column.getCollation()));

            // TODO: abort

            id++;
        }
        schema = new MessageType("bdec", parquetTypes);
    }

    private Type buildField(ColumnMetadata column, int id) {
        Type parquetType;
        String name = column.getName();

        ColumnPhysicalType physicalType;
        ColumnLogicalType logicalType;
        try {
            physicalType = ColumnPhysicalType.valueOf(column.getPhysicalType());
            logicalType = ColumnLogicalType.valueOf(column.getLogicalType());
        } catch (IllegalArgumentException e) {
            throw new SFException(
                    ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
        }

        this.metadata.put(Integer.toString(id), logicalType.getOrdinal() + "," + physicalType.getOrdinal());

        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalTypeAnnotation = column.getScale() != null && column.getPrecision() != null ?
                LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.decimalType(column.getScale(), column.getPrecision()) : null;

        // Handle differently depends on the column logical and physical types
        switch (logicalType) {
            case FIXED:
                if ((column.getScale() != null && column.getScale() != 0)
                        || physicalType == ColumnPhysicalType.SB16) {
                    throw new UnsupportedOperationException("Data type: " + logicalType);
                } else {
                    switch (physicalType) {
                        case SB1:
                            parquetType = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED).as(decimalLogicalTypeAnnotation).id(id).length(4).named(name);
                            break;
                        case SB2:
                            parquetType = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED).as(decimalLogicalTypeAnnotation).id(id).length(4).named(name);
                            break;
                        case SB4:
                            parquetType = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED).as(decimalLogicalTypeAnnotation).id(id).length(4).named(name);
                            break;
                        case SB8:
                            parquetType = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED).as(decimalLogicalTypeAnnotation).id(id).length(8).named(name);
                            break;
                        default:
                            throw new SFException(
                                    ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
                    }
                }
                break;
            case ARRAY:
            case OBJECT:
            case VARIANT:
                this.metadata.put(id + ":obj_enc", "1");
            case ANY:
            case CHAR:
            case TEXT:
            case BINARY:
                parquetType = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).as(LogicalTypeAnnotation.stringType()).id(id).named(name);
                break;
            case TIMESTAMP_LTZ:
            case TIMESTAMP_NTZ:
            case TIMESTAMP_TZ:
            case DATE:
            case TIME:
                throw new UnsupportedOperationException("Data type: " + logicalType);
            case BOOLEAN:
                parquetType = Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED).id(id).named(name);
                break;
            case REAL:
                parquetType = Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED).id(id).named(name);
                break;
            default:
                throw new SFException(
                        ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
        }

        return parquetType;
    }

    private Set<String> verifyInputColumns(Map<String, Object> row) {
        Set<String> inputColumns =
                row.keySet().stream().map(ParquetRowBuffer::formatColumnName).collect(Collectors.toSet());

        for (String columnName : this.nonNullableFieldNames) {
            if (!inputColumns.contains(columnName)) {
                throw new SFException(
                        ErrorCode.INVALID_ROW,
                        "Missing column: " + columnName,
                        "Values for all non-nullable columns must be specified.");
            }
        }

        for (String columnName : inputColumns) {
            if (!fieldIndex.containsKey(columnName)) {
                throw new SFException(
                        ErrorCode.INVALID_ROW,
                        "Extra column: " + columnName,
                        "Columns not present in the table shouldn't be specified.");
            }
        }

        return inputColumns;
    }

    private static String formatColumnName(String columnName) {
        Utils.assertStringNotNullOrEmpty("invalid column name", columnName);
        return (columnName.charAt(0) == '"' && columnName.charAt(columnName.length() - 1) == '"')
                ? columnName.substring(1, columnName.length() - 1)
                : columnName.toUpperCase();
    }

    @Override
    public InsertValidationResponse insertRows(Iterable<Map<String, Object>> rows, String offsetToken) {
        float rowSize = 0F;
        if (fieldIndex.isEmpty()) {
            throw new SFException(ErrorCode.INTERNAL_ERROR, "Empty column fields");
        }

        InsertValidationResponse response = new InsertValidationResponse();
        this.flushLock.lock();
        try {
            if (this.owningChannel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.CONTINUE) {
                // Used to map incoming row(nth row) to InsertError(for nth row) in response
                long rowIndex = 0;
                for (Map<String, Object> row : rows) {
                    try {
                        rowSize += addRow(row, data);
                        this.rowCount++;
                        this.bufferSize += rowSize;
                    } catch (SFException e) {
                        response.addError(new InsertValidationResponse.InsertError(row, e, rowIndex));
                    } catch (Throwable e) {
                        logger.logWarn("Unexpected error happens during insertRows: {}", e.getMessage());
                        response.addError(
                                new InsertValidationResponse.InsertError(
                                        row, new SFException(e, ErrorCode.INTERNAL_ERROR, e.getMessage()), rowIndex));
                    }
                    rowIndex++;
                    if (this.rowCount == Integer.MAX_VALUE) {
                        throw new SFException(ErrorCode.INTERNAL_ERROR, "Row count reaches MAX value");
                    }
                }
            } else {
                // If the on_error option is ABORT, simply throw the first exception
                float tempRowSize = 0F;
                int tempRowCount = 0;
                for (Map<String, Object> row : rows) {
                    tempRowSize += addRow(row, tempData);
                    tempRowCount++;
                }

                // If all the rows are inserted successfully, transfer the rows from temp to actual data
                // and update the row size and row count
                data.addAll(tempData);
                rowSize = tempRowSize;
                if ((long) this.rowCount + tempRowCount >= Integer.MAX_VALUE) {
                    throw new SFException(ErrorCode.INTERNAL_ERROR, "Row count reaches MAX value");
                }
                this.rowCount += tempRowCount;
                this.bufferSize += rowSize;
                this.statsMap.forEach(
                        (colName, stats) -> {
                            this.statsMap.put(
                                    colName, RowBufferStats.getCombinedStats(stats, this.tempStatsMap.get(colName)));
                        });
            }

            this.owningChannel.setOffsetToken(offsetToken);
            this.owningChannel.collectRowSize(rowSize);
        } finally {
            this.tempStatsMap.values().forEach(RowBufferStats::reset);
            this.tempData.clear();
            this.flushLock.unlock();
        }

        return response;
    }

    private float addRow(Map<String, Object> row, List<List<Object>> out) {
        Object[] indexedRow = new Object[row.size()];
        Set<String> inputColumnNames = verifyInputColumns(row);
        float size = 0F;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String columnName = formatColumnName(key);
            int colIndex = fieldIndex.get(columnName).getSecond();
            RowBufferStats stats = statsMap.get(columnName);
            Utils.assertNotNull("Arrow column stats", stats);
            if (value != null) {
                ColumnDescriptor columnDescriptor = schema.getColumns().get(colIndex);
                PrimitiveType.PrimitiveTypeName typeName = columnDescriptor.getPrimitiveType().getPrimitiveTypeName();
                switch (typeName) {
                    case BOOLEAN:
                        int intValue = DataValidationUtil.validateAndParseBoolean(value);
                        value = intValue > 0;
                        stats.addIntValue(BigInteger.valueOf(intValue));
                        size += 1;
                        break;
                    case INT32:
                        int intVal = DataValidationUtil.validateAndParseInteger(value);
                        value = intVal;
                        stats.addIntValue(BigInteger.valueOf(intVal));
                        size += 4;
                        break;
                    case INT64:
                        long longValue = DataValidationUtil.validateAndParseLong(value);
                        value = longValue;
                        stats.addIntValue(BigInteger.valueOf(longValue));
                        size += 8;
                        break;
                    case DOUBLE:
                        double doubleValue = DataValidationUtil.validateAndParseReal(value);
                        value = doubleValue;
                        stats.addRealValue(doubleValue);
                        size += 8;
                        break;
                    case BINARY:
                        ColumnLogicalType logicalType = ColumnLogicalType.valueOf(fieldIndex.get(columnName).getFirst().getLogicalType());
                        String str;
                        if (logicalType.isObject()) {
                            str = logicalType == ColumnLogicalType.OBJECT ? DataValidationUtil.validateAndParseObject(value) : DataValidationUtil.validateAndParseVariant(value);
                        } else {
                            String maxLengthString = fieldIndex.get(columnName).getFirst().getLength().toString();
                            str =
                                    DataValidationUtil.validateAndParseString(
                                            value,
                                            Optional.of(maxLengthString)
                                                    .map(s -> DataValidationUtil.validateAndParseInteger(maxLengthString)));
                            stats.addStrValue(str);
                        }
                        value = str;
                        size += str.getBytes().length;
                        break;
                    default:
                        throw new ParquetEncodingException("Unsupported column type: " + typeName);
                }
            } else {
                statsMap.get(columnName).incCurrentNullCount();
            }
            indexedRow[colIndex] = value;
        }
        out.add(Arrays.asList(indexedRow));

        for (String columnName : Sets.difference(this.fieldIndex.keySet(), inputColumnNames)) {
            statsMap.get(columnName).incCurrentNullCount();
        }
        return size;
    }

    @Override
    public ChannelData<ParquetChunkData> flush() {
        logger.logDebug("Start get data for channel={}", this.owningChannel.getFullyQualifiedName());
        if (rowCount > 0) {
            List<List<Object>> oldData = new ArrayList<>();
            int oldRowCount = 0;
            float oldBufferSize = 0F;
            long oldRowSequencer = 0;
            String oldOffsetToken = null;
            Map<String, RowBufferStats> oldColumnEps = null;

            logger.logDebug(
                    "Parquet buffer flush about to take lock on channel={}",
                    this.owningChannel.getFullyQualifiedName());

            this.flushLock.lock();
            try {
                if (this.rowCount > 0) {
                    data.forEach(r -> oldData.add(new ArrayList<>(r)));
                    oldRowCount = this.rowCount;
                    oldBufferSize = this.bufferSize;
                    oldRowSequencer = this.owningChannel.incrementAndGetRowSequencer();
                    oldOffsetToken = this.owningChannel.getOffsetToken();
                    oldColumnEps = new HashMap<>(this.statsMap);
                    // Reset everything in the buffer once we save all the info
                    reset();
                }
            } finally {
                this.flushLock.unlock();
            }

            logger.logDebug(
                    "Parquet buffer flush released lock on channel={}, rowCount={}, bufferSize={}",
                    this.owningChannel.getFullyQualifiedName(),
                    rowCount,
                    bufferSize);

            ChannelData<ParquetChunkData> data = new ChannelData<>();
            data.setVectors(new ParquetChunkData(oldData, metadata));
            data.setRowCount(oldRowCount);
            data.setBufferSize(oldBufferSize);
            data.setChannel(this.owningChannel);
            data.setRowSequencer(oldRowSequencer);
            data.setOffsetToken(oldOffsetToken);
            data.setColumnEps(oldColumnEps);
            return data;
        }
        return null;
    }

    void reset() {
        data.clear();
        this.rowCount = 0;
        this.bufferSize = 0F;
        this.statsMap.replaceAll(
                (key, value) -> new RowBufferStats(value.getCollationDefinitionString()));
    }

    @Override
    public void close(String name) {
        this.fieldIndex.clear();
        logger.logInfo(
                "Trying to close parquet buffer for channel={} from function={}",
                this.owningChannel.getName(),
                name);
    }

    public float getSize() {
        return bufferSize;
    }

    public MessageType getSchema() {
        return schema;
    }

    public  List<ColumnMetadata> getColumns() {
        return columns;
    }

    public static class ParquetChunkData {
        final List<List<Object>> rows;
        final Map<String, String> metadata;

        public ParquetChunkData(List<List<Object>> rows, Map<String, String> metadata) {
            this.rows = rows;
            this.metadata = metadata;
        }
    }
}

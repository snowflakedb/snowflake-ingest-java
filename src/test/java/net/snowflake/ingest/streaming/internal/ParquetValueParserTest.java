package net.snowflake.ingest.streaming.internal;

import static java.time.ZoneOffset.UTC;
import static net.snowflake.ingest.streaming.internal.ParquetValueParser.BIT_ENCODING_BYTE_LEN;
import static net.snowflake.ingest.streaming.internal.ParquetValueParser.BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN;
import static net.snowflake.ingest.streaming.internal.ParquetValueParser.DEFINITION_LEVEL_ENCODING_BYTE_LEN;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Test;

public class ParquetValueParserTest {

  @Test
  public void parseValueFixedSB1ToInt32() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("FIXED")
            .physicalType("SB1")
            .scale(0)
            .precision(2)
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            12,
            testCol,
            PrimitiveType.PrimitiveTypeName.INT32,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedParsedValue(12)
        .expectedSize(4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(12))
        .assertMatches();
  }

  @Test
  public void parseValueFixedSB2ToInt32() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("FIXED")
            .physicalType("SB2")
            .scale(0)
            .precision(4)
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            1234,
            testCol,
            PrimitiveType.PrimitiveTypeName.INT32,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedParsedValue(1234)
        .expectedSize(4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(1234))
        .assertMatches();
  }

  @Test
  public void parseValueFixedSB4ToInt32() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("FIXED")
            .physicalType("SB4")
            .scale(0)
            .precision(9)
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            123456789,
            testCol,
            PrimitiveType.PrimitiveTypeName.INT32,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedParsedValue(123456789)
        .expectedSize(4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(123456789))
        .assertMatches();
  }

  @Test
  public void parseValueFixedSB8ToInt64() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("FIXED")
            .physicalType("SB8")
            .scale(0)
            .precision(18)
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            123456789987654321L,
            testCol,
            PrimitiveType.PrimitiveTypeName.INT64,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Long.class)
        .expectedParsedValue(123456789987654321L)
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(123456789987654321L))
        .assertMatches();
  }

  @Test
  public void parseValueFixedSB16ToByteArray() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("FIXED")
            .physicalType("SB16")
            .scale(0)
            .precision(38)
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            new BigDecimal("91234567899876543219876543211234567891"),
            testCol,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(byte[].class)
        .expectedParsedValue(
            ParquetValueParser.getSb16Bytes(
                new BigInteger("91234567899876543219876543211234567891")))
        .expectedSize(16.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(new BigInteger("91234567899876543219876543211234567891"))
        .assertMatches();
  }

  @Test
  public void parseValueFixedDecimalToInt32() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("FIXED")
            .physicalType("SB8")
            .scale(5)
            .precision(10)
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            new BigDecimal("12345.54321"),
            testCol,
            PrimitiveType.PrimitiveTypeName.DOUBLE,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Double.class)
        .expectedParsedValue(Double.valueOf("12345.54321"))
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(Double.valueOf("12345.54321"))
        .assertMatches();
  }

  @Test
  public void parseValueDouble() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("REAL")
            .physicalType("DOUBLE")
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            12345.54321d,
            testCol,
            PrimitiveType.PrimitiveTypeName.DOUBLE,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Double.class)
        .expectedParsedValue(Double.valueOf(12345.54321))
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(Double.valueOf(12345.54321))
        .assertMatches();
  }

  @Test
  public void parseValueBoolean() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("BOOLEAN")
            .physicalType("SB1")
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            true,
            testCol,
            PrimitiveType.PrimitiveTypeName.BOOLEAN,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Boolean.class)
        .expectedParsedValue(true)
        .expectedSize(BIT_ENCODING_BYTE_LEN + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(1))
        .assertMatches();
  }

  @Test
  public void parseValueBinary() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("BINARY")
            .physicalType("LOB")
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "1234abcd".getBytes(),
            testCol,
            PrimitiveType.PrimitiveTypeName.BINARY,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(byte[].class)
        .expectedParsedValue("1234abcd".getBytes())
        .expectedSize(
            BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN + 8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax("1234abcd".getBytes(StandardCharsets.UTF_8))
        .assertMatches();
  }

  @Test
  public void parseValueVariantToBinary() {
    testJsonWithLogicalType("VARIANT", true);
    testJsonWithLogicalType("VARIANT", false);
  }

  @Test
  public void parseValueObjectToBinary() {
    testJsonWithLogicalType("OBJECT", true);
    testJsonWithLogicalType("OBJECT", false);
  }

  private void testJsonWithLogicalType(String logicalType, boolean enableNewJsonParsingLogic) {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType(logicalType)
            .physicalType("BINARY")
            .nullable(true)
            .build();

    String var =
        "{\"key1\":-879869596,\"key2\":\"value2\",\"key3\":null,"
            + "\"key4\":{\"key41\":0.032437,\"key42\":\"value42\",\"key43\":null}}";
    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            var,
            testCol,
            PrimitiveType.PrimitiveTypeName.BINARY,
            rowBufferStats,
            UTC,
            0,
            enableNewJsonParsingLogic);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(String.class)
        .expectedParsedValue(var)
        .expectedSize(
            BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN
                + var.getBytes().length
                + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(null)
        .assertMatches();
  }

  @Test
  public void parseValueNullVariantToBinary() {
    testNullJsonWithLogicalType(null, true);
    testNullJsonWithLogicalType(null, false);
  }

  @Test
  public void parseValueEmptyStringVariantToBinary() {
    testNullJsonWithLogicalType("", true);
    testNullJsonWithLogicalType("", false);
  }

  @Test
  public void parseValueEmptySpaceStringVariantToBinary() {
    testNullJsonWithLogicalType("     ", true);
    testNullJsonWithLogicalType("     ", false);
  }

  private void testNullJsonWithLogicalType(String var, boolean enableNewJsonParsingLogic) {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("VARIANT")
            .physicalType("BINARY")
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            var,
            testCol,
            PrimitiveType.PrimitiveTypeName.BINARY,
            rowBufferStats,
            UTC,
            0,
            enableNewJsonParsingLogic);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(String.class)
        .expectedParsedValue(var)
        .expectedSize(0)
        .expectedMinMax(null)
        .expectedNullCount(1)
        .assertNull();
  }

  @Test
  public void parseValueArrayToBinary() {
    parseValueArrayToBinaryInternal(false);
    parseValueArrayToBinaryInternal(true);
  }

  public void parseValueArrayToBinaryInternal(boolean enableNewJsonParsingLogic) {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("ARRAY")
            .physicalType("BINARY")
            .nullable(true)
            .build();

    Map<String, String> input = new HashMap<>();
    input.put("a", "1");
    input.put("b", "2");
    input.put("c", "3");

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            input,
            testCol,
            PrimitiveType.PrimitiveTypeName.BINARY,
            rowBufferStats,
            UTC,
            0,
            enableNewJsonParsingLogic);

    String resultArray = "[{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}]";

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(String.class)
        .expectedParsedValue(resultArray)
        .expectedSize(
            BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN
                + resultArray.length()
                + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(null)
        .assertMatches();
  }

  @Test
  public void parseValueTextToBinary() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TEXT")
            .physicalType("LOB")
            .nullable(true)
            .length(56)
            .build();

    String text = "This is a sample text! Length is bigger than 32 bytes :)";

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            text,
            testCol,
            PrimitiveType.PrimitiveTypeName.BINARY,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    String result = text;

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(String.class)
        .expectedParsedValue(result)
        .expectedSize(
            BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN
                + result.length()
                + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(text) // min/max are truncated later to 32 bytes, not in the parsing step.
        .assertMatches();
  }

  @Test
  public void parseValueTimestampNtzSB4Error() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIMESTAMP_NTZ")
            .physicalType("SB4")
            .scale(0) // seconds
            .precision(9)
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    SFException exception =
        Assert.assertThrows(
            SFException.class,
            () ->
                ParquetValueParser.parseColumnValueToParquet(
                    "2013-04-28 20:57:00",
                    testCol,
                    PrimitiveType.PrimitiveTypeName.INT32,
                    rowBufferStats,
                    UTC,
                    0,
                    ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT));
    Assert.assertEquals(
        "Unknown data type for logical: TIMESTAMP_NTZ, physical: SB4.", exception.getMessage());
  }

  @Test
  public void parseValueTimestampNtzSB8ToINT64() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIMESTAMP_NTZ")
            .physicalType("SB8")
            .scale(3) // millis
            .precision(18)
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "2013-04-28T20:57:01.000",
            testCol,
            PrimitiveType.PrimitiveTypeName.INT64,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Long.class)
        .expectedParsedValue(1367182621000L)
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(1367182621000L))
        .assertMatches();
  }

  @Test
  public void parseValueTimestampNtzSB16ToByteArray() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIMESTAMP_NTZ")
            .physicalType("SB16")
            .nullable(true)
            .scale(9) // nanos
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "2022-09-18T22:05:07.123456789",
            testCol,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(byte[].class)
        .expectedParsedValue(
            ParquetValueParser.getSb16Bytes(BigInteger.valueOf(1663538707123456789L)))
        .expectedSize(16.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(1663538707123456789L))
        .assertMatches();
  }

  @Test
  public void parseValueDateToInt32() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("DATE")
            .physicalType("SB4")
            .scale(0) // seconds
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "2021-01-01",
            testCol,
            PrimitiveType.PrimitiveTypeName.INT32,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedParsedValue(Integer.valueOf(18628))
        .expectedSize(4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(18628))
        .assertMatches();
  }

  @Test
  public void parseValueTimeSB4ToInt32() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIME")
            .physicalType("SB4")
            .scale(0) // seconds
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "01:00:00",
            testCol,
            PrimitiveType.PrimitiveTypeName.INT32,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedParsedValue(3600)
        .expectedSize(4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(3600))
        .assertMatches();
  }

  @Test
  public void parseValueTimeSB8ToInt64() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIME")
            .physicalType("SB8")
            .scale(3) // milliseconds
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "01:00:00.123",
            testCol,
            PrimitiveType.PrimitiveTypeName.INT64,
            rowBufferStats,
            UTC,
            0,
            ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Long.class)
        .expectedParsedValue(3600123L)
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(3600123))
        .assertMatches();
  }

  @Test
  public void parseValueTimeSB16Error() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIME")
            .physicalType("SB16")
            .scale(9) // nanos
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats("COL1");
    SFException exception =
        Assert.assertThrows(
            SFException.class,
            () ->
                ParquetValueParser.parseColumnValueToParquet(
                    "11:00:00.12345678",
                    testCol,
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                    rowBufferStats,
                    UTC,
                    0,
                    ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT));
    Assert.assertEquals(
        "Unknown data type for logical: TIME, physical: SB16.", exception.getMessage());
  }

  /** Builder that helps to assert parsing of values to parquet types */
  private static class ParquetValueParserAssertionBuilder {
    private ParquetValueParser.ParquetBufferValue parquetBufferValue;
    private RowBufferStats rowBufferStats;
    private Class valueClass;
    private Object value;
    private float size;
    private Object minMaxStat;
    private long currentNullCount;

    static ParquetValueParserAssertionBuilder newBuilder() {
      ParquetValueParserAssertionBuilder builder = new ParquetValueParserAssertionBuilder();
      return builder;
    }

    ParquetValueParserAssertionBuilder parquetBufferValue(
        ParquetValueParser.ParquetBufferValue parquetBufferValue) {
      this.parquetBufferValue = parquetBufferValue;
      return this;
    }

    ParquetValueParserAssertionBuilder rowBufferStats(RowBufferStats rowBufferStats) {
      this.rowBufferStats = rowBufferStats;
      return this;
    }

    ParquetValueParserAssertionBuilder expectedValueClass(Class valueClass) {
      this.valueClass = valueClass;
      return this;
    }

    ParquetValueParserAssertionBuilder expectedParsedValue(Object value) {
      this.value = value;
      return this;
    }

    ParquetValueParserAssertionBuilder expectedSize(float size) {
      this.size = size;
      return this;
    }

    public ParquetValueParserAssertionBuilder expectedMinMax(Object minMaxStat) {
      this.minMaxStat = minMaxStat;
      return this;
    }

    public ParquetValueParserAssertionBuilder expectedNullCount(long currentNullCount) {
      this.currentNullCount = currentNullCount;
      return this;
    }

    void assertMatches() {
      Assert.assertEquals(valueClass, parquetBufferValue.getValue().getClass());
      if (valueClass.equals(byte[].class)) {
        Assert.assertArrayEquals((byte[]) value, (byte[]) parquetBufferValue.getValue());
      } else {
        Assert.assertEquals(value, parquetBufferValue.getValue());
      }
      Assert.assertEquals(size, parquetBufferValue.getSize(), 0);
      if (minMaxStat instanceof BigInteger) {
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMinIntValue());
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMaxIntValue());
        return;
      } else if (minMaxStat instanceof byte[]) {
        Assert.assertArrayEquals((byte[]) minMaxStat, rowBufferStats.getCurrentMinStrValue());
        Assert.assertArrayEquals((byte[]) minMaxStat, rowBufferStats.getCurrentMaxStrValue());
        return;
      } else if (valueClass.equals(String.class)) {
        // String can have null min/max stats for variant data types
        Object min =
            rowBufferStats.getCurrentMinStrValue() != null
                ? new String(rowBufferStats.getCurrentMinStrValue(), StandardCharsets.UTF_8)
                : rowBufferStats.getCurrentMinStrValue();
        Object max =
            rowBufferStats.getCurrentMaxStrValue() != null
                ? new String(rowBufferStats.getCurrentMaxStrValue(), StandardCharsets.UTF_8)
                : rowBufferStats.getCurrentMaxStrValue();
        Assert.assertEquals(minMaxStat, min);
        Assert.assertEquals(minMaxStat, max);
        return;
      } else if (minMaxStat instanceof Double || minMaxStat instanceof BigDecimal) {
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMinRealValue());
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMaxRealValue());
        return;
      }
      throw new IllegalArgumentException(
          String.format("Unknown data type for min stat: %s", minMaxStat.getClass()));
    }

    void assertNull() {
      Assert.assertNull(parquetBufferValue.getValue());
      Assert.assertEquals(currentNullCount, rowBufferStats.getCurrentNullCount());
    }
  }
}

package net.snowflake.ingest.streaming.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            12, testCol, PrimitiveType.PrimitiveTypeName.INT32, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedSize(4.0f)
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            1234, testCol, PrimitiveType.PrimitiveTypeName.INT32, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedSize(4.0f)
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            123456789, testCol, PrimitiveType.PrimitiveTypeName.INT32, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedSize(4.0f)
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            123456789987654321L, testCol, PrimitiveType.PrimitiveTypeName.INT64, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Long.class)
        .expectedSize(8.0f)
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            new BigDecimal("91234567899876543219876543211234567891"),
            testCol,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(byte[].class)
        .expectedSize(16.0f)
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            new BigDecimal("12345.54321"),
            testCol,
            PrimitiveType.PrimitiveTypeName.DOUBLE,
            rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Double.class)
        .expectedSize(8.0f)
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            12345.54321d, testCol, PrimitiveType.PrimitiveTypeName.DOUBLE, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Double.class)
        .expectedSize(8.0f)
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            true, testCol, PrimitiveType.PrimitiveTypeName.BOOLEAN, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Boolean.class)
        .expectedSize(1.0f)
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "Length7".getBytes(), testCol, PrimitiveType.PrimitiveTypeName.BINARY, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(String.class)
        .expectedSize(7.0f)
        .expectedMinMax("Length7")
        .assertMatches();
  }

  @Test
  public void parseValueVariantToBinary() {
    testJsonWithLogicalType("VARIANT");
  }

  @Test
  public void parseValueObjectToBinary() {
    testJsonWithLogicalType("OBJECT");
  }

  private void testJsonWithLogicalType(String logicalType) {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType(logicalType)
            .physicalType("BINARY")
            .nullable(true)
            .build();

    String var =
        "{\"key1\":-879869596,\"key2\":\"value2\",\"key3\":null,"
            + "\"key4\":{\"key41\":0.032437,\"key42\":\"value42\",\"key43\":null}}";
    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            var, testCol, PrimitiveType.PrimitiveTypeName.BINARY, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(String.class)
        .expectedSize(var.getBytes().length)
        .expectedMinMax(null)
        .assertMatches();
  }

  @Test
  public void parseValueArrayToBinary() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("ARRAY")
            .physicalType("BINARY")
            .nullable(true)
            .build();

    List<String> arr = Arrays.asList("{ \"a\": 1}", "{ \"b\": 2 }", "{ \"c\": 3 }");
    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            arr, testCol, PrimitiveType.PrimitiveTypeName.BINARY, rowBufferStats);

    String resultArray = "[{ \"a\": 1}, { \"b\": 2 }, { \"c\": 3 }]";

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(String.class)
        .expectedSize(resultArray.length())
        .expectedMinMax(null)
        .assertMatches();
  }

  @Test
  public void parseValueTimestampNtzSB4ToINT64() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIMESTAMP_NTZ")
            .physicalType("SB4")
            .scale(0) // seconds
            .precision(9)
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "1663531507", testCol, PrimitiveType.PrimitiveTypeName.INT32, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedSize(4.0f)
        .expectedMinMax(BigInteger.valueOf(1663531507L))
        .assertMatches();
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "1663531507000", testCol, PrimitiveType.PrimitiveTypeName.INT64, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Long.class)
        .expectedSize(8.0f)
        .expectedMinMax(BigInteger.valueOf(1663531507000000L))
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "1663531507.801809412",
            testCol,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(byte[].class)
        .expectedSize(16.0f)
        .expectedMinMax(BigInteger.valueOf(1663531507801809412L))
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "2021-01-01", testCol, PrimitiveType.PrimitiveTypeName.INT32, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedSize(4.0f)
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "01:00:00", testCol, PrimitiveType.PrimitiveTypeName.INT32, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedSize(4.0f)
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

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "01:00:00.123", testCol, PrimitiveType.PrimitiveTypeName.INT64, rowBufferStats);

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Long.class)
        .expectedSize(8.0f)
        .expectedMinMax(BigInteger.valueOf(3600123))
        .assertMatches();
  }

  @Test
  public void parseValueTimeSB16ToByteArray() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIME")
            .physicalType("SB16")
            .scale(9) // nanos
            .nullable(true)
            .build();

    RowBufferStats rowBufferStats = new RowBufferStats();
    ParquetValueParser.ParquetBufferValue pv =
        ParquetValueParser.parseColumnValueToParquet(
            "36000.12345678",
            testCol,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            rowBufferStats); // todo specifying string "11:00:00.12345678" doesn't work

    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(byte[].class)
        .expectedSize(16.0f)
        .expectedMinMax(BigInteger.valueOf(36000123456780L))
        .assertMatches();
  }

  /** Builder that helps to assert parsing of values to parquet types */
  private static class ParquetValueParserAssertionBuilder {
    private ParquetValueParser.ParquetBufferValue parquetBufferValue;
    private RowBufferStats rowBufferStats;
    private Class valueClass;
    private float size;
    private Object minMaxStat;

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

    ParquetValueParserAssertionBuilder expectedSize(float size) {
      this.size = size;
      return this;
    }

    public ParquetValueParserAssertionBuilder expectedMinMax(Object minMaxStat) {
      this.minMaxStat = minMaxStat;
      return this;
    }

    void assertMatches() {
      Assert.assertEquals(valueClass, parquetBufferValue.getValue().getClass());
      Assert.assertEquals(size, parquetBufferValue.getSize(), 0);
      if (minMaxStat instanceof BigInteger) {
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMinIntValue());
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMaxIntValue());
        return;
      } else if (minMaxStat instanceof String || valueClass.equals(String.class)) {
        // String can have null min/max stats for variant data types
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMinColStrValue());
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMaxColStrValue());
        return;
      } else if (minMaxStat instanceof Double || minMaxStat instanceof BigDecimal) {
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMinRealValue());
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMaxRealValue());
        return;
      }
      throw new IllegalArgumentException("Unknown data type for min stat");
    }
  }
}

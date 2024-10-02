package net.snowflake.ingest.streaming.internal.datatypes;

import java.math.BigDecimal;
import java.time.*;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IcebergDataTypeTest extends AbstractDataTypeTest {
  @Before
  public void before() throws Exception {
    super.before(true);
  }

  @Test
  public void testBoolean() throws Exception {
    testIcebergIngestion("boolean", true, new BooleanProvider());
    testIcebergIngestion("boolean", false, new BooleanProvider());
    testIcebergIngestion("boolean", 1, true, new BooleanProvider());
    testIcebergIngestion("boolean", "false", false, new BooleanProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () -> testIcebergIngestion("boolean", new Object(), true, new BooleanProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testInt() throws Exception {
    testIcebergIngestion("int", 1, new IntProvider());
    testIcebergIngestion("int", -.0f, 0, new IntProvider());
    testIcebergIngestion("int", 0.5f, 1, new IntProvider());
    testIcebergIngestion("int", "100.4", 100, new IntProvider());
    testIcebergIngestion("int", new BigDecimal("1000000.09"), 1000000, new IntProvider());
    testIcebergIngestion("int", Integer.MAX_VALUE, new IntProvider());
    testIcebergIngestion("int", Integer.MIN_VALUE, new IntProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () -> testIcebergIngestion("int", Long.MAX_VALUE, new LongProvider()));
    Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

    ex =
        Assert.assertThrows(
            SFException.class, () -> testIcebergIngestion("int", true, 0, new IntProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testLong() throws Exception {
    testIcebergIngestion("long", 1L, new LongProvider());
    testIcebergIngestion("long", -.0f, 0L, new LongProvider());
    testIcebergIngestion("long", 0.5f, 1L, new LongProvider());
    testIcebergIngestion("long", "100.4", 100L, new LongProvider());
    testIcebergIngestion("long", new BigDecimal("1000000.09"), 1000000L, new LongProvider());
    testIcebergIngestion("long", Long.MAX_VALUE, new LongProvider());
    testIcebergIngestion("long", Long.MIN_VALUE, new LongProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () -> testIcebergIngestion("long", Double.MAX_VALUE, new DoubleProvider()));
    Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

    ex =
        Assert.assertThrows(
            SFException.class,
            () -> testIcebergIngestion("long", Double.NaN, new DoubleProvider()));
    Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

    ex =
        Assert.assertThrows(
            SFException.class, () -> testIcebergIngestion("long", false, 0L, new LongProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testFloat() throws Exception {
    testIcebergIngestion("float", 1.0f, new FloatProvider());
    testIcebergIngestion("float", -.0f, .0f, new FloatProvider());
    testIcebergIngestion("float", Float.POSITIVE_INFINITY, new FloatProvider());
    testIcebergIngestion("float", "NaN", Float.NaN, new FloatProvider());
    testIcebergIngestion("float", new BigDecimal("1000.0"), 1000f, new FloatProvider());
    testIcebergIngestion("float", Double.MAX_VALUE, Float.POSITIVE_INFINITY, new FloatProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () -> testIcebergIngestion("float", new Object(), 1f, new FloatProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testDouble() throws Exception {
    testIcebergIngestion("double", 1.0, new DoubleProvider());
    testIcebergIngestion("double", -.0, .0, new DoubleProvider());
    testIcebergIngestion("double", Double.POSITIVE_INFINITY, new DoubleProvider());
    testIcebergIngestion("double", "NaN", Double.NaN, new DoubleProvider());
    testIcebergIngestion("double", new BigDecimal("1000.0"), 1000.0, new DoubleProvider());
    testIcebergIngestion("double", Double.MAX_VALUE, Double.MAX_VALUE, new DoubleProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () -> testIcebergIngestion("double", new Object(), 1.0, new DoubleProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testDecimal() throws Exception {
    testIcebergIngestion("decimal(3, 1)", new BigDecimal("-12.3"), new BigDecimalProvider());
    testIcebergIngestion("decimal(1, 0)", new BigDecimal("-0.0"), new BigDecimalProvider());
    testIcebergIngestion("decimal(3, 1)", 12.5f, new FloatProvider());
    testIcebergIngestion("decimal(3, 1)", -99, new IntProvider());
    testIcebergIngestion("decimal(38, 0)", Long.MAX_VALUE, new LongProvider());
    testIcebergIngestion(
        "decimal(38, 10)",
        "1234567890123456789012345678.1234567890",
        new BigDecimal("1234567890123456789012345678.1234567890"),
        new BigDecimalProvider());

    testIcebergIngestion(
        "decimal(3, 1)", "12.21999", new BigDecimal("12.2"), new BigDecimalProvider());
    testIcebergIngestion(
        "decimal(5, 0)", "12345.52199", new BigDecimal("12346"), new BigDecimalProvider());
    testIcebergIngestion(
        "decimal(5, 2)", "12345e-2", new BigDecimal("123.45"), new BigDecimalProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "decimal(3, 1)", new BigDecimal("123.23"), new BigDecimalProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testString() throws Exception {
    testIcebergIngestion("string", "test", new StringProvider());
    testIcebergIngestion("string", 123, "123", new StringProvider());
    testIcebergIngestion("string", 123.45, "123.45", new StringProvider());
    testIcebergIngestion("string", true, "true", new StringProvider());
    testIcebergIngestion(
        "string", new BigDecimal("123456.789"), "123456.789", new StringProvider());
    testIcebergIngestion("string", StringUtils.repeat("a", 16 * 1024 * 1024), new StringProvider());
    testIcebergIngestion("string", "❄️", new StringProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () -> testIcebergIngestion("string", new Object(), "test", new StringProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());

    ex =
        Assert.assertThrows(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "string", StringUtils.repeat("a", 16 * 1024 * 1024 + 1), new StringProvider()));
    Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testFixedLenByteArray() throws Exception {
    testIcebergIngestion("fixed(3)", new byte[] {1, 2, 3}, new ByteArrayProvider());
    testIcebergIngestion("fixed(3)", "313233", new byte[] {49, 50, 51}, new ByteArrayProvider());
    testIcebergIngestion("fixed(8388608)", new byte[8388608], new ByteArrayProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "fixed(10)",
                    "313233",
                    new byte[] {49, 50, 51, 0, 0, 0, 0, 0, 0, 0},
                    new ByteArrayProvider()));
    Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

    ex =
        Assert.assertThrows(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "fixed(3)", new byte[] {49, 50, 51, 52}, new ByteArrayProvider()));
    Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

    ex =
        Assert.assertThrows(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "fixed(3)", "313", new byte[] {49, 50}, new ByteArrayProvider()));
    Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

    ex =
        Assert.assertThrows(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "fixed(3)", new Object(), new byte[] {1, 2, 3, 4, 5}, new ByteArrayProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testBinary() throws Exception {
    testIcebergIngestion("binary", new byte[] {1, 2, 3}, new ByteArrayProvider());
    testIcebergIngestion("binary", "313233", new byte[] {49, 50, 51}, new ByteArrayProvider());
    testIcebergIngestion("binary", new byte[8388608], new ByteArrayProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () -> testIcebergIngestion("binary", new byte[8388608 + 1], new ByteArrayProvider()));
    Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

    ex =
        Assert.assertThrows(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "binary", new Object(), new byte[] {1, 2, 3, 4, 5}, new ByteArrayProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testDate() throws Exception {
    testIcebergIngestion("date", "9999-12-31", new StringProvider());
    testIcebergIngestion("date", "1582-10-05", new StringProvider());
    testIcebergIngestion("date", LocalDate.parse("1998-09-08"), "1998-09-08", new StringProvider());
    testIcebergIngestion(
        "date", LocalDateTime.parse("1998-09-08T02:00:00.123"), "1998-09-08", new StringProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () -> testIcebergIngestion("date", "2000-01-32", new StringProvider()));
    Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

    ex =
        Assert.assertThrows(
            SFException.class,
            () -> testIcebergIngestion("date", new Object(), "2000-01-01", new StringProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testTime() throws Exception {
    testIcebergIngestion("time", "00:00:00", "00:00:00.000000 Z", new StringProvider());
    testIcebergIngestion("time", "23:59:59", "23:59:59.000000 Z", new StringProvider());
    testIcebergIngestion("time", "12:00:00", "12:00:00.000000 Z", new StringProvider());
    testIcebergIngestion("time", "12:00:00.123", "12:00:00.123000 Z", new StringProvider());
    testIcebergIngestion("time", "12:00:00.123456", "12:00:00.123456 Z", new StringProvider());
    testIcebergIngestion("time", "12:00:00.123456789", "12:00:00.123456 Z", new StringProvider());
    testIcebergIngestion(
        "time", LocalTime.of(23, 59, 59), "23:59:59.000000 Z", new StringProvider());
    testIcebergIngestion(
        "time",
        OffsetTime.of(12, 0, 0, 123000, ZoneOffset.ofHoursMinutes(0, 0)),
        "12:00:00.000123 Z",
        new StringProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () -> testIcebergIngestion("time", "12:00:00.123456789012", new StringProvider()));
    Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

    ex =
        Assert.assertThrows(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "time",
                    LocalDateTime.parse("1998-09-08T02:00:00.123"),
                    "02:00:00",
                    new StringProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testTimestamp() throws Exception {
    testIcebergIngestion(
        "timestamp", "2000-12-31T23:59:59", "2000-12-31 23:59:59.000000 Z", new StringProvider());
    testIcebergIngestion(
        "timestamp",
        "2000-12-31T23:59:59.123456",
        "2000-12-31 23:59:59.123456 Z",
        new StringProvider());
    testIcebergIngestion(
        "timestamp",
        "2000-12-31T23:59:59.123456789+08:00",
        "2000-12-31 23:59:59.123456 Z",
        new StringProvider());
    testIcebergIngestion(
        "timestamp",
        LocalDate.parse("2000-12-31"),
        "2000-12-31 00:00:00.000000 Z",
        new StringProvider());
    testIcebergIngestion(
        "timestamp",
        LocalDateTime.parse("2000-12-31T23:59:59.123456789"),
        "2000-12-31 23:59:59.123456 Z",
        new StringProvider());
    testIcebergIngestion(
        "timestamp",
        OffsetDateTime.parse("2000-12-31T23:59:59.123456789Z"),
        "2000-12-31 23:59:59.123456 Z",
        new StringProvider());

    SFException ex =
        Assert.assertThrows(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "timestamp", "2000-12-31T23:59:59.123456789012", new StringProvider()));
    Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

    ex =
        Assert.assertThrows(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "timestamp",
                    new Object(),
                    "2000-12-31 00:00:00.000000 Z",
                    new StringProvider()));
    Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
  }

  @Test
  public void testTimestampTZ() throws Exception {
    testIcebergIngestion(
        "timestamptz", "2000-12-31T23:59:59", "2000-12-31 23:59:59.000000 Z", new StringProvider());
    testIcebergIngestion(
        "timestamptz",
        "2000-12-31T23:59:59.123456+08:00",
        "2000-12-31 23:59:59.123456 +0800",
        new StringProvider());
  }
}

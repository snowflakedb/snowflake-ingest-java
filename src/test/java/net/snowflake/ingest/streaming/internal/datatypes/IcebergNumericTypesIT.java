package net.snowflake.ingest.streaming.internal.datatypes;

import java.math.BigDecimal;
import java.util.Arrays;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("This test can be enabled after server side Iceberg EP support is released")
public class IcebergNumericTypesIT extends AbstractDataTypeTest {
  @Before
  public void before() throws Exception {
    super.before(true);
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
    testIcebergIngestion("int", null, new IntProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("int", 1L + Integer.MAX_VALUE, new LongProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class, () -> testIcebergIngestion("int", true, 0, new IntProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestionNonNullable("int", null, new IntProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testIntAndQueries() throws Exception {
    testIcebergIngestAndQuery(
        "int",
        Arrays.asList(1, -0, Integer.MAX_VALUE, Integer.MIN_VALUE, null),
        "select {columnName} from {tableName}",
        Arrays.asList(1L, 0L, (long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE, null));
    testIcebergIngestAndQuery(
        "int",
        Arrays.asList(null, null, null, Integer.MIN_VALUE),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(3L));
    testIcebergIngestAndQuery(
        "int",
        Arrays.asList(1, -0, Integer.MAX_VALUE, Integer.MIN_VALUE),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList((long) Integer.MAX_VALUE));
    testIcebergIngestAndQuery(
        "int",
        Arrays.asList(null, null, null),
        "select MIN({columnName}) from {tableName}",
        Arrays.asList((Object) null));
    testIcebergIngestAndQuery(
        "int",
        Arrays.asList(
            Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, null),
        "select COUNT({columnName}) from {tableName} where {columnName} = 2147483647",
        Arrays.asList(4L));
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
    testIcebergIngestion("long", null, new LongProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("long", Double.MAX_VALUE, new DoubleProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("long", Double.NaN, new DoubleProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class, () -> testIcebergIngestion("long", false, 0L, new LongProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestionNonNullable("long", null, new LongProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testLongAndQueries() throws Exception {
    testIcebergIngestAndQuery(
        "long",
        Arrays.asList(1L, -0L, Long.MAX_VALUE, Long.MIN_VALUE, null),
        "select {columnName} from {tableName}",
        Arrays.asList(1L, 0L, Long.MAX_VALUE, Long.MIN_VALUE, null));
    testIcebergIngestAndQuery(
        "long",
        Arrays.asList(null, null, null, Long.MIN_VALUE),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(3L));
    testIcebergIngestAndQuery(
        "long",
        Arrays.asList(1L, -0L, Long.MAX_VALUE, Long.MIN_VALUE),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(Long.MAX_VALUE));
    testIcebergIngestAndQuery(
        "long",
        Arrays.asList(null, null, null),
        "select MIN({columnName}) from {tableName}",
        Arrays.asList((Object) null));
    testIcebergIngestAndQuery(
        "long",
        Arrays.asList(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, null),
        "select COUNT({columnName}) from {tableName} where {columnName} = 9223372036854775807",
        Arrays.asList(4L));
  }

  @Test
  public void testFloat() throws Exception {
    testIcebergIngestion("float", 1.0f, new FloatProvider());
    testIcebergIngestion("float", -.0f, .0f, new FloatProvider());
    testIcebergIngestion("float", Float.POSITIVE_INFINITY, new FloatProvider());
    testIcebergIngestion("float", "NaN", Float.NaN, new FloatProvider());
    testIcebergIngestion("float", new BigDecimal("1000.0"), 1000f, new FloatProvider());
    testIcebergIngestion("float", Double.MAX_VALUE, Float.POSITIVE_INFINITY, new FloatProvider());
    testIcebergIngestion("float", null, new FloatProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("float", new Object(), 1f, new FloatProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestionNonNullable("float", null, new LongProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testFloatAndQueries() throws Exception {
    testIcebergIngestAndQuery(
        "float",
        Arrays.asList(1.0f, -0.0f, Float.POSITIVE_INFINITY, Float.NaN, null),
        "select {columnName} from {tableName}",
        Arrays.asList(1.0, -0.0, (double) Float.POSITIVE_INFINITY, (double) Float.NaN, null));
    testIcebergIngestAndQuery(
        "float",
        Arrays.asList(null, null, null, Float.NaN),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(3L));
    testIcebergIngestAndQuery(
        "float",
        Arrays.asList(1.0f, -0.0f, Float.POSITIVE_INFINITY, Float.NaN),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList((double) Float.NaN));
    testIcebergIngestAndQuery(
        "float",
        Arrays.asList(1.0f, -0.0f, Float.POSITIVE_INFINITY, null),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList((double) Float.POSITIVE_INFINITY));
    testIcebergIngestAndQuery(
        "float",
        Arrays.asList(null, null, null),
        "select MIN({columnName}) from {tableName}",
        Arrays.asList((Object) null));
    testIcebergIngestAndQuery(
        "float",
        Arrays.asList(
            Float.POSITIVE_INFINITY,
            Float.POSITIVE_INFINITY,
            Float.POSITIVE_INFINITY,
            Float.POSITIVE_INFINITY,
            null),
        "select COUNT({columnName}) from {tableName} where {columnName} = 'Infinity'",
        Arrays.asList(4L));
  }

  @Test
  public void testDouble() throws Exception {
    testIcebergIngestion("double", 1.0, new DoubleProvider());
    testIcebergIngestion("double", -.0, .0, new DoubleProvider());
    testIcebergIngestion("double", Double.POSITIVE_INFINITY, new DoubleProvider());
    testIcebergIngestion("double", "NaN", Double.NaN, new DoubleProvider());
    testIcebergIngestion("double", new BigDecimal("1000.0"), 1000.0, new DoubleProvider());
    testIcebergIngestion("double", Double.MAX_VALUE, Double.MAX_VALUE, new DoubleProvider());
    testIcebergIngestion("double", null, new DoubleProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("double", new Object(), 1.0, new DoubleProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestionNonNullable("double", null, new LongProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testDoubleAndQueries() throws Exception {
    testIcebergIngestAndQuery(
        "double",
        Arrays.asList(1.0, -0.0, Double.POSITIVE_INFINITY, Double.NaN, null),
        "select {columnName} from {tableName}",
        Arrays.asList(1.0, -0.0, Double.POSITIVE_INFINITY, Double.NaN, null));
    testIcebergIngestAndQuery(
        "double",
        Arrays.asList(null, null, null, Double.NaN),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(3L));
    testIcebergIngestAndQuery(
        "double",
        Arrays.asList(1.0, -0.0, Double.POSITIVE_INFINITY, Double.NaN),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(Double.NaN));
    testIcebergIngestAndQuery(
        "double",
        Arrays.asList(1.0, -0.0, Double.POSITIVE_INFINITY, null),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(Double.POSITIVE_INFINITY));
    testIcebergIngestAndQuery(
        "double",
        Arrays.asList(null, null, null),
        "select MIN({columnName}) from {tableName}",
        Arrays.asList((Object) null));
    testIcebergIngestAndQuery(
        "double",
        Arrays.asList(
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            null),
        "select COUNT({columnName}) from {tableName} where {columnName} = 'Infinity'",
        Arrays.asList(4L));
  }

  @Test
  public void testDecimal() throws Exception {
    testIcebergIngestion("decimal(3, 1)", new BigDecimal("-12.3"), new BigDecimalProvider());
    testIcebergIngestion("decimal(1, 0)", new BigDecimal("-0.0"), new BigDecimalProvider());
    testIcebergIngestion("decimal(3, 1)", 12.5f, new FloatProvider());
    testIcebergIngestion("decimal(3, 1)", -99, new IntProvider());
    testIcebergIngestion("decimal(38, 0)", Long.MAX_VALUE, new LongProvider());
    testIcebergIngestion("decimal(38, 10)", null, new BigDecimalProvider());

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
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "decimal(3, 1)", new BigDecimal("123.23"), new BigDecimalProvider()));

    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestionNonNullable("decimal(38, 10)", null, new BigDecimalProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testDecimalAndQueries() throws Exception {
    testIcebergIngestAndQuery(
        "decimal(3, 1)",
        Arrays.asList(new BigDecimal("-12.3"), new BigDecimal("-0.0"), null),
        "select {columnName} from {tableName}",
        Arrays.asList(new BigDecimal("-12.3"), new BigDecimal("-0.0"), null));
    testIcebergIngestAndQuery(
        "decimal(38, 10)",
        Arrays.asList(null, null, null),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(3L));
    testIcebergIngestAndQuery(
        "decimal(32, 10)",
        Arrays.asList(new BigDecimal("-233333.3"), new BigDecimal("-23.03"), null),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(new BigDecimal("-23.03")));
    testIcebergIngestAndQuery(
        "decimal(11, 1)",
        Arrays.asList(new BigDecimal("-1222222222.3"), new BigDecimal("-0.0"), null),
        "select MIN({columnName}) from {tableName}",
        Arrays.asList(new BigDecimal("-1222222222.3")));
    testIcebergIngestAndQuery(
        "decimal(3, 1)",
        Arrays.asList(new BigDecimal("-12.3"), new BigDecimal("-12.3"), null),
        "select COUNT({columnName}) from {tableName} where {columnName} = -12.3",
        Arrays.asList(2L));
  }
}

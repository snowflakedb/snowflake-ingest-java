package net.snowflake.ingest.streaming.internal.datatypes;

import java.math.BigDecimal;
import java.util.Arrays;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("This test can be enabled after server side Iceberg EP support is released")
public class IcebergStringIT extends AbstractDataTypeTest {
  @Before
  public void before() throws Exception {
    super.before(true);
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
    testIcebergIngestion("string", null, new StringProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("string", new Object(), "test", new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "string", StringUtils.repeat("a", 16 * 1024 * 1024 + 1), new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestionNonNullable("string", null, new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testStringAndQueries() throws Exception {
    testIcebergIngestAndQuery(
        "string",
        Arrays.asList("test", "test2", "test3", null, "❄️"),
        "select {columnName} from {tableName}",
        Arrays.asList("test", "test2", "test3", null, "❄️"));
    testIcebergIngestAndQuery(
        "string", Arrays.asList(null, null, null, null, "aaa"),
        "select COUNT(*) from {tableName} where {columnName} is null", Arrays.asList(4L));
    testIcebergIngestAndQuery(
        "string",
        Arrays.asList(StringUtils.repeat("a", 16 * 1024 * 1024), null, null, null, "aaa"),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(StringUtils.repeat("a", 16 * 1024 * 1024)));
    testIcebergIngestAndQuery(
        "string",
        Arrays.asList(StringUtils.repeat("a", 33), StringUtils.repeat("*", 3), null, ""),
        "select MAX(LENGTH({columnName})) from {tableName}",
        Arrays.asList(33L));
  }
}

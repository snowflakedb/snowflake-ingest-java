/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.datatypes;

import java.math.BigDecimal;
import java.util.Arrays;
import net.snowflake.ingest.IcebergIT;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(IcebergIT.class)
@RunWith(Parameterized.class)
public class IcebergStringIT extends AbstractDataTypeTest {
  @Parameterized.Parameters(name = "compressionAlgorithm={0}, icebergSerializationPolicy={1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"ZSTD", Constants.IcebergSerializationPolicy.COMPATIBLE},
      {"ZSTD", Constants.IcebergSerializationPolicy.OPTIMIZED}
    };
  }

  @Parameterized.Parameter(0)
  public static String compressionAlgorithm;

  @Parameterized.Parameter(1)
  public static Constants.IcebergSerializationPolicy icebergSerializationPolicy;

  @Before
  public void before() throws Exception {
    super.setUp(true, compressionAlgorithm, icebergSerializationPolicy);
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
            () -> testIcebergIngestion("string not null", null, new StringProvider()));
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
        Arrays.asList(StringUtils.repeat("a", 33), StringUtils.repeat("*", 3), null, ""),
        "select MAX(LENGTH({columnName})) from {tableName}",
        Arrays.asList(33L));

    // TODO: Change to 16MB after SNOW-1798403 fixed
    testIcebergIngestAndQuery(
        "string",
        Arrays.asList(StringUtils.repeat("a", 16 * 1024 * 1024 - 1), null, null, null, "aaa"),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(StringUtils.repeat("a", 16 * 1024 * 1024 - 1)));
  }
}

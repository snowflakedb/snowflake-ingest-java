/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.datatypes;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import net.snowflake.ingest.IcebergIT;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(IcebergIT.class)
@RunWith(Parameterized.class)
public class IcebergDateTimeIT extends AbstractDataTypeTest {
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
  public void testDate() throws Exception {
    testIcebergIngestion("date", "9999-12-31", new StringProvider());
    testIcebergIngestion("date", "1582-10-05", new StringProvider());
    testIcebergIngestion("date", LocalDate.parse("1998-09-08"), "1998-09-08", new StringProvider());
    testIcebergIngestion(
        "date", LocalDateTime.parse("1998-09-08T02:00:00.123"), "1998-09-08", new StringProvider());
    testIcebergIngestion("date", null, new StringProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("date", "2000-01-32", new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("date", new Object(), "2000-01-01", new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("date not null", null, new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testDateAndQueries() throws Exception {
    testIcebergIngestAndQuery(
        "date",
        Arrays.asList(
            "1998-09-08",
            LocalDate.parse("1998-09-11"),
            LocalDateTime.parse("1998-09-05T02:00:00.123")),
        "select {columnName} from {tableName}",
        Arrays.asList(
            Date.valueOf("1998-09-08"), Date.valueOf("1998-09-11"), Date.valueOf("1998-09-05")));
    testIcebergIngestAndQuery(
        "date",
        Arrays.asList(null, null, null),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList((Object) null));
    testIcebergIngestAndQuery(
        "date",
        Arrays.asList("9999-12-31", "2024-10-08", "0000-01-01"),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(Date.valueOf("9999-12-31")));
    testIcebergIngestAndQuery(
        "date",
        Arrays.asList(null, "2001-01-01", "2000-01-01", null, null),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(3L));
    testIcebergIngestAndQuery(
        "date",
        Arrays.asList("2001-01-01", "2001-01-01", "2001-01-01", "2001-01-01", null),
        "select COUNT(*) from {tableName} where {columnName} = '2001-01-01'",
        Arrays.asList(4L));
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
    testIcebergIngestion("time", null, new StringProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("time", "12:00:00.123456789012", new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "time",
                    LocalDateTime.parse("1998-09-08T02:00:00.123"),
                    "02:00:00",
                    new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("time not null", null, new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testTimeAndQueries() throws Exception {
    testIcebergIngestAndQuery(
        "time",
        Arrays.asList(
            "00:00:00",
            LocalTime.of(23, 59, 59),
            OffsetTime.of(12, 0, 0, 123000, ZoneOffset.ofHoursMinutes(0, 0))),
        "select {columnName} from {tableName}",
        Arrays.asList(
            Time.valueOf("00:00:00"), Time.valueOf("23:59:59"), Time.valueOf("12:00:00")));
    testIcebergIngestAndQuery(
        "time",
        Arrays.asList(null, null, null),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList((Object) null));
    testIcebergIngestAndQuery(
        "time",
        Arrays.asList("23:59:59", "12:00:00", "00:00:00"),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(Time.valueOf("23:59:59")));
    testIcebergIngestAndQuery(
        "time",
        Arrays.asList(null, "12:00:00", "12:00:00", null, null),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(3L));
    testIcebergIngestAndQuery(
        "time",
        Arrays.asList("12:00:00", "12:00:00", "12:00:00", "12:00:00", null),
        "select COUNT(*) from {tableName} where {columnName} = '12:00:00'",
        Arrays.asList(4L));
  }

  @Test
  public void testTimestamp() throws Exception {
    testIcebergIngestion(
        "timestamp_ntz(6)",
        "2000-12-31T23:59:59",
        "2000-12-31 23:59:59.000000 Z",
        new StringProvider());
    testIcebergIngestion(
        "timestamp_ntz(6)",
        "2000-12-31T23:59:59.123456",
        "2000-12-31 23:59:59.123456 Z",
        new StringProvider());
    testIcebergIngestion(
        "timestamp_ntz(6)",
        "2000-12-31T23:59:59.123456789+08:00",
        "2000-12-31 23:59:59.123456 Z",
        new StringProvider());
    testIcebergIngestion(
        "timestamp_ntz(6)",
        LocalDate.parse("2000-12-31"),
        "2000-12-31 00:00:00.000000 Z",
        new StringProvider());
    testIcebergIngestion(
        "timestamp_ntz(6)",
        LocalDateTime.parse("2000-12-31T23:59:59.123456789"),
        "2000-12-31 23:59:59.123456 Z",
        new StringProvider());
    testIcebergIngestion(
        "timestamp_ntz(6)",
        OffsetDateTime.parse("2000-12-31T23:59:59.123456789Z"),
        "2000-12-31 23:59:59.123456 Z",
        new StringProvider());
    testIcebergIngestion("timestamp_ntz(6)", null, new StringProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "timestamp", "2000-12-31T23:59:59.123456789012", new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "timestamp",
                    new Object(),
                    "2000-12-31 00:00:00.000000 Z",
                    new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("timestamp_ntz(6) not null", null, new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testTimestampAndQueries() throws Exception {
    testIcebergIngestAndQuery(
        "timestamp_ntz(6)",
        Arrays.asList(
            "2000-12-31T23:59:59",
            LocalDateTime.parse("2000-12-31T23:59:59.123456789"),
            OffsetDateTime.parse("2000-12-31T23:59:59.123456789Z")),
        "select {columnName} from {tableName}",
        Arrays.asList(
            Timestamp.valueOf("2000-12-31 23:59:59"),
            Timestamp.valueOf("2000-12-31 23:59:59.123456"),
            Timestamp.valueOf("2000-12-31 23:59:59.123456")));
    testIcebergIngestAndQuery(
        "timestamp_ntz(6)",
        Arrays.asList(null, null, null),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList((Object) null));
    testIcebergIngestAndQuery(
        "timestamp_ntz(6)",
        Arrays.asList(
            "2000-12-31T23:59:59",
            "2000-12-31T23:59:59.123456789",
            "2000-12-31T23:59:59.123456789"),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(Timestamp.valueOf("2000-12-31 23:59:59.123456")));
    testIcebergIngestAndQuery(
        "timestamp_ntz(6)",
        Arrays.asList(null, "2000-12-31T23:59:59", "2000-12-31T23:59:59", null, null),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(3L));
    testIcebergIngestAndQuery(
        "timestamp_ntz(6)",
        Arrays.asList(
            "2000-12-31T23:59:59",
            "2000-12-31T23:59:59",
            "2000-12-31T23:59:59",
            "2000-12-31T23:59:59",
            null),
        "select COUNT(*) from {tableName} where {columnName} = '2000-12-31T23:59:59'",
        Arrays.asList(4L));
  }

  @Test
  public void testTimestampTZ() throws Exception {
    conn.createStatement().execute("alter session set timezone = 'UTC';");
    testIcebergIngestion(
        "timestamp_ltz(6)",
        "2000-12-31T23:59:59.000000+08:00",
        "2000-12-31 15:59:59.000000 Z",
        new StringProvider());
    testIcebergIngestion(
        "timestamp_ltz(6)",
        "2000-12-31T23:59:59.123456789+00:00",
        "2000-12-31 23:59:59.123456 Z",
        new StringProvider());
    testIcebergIngestion(
        "timestamp_ltz(6)",
        "2000-12-31T23:59:59.123456-08:00",
        "2001-01-01 07:59:59.123456 Z",
        new StringProvider());
    testIcebergIngestion("timestamp_ltz(6)", null, new StringProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "timestamp", "2000-12-31T23:59:59.123456789012+08:00", new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "timestamp",
                    new Object(),
                    "2000-12-31 00:00:00.000000 Z",
                    new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("timestamp_ltz(6) not null", null, new StringProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testTimestampTZAndQueries() throws Exception {
    conn.createStatement().execute("alter session set timezone = 'UTC';");
    testIcebergIngestAndQuery(
        "timestamp_ltz(6)",
        Arrays.asList(
            "2000-12-31T23:59:59.000000+08:00",
            "2000-12-31T23:59:59.123456789+00:00",
            "2000-12-31T23:59:59.123456-08:00"),
        "select {columnName} from {tableName}",
        Arrays.asList(
            Timestamp.valueOf("2000-12-31 15:59:59"),
            Timestamp.valueOf("2000-12-31 23:59:59.123456"),
            Timestamp.valueOf("2001-01-01 07:59:59.123456")));
    testIcebergIngestAndQuery(
        "timestamp_ltz(6)",
        Arrays.asList(null, null, null),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList((Object) null));
    testIcebergIngestAndQuery(
        "timestamp_ltz(6)",
        Arrays.asList(
            "2000-12-31T23:59:59.000000+08:00",
            "2000-12-31T23:59:59.123456789+00:00",
            "2000-12-31T23:59:59.123456-08:00"),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(Timestamp.valueOf("2001-01-01 07:59:59.123456")));
    testIcebergIngestAndQuery(
        "timestamp_ltz(6)",
        Arrays.asList(
            null,
            "2000-12-31T23:59:59.000000+08:00",
            "2000-12-31T23:59:59.000000+08:00",
            null,
            null),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(3L));
    testIcebergIngestAndQuery(
        "timestamp_ltz(6)",
        Arrays.asList(
            "2000-12-31T23:59:59.000000+08:00",
            "2000-12-31T23:59:59.000000+08:00",
            "2000-12-31T23:59:59.000000+08:00",
            "2000-12-31T23:59:59.000000+08:00",
            null),
        "select COUNT(*) from {tableName} where {columnName} = '2000-12-31T23:59:59.000000+08:00'",
        Arrays.asList(4L));
  }
}

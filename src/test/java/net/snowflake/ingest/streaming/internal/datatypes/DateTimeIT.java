package net.snowflake.ingest.streaming.internal.datatypes;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DateTimeIT extends AbstractDataTypeTest {
  private static final ZoneId TZ_LOS_ANGELES = ZoneId.of("America/Los_Angeles");
  private static final ZoneId TZ_BERLIN = ZoneId.of("Europe/Berlin");
  private static final ZoneId TZ_TOKYO = ZoneId.of("Asia/Tokyo");

  @Parameters(name = "{index}: {0}")
  public static Object[] parameters() {
    return new Object[] {"GZIP", "ZSTD"};
  }

  @Parameter public String compressionAlgorithm;

  @Before
  public void setup() throws Exception {
    super.setUp(false, compressionAlgorithm, null);
    // Set to a random time zone not to interfere with any of the tests
    conn.createStatement().execute("alter session set timezone = 'America/New_York';");
  }

  @After
  public void tearDown() throws Exception {
    if (conn != null) {
      conn.createStatement().execute("alter session unset timezone;");
    }
  }

  @Test
  public void testDefaultTimezone() throws Exception {
    testIngestion(
        "TIMESTAMP_TZ", "2000-01-01", "2000-01-01 00:00:00.000000000 -0800", new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ", "2000-01-01", "2000-01-01 03:00:00.000000000 -0500", new StringProvider());
  }

  @Test
  public void testTimestampWithTimeZone() throws Exception {
    setJdbcSessionTimezone(TZ_LOS_ANGELES);
    setChannelDefaultTimezone(TZ_LOS_ANGELES);

    // Test timestamp formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28T20:57",
        "2013-04-28 20:57:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28T20:57:01",
        "2013-04-28 20:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28T20:57:01-07:00",
        "2013-04-28 20:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28T20:57:01.123456",
        "2013-04-28 20:57:01.123456000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28T20:57:01.123456789+07:00",
        "2013-04-28 20:57:01.123456789 +0700",
        new StringProvider(),
        new StringProvider());
    // Here we test ingestion only because JDBC cannot ingest ISO_ZONED_DATE_TIME
    testIngestion(
        "TIMESTAMP_TZ",
        "2013-04-28T20:57:01.123456789+09:00[Asia/Tokyo]",
        "2013-04-28 20:57:01.123456789 +0900",
        new StringProvider());

    // Test date formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28",
        "2013-04-28 00:00:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());

    // Test leap years
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2024-02-29T23:59:59.999999999Z",
        "2024-02-29 23:59:59.999999999 Z",
        new StringProvider(),
        new StringProvider());
    expectNotSupported("TIMESTAMP_TZ", "2023-02-29T23:59:59.999999999");

    // Numeric strings
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "0",
        "1970-01-01 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "1674478926",
        "2023-01-23 13:02:06.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "1674478926123",
        "2023-01-23 13:02:06.123000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "1674478926123456",
        "2023-01-23 13:02:06.123456000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "1674478926123456789",
        "2023-01-23 13:02:06.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "-1674478926",
        "1916-12-09 10:57:54.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "-1674478926123",
        "1916-12-09 10:57:53.877000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "-1674478926123456",
        "1916-12-09 10:57:53.876544000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "-1674478926123456789",
        "1916-12-09 10:57:53.876543211 Z",
        new StringProvider(),
        new StringProvider());

    // java.time.LocalDate
    testIngestion(
        "TIMESTAMP_TZ",
        LocalDate.parse("2007-12-03"),
        "2007-12-03 00:00:00.000000000 -0800",
        new StringProvider());
    // java.time.LocalDateTime
    testIngestion(
        "TIMESTAMP_TZ",
        LocalDateTime.parse("2007-12-03T10:15:30"),
        "2007-12-03 10:15:30.000000000 -0800",
        new StringProvider());
    // java.time.OffsetDateTime
    testIngestion(
        "TIMESTAMP_TZ",
        OffsetDateTime.parse("2007-12-03T10:15:30+01:00"),
        "2007-12-03 10:15:30.000000000 +0100",
        new StringProvider());
    // java.time.ZonedDateTime
    testIngestion(
        "TIMESTAMP_TZ",
        ZonedDateTime.parse("2007-12-03T10:15:30.000456789+01:00[Europe/Paris]"),
        "2007-12-03 10:15:30.000456789 +0100",
        new StringProvider());
    // java.time.Instant
    testIngestion(
        "TIMESTAMP_TZ",
        OffsetDateTime.parse("2007-12-03T10:15:30.123456789+00:00").toInstant(),
        "2007-12-03 10:15:30.123456789 Z",
        new StringProvider());

    // Verify that default timezone has impact on input without timezone information
    setChannelDefaultTimezone(TZ_BERLIN);
    testIngestion(
        "TIMESTAMP_TZ",
        "2013-04-28T20:57:01.123456789",
        "2013-04-28 20:57:01.123456789 +0200",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ",
        LocalDate.parse("2007-12-03"),
        "2007-12-03 00:00:00.000000000 +0100",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ",
        LocalDateTime.parse("2007-12-03T10:15:30"),
        "2007-12-03 10:15:30.000000000 +0100",
        new StringProvider());

    // No impact on integer-stored and instant as they are always UTC
    testIngestion(
        "TIMESTAMP_TZ",
        "1674478926123456789",
        "2023-01-23 13:02:06.123456789 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ",
        OffsetDateTime.parse("2007-12-03T10:15:30.123456789+00:00").toInstant(),
        "2007-12-03 10:15:30.123456789 Z",
        new StringProvider());

    // Limited scale
    testIngestion(
        "TIMESTAMP_TZ(0)",
        "2022-01-31T13:00:00+09:00",
        "2022-01-31 13:00:00. +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ(0)",
        "2022-01-31T13:00:00.123456+09:00",
        "2022-01-31 13:00:00. +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ(1)",
        "2022-01-31T13:00:00+09:00",
        "2022-01-31 13:00:00.0 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ(1)",
        "2022-01-31T13:00:00.123456+09:00",
        "2022-01-31 13:00:00.1 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ(2)",
        "2022-01-31T13:00:00.1+09:00",
        "2022-01-31 13:00:00.10 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ(2)",
        "2022-01-31T13:00:00.123456+09:00",
        "2022-01-31 13:00:00.12 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ(8)",
        "2022-01-31T13:00:00.1+09:00",
        "2022-01-31 13:00:00.10000000 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ(8)",
        "2022-01-31T13:00:00.123456789+09:00",
        "2022-01-31 13:00:00.12345678 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ(8)",
        "2022-01-31T13:00:00.000000009+09:00",
        "2022-01-31 13:00:00.00000000 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ(8)",
        "2022-01-31T13:00:00.000000019+09:00",
        "2022-01-31 13:00:00.00000001 +0900",
        new StringProvider());
  }

  @Test
  public void testTimestampWithLocalTimeZone() throws Exception {
    setJdbcSessionTimezone(TZ_LOS_ANGELES);
    setChannelDefaultTimezone(TZ_LOS_ANGELES);

    // Test timestamp formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28T20:57",
        "2013-04-28 20:57:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28T20:57:01",
        "2013-04-28 20:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28T20:57:01-07:00",
        "2013-04-28 20:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28T20:57:01.123456",
        "2013-04-28 20:57:01.123456000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28T20:57:01.123456789+07:00",
        "2013-04-28 06:57:01.123456789 -0700",
        new StringProvider(),
        new StringProvider());
    // Here we test ingestion only because JDBC cannot ingest ISO_ZONED_DATE_TIME
    testIngestion(
        "TIMESTAMP_LTZ",
        "2013-04-28T20:57:01.123456789+09:00[Asia/Tokyo]",
        "2013-04-28 04:57:01.123456789 -0700",
        new StringProvider());

    // Test date formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28",
        "2013-04-28 00:00:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());

    // Test boundary time zone
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28T00:00:00+07:00",
        "2013-04-27 10:00:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28T00:00:00-07:00",
        "2013-04-28 00:00:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());

    // Test leap years
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2024-02-29T23:59:59.999999999Z",
        "2024-02-29 15:59:59.999999999 -0800",
        new StringProvider(),
        new StringProvider());
    expectNotSupported("TIMESTAMP_LTZ", "2023-02-29T23:59:59.999999999");

    // Numeric strings
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "0",
        "1969-12-31 16:00:00.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "1674478926",
        "2023-01-23 05:02:06.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "1674478926123",
        "2023-01-23 05:02:06.123000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "1674478926123456",
        "2023-01-23 05:02:06.123456000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "1674478926123456789",
        "2023-01-23 05:02:06.123456789 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "-1674478926",
        "1916-12-09 02:57:54.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "-1674478926123",
        "1916-12-09 02:57:53.877000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "-1674478926123456",
        "1916-12-09 02:57:53.876544000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "-1674478926123456789",
        "1916-12-09 02:57:53.876543211 -0800",
        new StringProvider(),
        new StringProvider());

    // java.time.LocalDate
    testIngestion(
        "TIMESTAMP_LTZ",
        LocalDate.parse("2007-12-03"),
        "2007-12-03 00:00:00.000000000 -0800",
        new StringProvider());
    // java.time.LocalDateTime
    testIngestion(
        "TIMESTAMP_LTZ",
        LocalDateTime.parse("2007-12-03T10:15:30"),
        "2007-12-03 10:15:30.000000000 -0800",
        new StringProvider());
    // java.time.OffsetDateTime
    testIngestion(
        "TIMESTAMP_LTZ",
        OffsetDateTime.parse("2007-12-03T10:15:30+01:00"),
        "2007-12-03 01:15:30.000000000 -0800",
        new StringProvider());
    // java.time.ZonedDateTime
    testIngestion(
        "TIMESTAMP_LTZ",
        ZonedDateTime.parse("2007-12-03T10:15:30.123456789+01:00[Europe/Paris]"),
        "2007-12-03 01:15:30.123456789 -0800",
        new StringProvider());
    // java.time.Instant
    testIngestion(
        "TIMESTAMP_LTZ",
        OffsetDateTime.parse("2007-12-03T10:15:30.123456789+00:00").toInstant(),
        "2007-12-03 02:15:30.123456789 -0800",
        new StringProvider());

    // Verify that default timezone has impact on input without timezone information
    setChannelDefaultTimezone(TZ_BERLIN);
    setJdbcSessionTimezone(TZ_BERLIN);
    testIngestion(
        "TIMESTAMP_LTZ",
        "1674478926123456789",
        "2023-01-23 14:02:06.123456789 +0100",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ",
        "2013-04-28T20:57:01.123456789",
        "2013-04-28 20:57:01.123456789 +0200",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ",
        LocalDate.parse("2007-12-03"),
        "2007-12-03 00:00:00.000000000 +0100",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ",
        LocalDateTime.parse("2007-12-03T10:15:30"),
        "2007-12-03 10:15:30.000000000 +0100",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ",
        OffsetDateTime.parse("2007-12-03T10:15:30.123456789+00:00").toInstant(),
        "2007-12-03 11:15:30.123456789 +0100",
        new StringProvider());

    // Limited scale
    setChannelDefaultTimezone(TZ_TOKYO);
    setJdbcSessionTimezone(TZ_TOKYO);
    testIngestion(
        "TIMESTAMP_LTZ(0)",
        "2022-01-31T13:00:00+09:00",
        "2022-01-31 13:00:00. +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ(0)",
        "2022-01-31T13:00:00.123456+09:00",
        "2022-01-31 13:00:00. +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ(1)",
        "2022-01-31T13:00:00+09:00",
        "2022-01-31 13:00:00.0 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ(1)",
        "2022-01-31T13:00:00.123456+09:00",
        "2022-01-31 13:00:00.1 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ(2)",
        "2022-01-31T13:00:00.1+09:00",
        "2022-01-31 13:00:00.10 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ(2)",
        "2022-01-31T13:00:00.123456+09:00",
        "2022-01-31 13:00:00.12 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ(8)",
        "2022-01-31T13:00:00.1+09:00",
        "2022-01-31 13:00:00.10000000 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ(8)",
        "2022-01-31T13:00:00.123456789+09:00",
        "2022-01-31 13:00:00.12345678 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ(8)",
        "2022-01-31T13:00:00.000000009+09:00",
        "2022-01-31 13:00:00.00000000 +0900",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ(8)",
        "2022-01-31T13:00:00.000000019+09:00",
        "2022-01-31 13:00:00.00000001 +0900",
        new StringProvider());
  }

  @Test
  public void testTimestampWithoutTimeZone() throws Exception {
    // Test timestamp formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28T20:57",
        "2013-04-28 20:57:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28T20:57:01",
        "2013-04-28 20:57:01.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28T20:57:01-07:00",
        "2013-04-28 20:57:01.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28T20:57:01.123456",
        "2013-04-28 20:57:01.123456000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28T20:57:01.123456789+07:00",
        "2013-04-28 20:57:01.123456789 Z",
        new StringProvider(),
        new StringProvider());
    // Here we test ingestion only because JDBC cannot ingest ISO_ZONED_DATE_TIME
    testIngestion(
        "TIMESTAMP_NTZ",
        "2013-04-28T20:57:01.123456789+09:00[Asia/Tokyo]",
        "2013-04-28 20:57:01.123456789 Z",
        new StringProvider());

    // Test date formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28",
        "2013-04-28 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());

    // Test boundary time zone
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28T00:00:00+07:00",
        "2013-04-28 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28T00:00:00-07:00",
        "2013-04-28 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());

    // Test leap years
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2024-02-29T23:59:59.999999999Z",
        "2024-02-29 23:59:59.999999999 Z",
        new StringProvider(),
        new StringProvider());
    expectNotSupported("TIMESTAMP_NTZ", "2023-02-29T23:59:59.999999999");

    // Numeric strings
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "0",
        "1970-01-01 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "1674478926",
        "2023-01-23 13:02:06.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "1674478926123",
        "2023-01-23 13:02:06.123000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "1674478926123456",
        "2023-01-23 13:02:06.123456000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "1674478926123456789",
        "2023-01-23 13:02:06.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "-1674478926",
        "1916-12-09 10:57:54.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "-1674478926123",
        "1916-12-09 10:57:53.877000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "-1674478926123456",
        "1916-12-09 10:57:53.876544000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "-1674478926123456789",
        "1916-12-09 10:57:53.876543211 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "31536000001",
        "1971-01-01 00:00:00.001000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "31535999999999",
        "2969-05-02 23:59:59.999000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "31536000000000",
        "1971-01-01 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "31535999999999",
        "2969-05-02 23:59:59.999000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "31536000000000000",
        "1971-01-01 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());

    // java.time.LocalDate
    testIngestion(
        "TIMESTAMP_NTZ",
        LocalDate.parse("2007-12-03"),
        "2007-12-03 00:00:00.000000000 Z",
        new StringProvider());
    // java.time.LocalDateTime
    testIngestion(
        "TIMESTAMP_NTZ",
        LocalDateTime.parse("2007-12-03T10:15:30"),
        "2007-12-03 10:15:30.000000000 Z",
        new StringProvider());
    // java.time.OffsetDateTime
    testIngestion(
        "TIMESTAMP_NTZ",
        OffsetDateTime.parse("2007-12-03T10:15:30+01:00"),
        "2007-12-03 10:15:30.000000000 Z",
        new StringProvider());
    // java.time.ZonedDateTime
    testIngestion(
        "TIMESTAMP_NTZ",
        ZonedDateTime.parse("2007-12-03T10:15:30.123456789+01:00[Europe/Paris]"),
        "2007-12-03 10:15:30.123456789 Z",
        new StringProvider());
    // java.time.Instant
    testIngestion(
        "TIMESTAMP_NTZ",
        OffsetDateTime.parse("2007-12-03T10:15:30.123456789+00:00").toInstant(),
        "2007-12-03 10:15:30.123456789 Z",
        new StringProvider());

    // Verify that default timezone has no impact
    setChannelDefaultTimezone(TZ_LOS_ANGELES);
    testIngestion(
        "TIMESTAMP_NTZ",
        "1674478926123456789",
        "2023-01-23 13:02:06.123456789 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ",
        "2013-04-28T20:57:01.123456789",
        "2013-04-28 20:57:01.123456789 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ",
        LocalDate.parse("2007-12-03"),
        "2007-12-03 00:00:00.000000000 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ",
        LocalDateTime.parse("2007-12-03T10:15:30"),
        "2007-12-03 10:15:30.000000000 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ",
        OffsetDateTime.parse("2007-12-03T10:15:30.123456789+00:00").toInstant(),
        "2007-12-03 10:15:30.123456789 Z",
        new StringProvider());

    // Limited scale
    testIngestion(
        "TIMESTAMP_NTZ(0)",
        "2022-01-31T13:00:00+09:00",
        "2022-01-31 13:00:00. Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ(0)",
        "2022-01-31T13:00:00.123456+09:00",
        "2022-01-31 13:00:00. Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ(1)",
        "2022-01-31T13:00:00+09:00",
        "2022-01-31 13:00:00.0 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ(1)",
        "2022-01-31T13:00:00.123456+09:00",
        "2022-01-31 13:00:00.1 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ(2)",
        "2022-01-31T13:00:00.1+09:00",
        "2022-01-31 13:00:00.10 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ(2)",
        "2022-01-31T13:00:00.123456+09:00",
        "2022-01-31 13:00:00.12 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ(8)",
        "2022-01-31T13:00:00.1+09:00",
        "2022-01-31 13:00:00.10000000 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ(8)",
        "2022-01-31T13:00:00.123456789+09:00",
        "2022-01-31 13:00:00.12345678 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ(8)",
        "2022-01-31T13:00:00.000000009+09:00",
        "2022-01-31 13:00:00.00000000 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ(8)",
        "2022-01-31T13:00:00.000000019+09:00",
        "2022-01-31 13:00:00.00000001 Z",
        new StringProvider());
  }

  @Test
  public void testTime() throws Exception {
    // Simple time parsing
    testJdbcTypeCompatibility(
        "TIME", "20:57", "20:57:00.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "20:57:01", "20:57:01.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME",
        "20:57:01.123456789",
        "20:57:01.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME",
        "20:57:01.123456789+07:00",
        "20:57:01.123456789 Z",
        new StringProvider(),
        new StringProvider());

    // Numeric strings
    testJdbcTypeCompatibility(
        "TIME", "0", "00:00:00.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "1674478926", "13:02:06.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME",
        "1674478926123",
        "13:02:06.123000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME",
        "1674478926123456",
        "13:02:06.123456000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME",
        "1674478926123456789",
        "13:02:06.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "-1674478926", "10:57:54.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME",
        "-1674478926123",
        "10:57:53.877000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME",
        "-1674478926123456",
        "10:57:53.876544000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME",
        "-1674478926123456789",
        "10:57:53.876543211 Z",
        new StringProvider(),
        new StringProvider());

    // java.time.LocalTime
    testIngestion("TIME", LocalTime.of(23, 59, 59), "23:59:59.000000000 Z", new StringProvider());

    // java.time.OffsetTime
    testIngestion(
        "TIME",
        OffsetTime.of(23, 59, 59, 0, ZoneOffset.ofHoursMinutes(4, 0)),
        "23:59:59.000000000 Z",
        new StringProvider());
    testIngestion(
        "TIME",
        OffsetTime.of(23, 59, 59, 123, ZoneOffset.ofHoursMinutes(-4, 0)),
        "23:59:59.000000123 Z",
        new StringProvider());
    testIngestion(
        "TIME",
        OffsetTime.of(23, 59, 59, 123456789, ZoneOffset.ofHoursMinutes(-4, 0)),
        "23:59:59.123456789 Z",
        new StringProvider());

    // Limited scale
    testIngestion("TIME(0)", "13:00:00", "13:00:00. Z", new StringProvider());
    testIngestion("TIME(0)", "13:00:00.123456", "13:00:00. Z", new StringProvider());
    testIngestion("TIME(1)", "13:00:00", "13:00:00.0 Z", new StringProvider());
    testIngestion("TIME(1)", "13:00:00.123456", "13:00:00.1 Z", new StringProvider());
    testIngestion("TIME(2)", "13:00:00.1", "13:00:00.10 Z", new StringProvider());
    testIngestion("TIME(2)", "13:00:00.123456", "13:00:00.12 Z", new StringProvider());
    testIngestion("TIME(8)", "13:00:00.1", "13:00:00.10000000 Z", new StringProvider());
    testIngestion("TIME(8)", "13:00:00.123456789", "13:00:00.12345678 Z", new StringProvider());
    testIngestion("TIME(8)", "13:00:00.000000009", "13:00:00.00000000 Z", new StringProvider());
    testIngestion("TIME(8)", "13:00:00.000000019", "13:00:00.00000001 Z", new StringProvider());
  }

  @Test
  public void testDate() throws Exception {
    // Test timestamp formats
    testJdbcTypeCompatibility(
        "DATE", "2013-04-28T20:57", "2013-04-28", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "2013-04-28T20:57:01", "2013-04-28", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28T20:57:01-07:00",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28T20:57:01.123456",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28T20:57:01.123456789+07:00",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "2013-04-28T20:57+07:00", "2013-04-28", new StringProvider(), new StringProvider());

    // Test date formats
    testJdbcTypeCompatibility(
        "DATE", "2013-04-28", "2013-04-28", new StringProvider(), new StringProvider());

    // Test LocalDate
    testIngestion("DATE", LocalDate.parse("2007-12-03"), "2007-12-03", new StringProvider());
    // Test LocalDateTime
    testIngestion(
        "DATE", LocalDateTime.parse("2007-12-03T10:15:30.123"), "2007-12-03", new StringProvider());
    // Test OffsetDateTime
    testIngestion(
        "DATE",
        OffsetDateTime.parse("2007-12-03T02:15:30.123+09:00"),
        "2007-12-03",
        new StringProvider());
    testIngestion(
        "DATE",
        OffsetDateTime.parse("2007-12-03T02:15:30.123-08:00"),
        "2007-12-03",
        new StringProvider());
    // Test ZonedDateTime
    testIngestion(
        "DATE",
        ZonedDateTime.parse("2007-12-03T02:15:30.123+09:00[Asia/Tokyo]"),
        "2007-12-03",
        new StringProvider());
    testIngestion(
        "DATE",
        ZonedDateTime.parse("2007-12-03T02:15:30.123-08:00[America/Los_Angeles]"),
        "2007-12-03",
        new StringProvider());
    // Test Instant
    testIngestion(
        "DATE",
        ZonedDateTime.parse("2007-12-03T02:15:30.123-08:00[America/Los_Angeles]").toInstant(),
        "2007-12-03",
        new StringProvider());

    // Test leap years
    testJdbcTypeCompatibility(
        "DATE",
        "2024-02-29T23:59:59.999999999Z",
        "2024-02-29",
        new StringProvider(),
        new StringProvider());
    expectNotSupported("DATE", "2023-02-29T23:59:59.999999999");

    // Test numeric strings
    testJdbcTypeCompatibility(
        "DATE", "0", "1970-01-01", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "1662731080", "2022-09-09", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "1662731080123", "2022-09-09", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "1662731080123456", "2022-09-09", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "1662731080123456789", "2022-09-09", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "-1674478926", "1916-12-09", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "-1674478926123", "1916-12-09", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "-1674478926123456", "1916-12-09", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "-1674478926123456789", "1916-12-09", new StringProvider(), new StringProvider());
  }

  @Test
  public void testOldTimestamps() throws Exception {
    // DATE
    testJdbcTypeCompatibility("DATE", "0001-12-31", new StringProvider());
    testJdbcTypeCompatibility("DATE", "0000-01-01", new StringProvider());
    testJdbcTypeCompatibility("DATE", "-0001-01-01", new StringProvider());
    testJdbcTypeCompatibility("DATE", "-9999-01-01", new StringProvider());

    // TIMESTAMP_NTZ
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "0001-12-31T11:11:11",
        "0001-12-31 11:11:11.000000000 Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "0001-01-01T00:00:00",
        "0001-01-01 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());

    // TIMESTAMP_TZ
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "0001-12-31T11:11:11+03:00",
        "0001-12-31 11:11:11.000000000 +0300",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "0001-01-01T00:00:00+03:00",
        "0001-01-01 00:00:00.000000000 +0300",
        new StringProvider(),
        new StringProvider());

    // TIMESTAMP_LTZ
    conn.createStatement()
        .execute("alter session set timezone = 'UTC';"); // workaround for SNOW-727474
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "0001-12-31T11:11:11+00:00",
        "0001-12-31 11:11:11.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "0001-01-01T00:00:00+00:00",
        "0001-01-01 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
  }

  @Test
  public void testJulianGregorianGap() throws Exception {
    // During switch from Julian to Gregorian calendars, there is a 10-day gap. The next day after
    // 1582-10-04 was 1582-10-15.
    // Snowflake doesn't have any special handling of this, these dates can be normally inserted.
    testJdbcTypeCompatibility(
        "DATE", "1582-10-04", "1582-10-04", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "1582-10-08", "1582-10-08", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "1582-10-15", "1582-10-15", new StringProvider(), new StringProvider());
  }

  @Test
  public void testFutureDates() throws Exception {
    setJdbcSessionTimezone(TZ_LOS_ANGELES);
    setChannelDefaultTimezone(TZ_LOS_ANGELES);
    testJdbcTypeCompatibility(
        "DATE", "9999-12-31", "9999-12-31", new StringProvider(), new StringProvider());

    // SB16
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "9999-12-31T23:59:59.999999999",
        "9999-12-31 23:59:59.999999999 Z",
        new StringProvider(),
        new StringProvider());

    // SB8
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(7)",
        "9999-12-31T23:59:59.999999999",
        "9999-12-31 23:59:59.9999999 Z",
        new StringProvider(),
        new StringProvider());

    // SB16
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "9999-12-31T23:59:59.999999999",
        "9999-12-31 23:59:59.999999999 -0800",
        new StringProvider(),
        new StringProvider());

    // SB8
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(7)",
        "9999-12-31T23:59:59.999999999",
        "9999-12-31 23:59:59.9999999 -0800",
        new StringProvider(),
        new StringProvider());

    // SB16
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "9999-12-31T23:59:59.999999999",
        "9999-12-31 23:59:59.999999999 -0800",
        new StringProvider(),
        new StringProvider());

    // SB8
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(3)",
        "9999-12-31T23:59:59.999999999",
        "9999-12-31 23:59:59.999 -0800",
        new StringProvider(),
        new StringProvider());
  }

  /**
   * To make assertions of timestamps without timezone to work, make sure JDBC connection session
   * time zone is the same as the streaming ingest default timezone
   */
  private void setJdbcSessionTimezone(ZoneId timezone) throws SQLException {
    conn.createStatement().execute(String.format("alter session set timezone = '%s';", timezone));
  }
}

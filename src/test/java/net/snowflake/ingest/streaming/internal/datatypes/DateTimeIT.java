package net.snowflake.ingest.streaming.internal.datatypes;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Supported date, time and timestamp formats:
 * https://docs.snowflake.com/en/user-guide/date-time-input-output.html#date-formats
 */
public class DateTimeIT extends AbstractDataTypeTest {

  @Test
  public void testTimestampWithTimeZone() throws Exception {
    useLosAngelesTimeZone();

    // Test timestamp formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2/18/2008 02:36:48",
        "2008-02-18 02:36:48.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20",
        "2013-04-28 20:00:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57",
        "2013-04-28 20:57:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57:01",
        "2013-04-28 20:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57:01 +07:00",
        "2013-04-28 20:57:01.000000000 +0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57:01 +0700",
        "2013-04-28 20:57:01.000000000 +0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57:01-07",
        "2013-04-28 20:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57:01-07:00",
        "2013-04-28 20:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57:01.123456",
        "2013-04-28 20:57:01.123456000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57:01.123456789 +07:00",
        "2013-04-28 20:57:01.123456789 +0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57:01.123456789 +0700",
        "2013-04-28 20:57:01.123456789 +0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57:01.123456789+07",
        "2013-04-28 20:57:01.123456789 +0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57:01.123456789+07:00",
        "2013-04-28 20:57:01.123456789 +0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28 20:57+07:00",
        "2013-04-28 20:57:00.000000000 +0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28T20",
        "2013-04-28 20:00:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
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
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28T20:57+07:00",
        "2013-04-28 20:57:00.000000000 +0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "Mon Jul 08 18:09:51 +0000 2013",
        "2013-07-08 18:09:51.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "Thu, 21 Dec 2000 04:01:07 PM",
        "2000-12-21 16:01:07.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "Thu, 21 Dec 2000 04:01:07 PM +0200",
        "2000-12-21 16:01:07.000000000 +0200",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "Thu, 21 Dec 2000 04:01:07.123456789 PM",
        "2000-12-21 16:01:07.123456789 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "Thu, 21 Dec 2000 04:01:07.123456789 PM +0200",
        "2000-12-21 16:01:07.123456789 +0200",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "Thu, 21 Dec 2000 16:01:07",
        "2000-12-21 16:01:07.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "Thu, 21 Dec 2000 16:01:07 +0200",
        "2000-12-21 16:01:07.000000000 +0200",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "Thu, 21 Dec 2000 16:01:07.123456789",
        "2000-12-21 16:01:07.123456789 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "Thu, 21 Dec 2000 16:01:07.123456789 +0200",
        "2000-12-21 16:01:07.123456789 +0200",
        new StringProvider(),
        new StringProvider());

    // Test date formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2013-04-28",
        "2013-04-28 00:00:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "17-DEC-1980",
        "1980-12-17 00:00:00.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "12/17/1980",
        "1980-12-17 00:00:00.000000000 -0800",
        new StringProvider(),
        new StringProvider());

    // Test leap years
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "2024-02-29T23:59:59.999999999Z",
        "2024-02-29 23:59:59.999999999 Z",
        new StringProvider(),
        new StringProvider());
    expectArrowNotSupported("TIMESTAMP_TZ", "2023-02-29T23:59:59.999999999Z");

    // Test numeric strings
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "0",
        "1970-01-01 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "86399",
        "1970-01-01 23:59:59.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "86401",
        "1970-01-02 00:00:01.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "-86401",
        "1969-12-30 23:59:59.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "-86399",
        "1969-12-31 00:00:01.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "1662731080",
        "2022-09-09 13:44:40.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "1662731080123456789",
        "2022-09-09 13:44:40.123456789 Z",
        new StringProvider(),
        new StringProvider());
  }

  @Test
  public void testTimestampWithLocalTimeZone() throws Exception {
    useLosAngelesTimeZone();

    // Test timestamp formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2/18/2008 02:36:48",
        "2008-02-18 02:36:48.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20",
        "2013-04-28 20:00:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57",
        "2013-04-28 20:57:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57:01",
        "2013-04-28 20:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57:01 +07:00",
        "2013-04-28 06:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57:01 +0700",
        "2013-04-28 06:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57:01-07",
        "2013-04-28 20:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57:01-07:00",
        "2013-04-28 20:57:01.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57:01.123456",
        "2013-04-28 20:57:01.123456000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57:01.123456789 +07:00",
        "2013-04-28 06:57:01.123456789 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57:01.123456789 +0700",
        "2013-04-28 06:57:01.123456789 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57:01.123456789+07",
        "2013-04-28 06:57:01.123456789 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57:01.123456789+07:00",
        "2013-04-28 06:57:01.123456789 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28 20:57+07:00",
        "2013-04-28 06:57:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28T20",
        "2013-04-28 20:00:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
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
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28T20:57+07:00",
        "2013-04-28 06:57:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "Mon Jul 08 18:09:51 +0000 2013",
        "2013-07-08 11:09:51.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "Thu, 21 Dec 2000 04:01:07 PM",
        "2000-12-21 16:01:07.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "Thu, 21 Dec 2000 04:01:07 PM +0200",
        "2000-12-21 06:01:07.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "Thu, 21 Dec 2000 04:01:07.123456789 PM",
        "2000-12-21 16:01:07.123456789 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "Thu, 21 Dec 2000 04:01:07.123456789 PM +0200",
        "2000-12-21 06:01:07.123456789 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "Thu, 21 Dec 2000 16:01:07",
        "2000-12-21 16:01:07.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "Thu, 21 Dec 2000 16:01:07 +0200",
        "2000-12-21 06:01:07.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "Thu, 21 Dec 2000 16:01:07.123456789",
        "2000-12-21 16:01:07.123456789 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "Thu, 21 Dec 2000 16:01:07.123456789 +0200",
        "2000-12-21 06:01:07.123456789 -0800",
        new StringProvider(),
        new StringProvider());

    // Test date formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "2013-04-28",
        "2013-04-28 00:00:00.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "17-DEC-1980",
        "1980-12-17 00:00:00.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "12/17/1980",
        "1980-12-17 00:00:00.000000000 -0800",
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
    expectArrowNotSupported("TIMESTAMP_LTZ", "2023-02-29T23:59:59.999999999Z");

    // Test numeric strings
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "0",
        "1969-12-31 16:00:00.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "86399",
        "1970-01-01 15:59:59.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "86401",
        "1970-01-01 16:00:01.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "-86401",
        "1969-12-30 15:59:59.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "-86399",
        "1969-12-30 16:00:01.000000000 -0800",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "1662731080",
        "2022-09-09 06:44:40.000000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ",
        "1662731080123456789",
        "2022-09-09 06:44:40.123456789 -0700",
        new StringProvider(),
        new StringProvider());
  }

  @Test
  public void testTimestampWithoutTimeZone() throws Exception {
    // Test timestamp formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2/18/2008 02:36:48",
        "2008-02-18 02:36:48.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20",
        "2013-04-28 20:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57",
        "2013-04-28 20:57:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57:01",
        "2013-04-28 20:57:01.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57:01 +07:00",
        "2013-04-28 20:57:01.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57:01 +0700",
        "2013-04-28 20:57:01.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57:01-07",
        "2013-04-28 20:57:01.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57:01-07:00",
        "2013-04-28 20:57:01.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57:01.123456",
        "2013-04-28 20:57:01.123456000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57:01.123456789 +07:00",
        "2013-04-28 20:57:01.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57:01.123456789 +0700",
        "2013-04-28 20:57:01.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57:01.123456789+07",
        "2013-04-28 20:57:01.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57:01.123456789+07:00",
        "2013-04-28 20:57:01.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28 20:57+07:00",
        "2013-04-28 20:57:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28T20",
        "2013-04-28 20:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
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
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28T20:57+07:00",
        "2013-04-28 20:57:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "Mon Jul 08 18:09:51 +0000 2013",
        "2013-07-08 18:09:51.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "Thu, 21 Dec 2000 04:01:07 PM",
        "2000-12-21 16:01:07.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "Thu, 21 Dec 2000 04:01:07 PM +0200",
        "2000-12-21 16:01:07.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "Thu, 21 Dec 2000 04:01:07.123456789 PM",
        "2000-12-21 16:01:07.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "Thu, 21 Dec 2000 04:01:07.123456789 PM +0200",
        "2000-12-21 16:01:07.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "Thu, 21 Dec 2000 16:01:07",
        "2000-12-21 16:01:07.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "Thu, 21 Dec 2000 16:01:07 +0200",
        "2000-12-21 16:01:07.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "Thu, 21 Dec 2000 16:01:07.123456789",
        "2000-12-21 16:01:07.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "Thu, 21 Dec 2000 16:01:07.123456789 +0200",
        "2000-12-21 16:01:07.123456789 Z",
        new StringProvider(),
        new StringProvider());

    // Test date formats
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "2013-04-28",
        "2013-04-28 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "17-DEC-1980",
        "1980-12-17 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "12/17/1980",
        "1980-12-17 00:00:00.000000000 Z",
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
    expectArrowNotSupported("TIMESTAMP_NTZ", "2023-02-29T23:59:59.999999999Z");

    // Test numeric strings
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "0",
        "1970-01-01 00:00:00.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "86399",
        "1970-01-01 23:59:59.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "86401",
        "1970-01-02 00:00:01.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "-86401",
        "1969-12-30 23:59:59.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "-86399",
        "1969-12-31 00:00:01.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "1662731080",
        "2022-09-09 13:44:40.000000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ",
        "1662731080123456789",
        "2022-09-09 13:44:40.123456789 Z",
        new StringProvider(),
        new StringProvider());
  }

  @Test
  public void testJavaTimeObjects() throws Exception {
    // TIME (LocalTime and OffsetTime are supported)
    testIngestion("TIME", LocalTime.of(23, 59, 59), "23:59:59.000000000 Z", new StringProvider());
    testIngestion(
        "TIME",
        OffsetTime.of(23, 59, 59, 0, ZoneOffset.ofHoursMinutes(4, 0)),
        "23:59:59.000000000 Z",
        new StringProvider());
    testIngestion(
        "TIME",
        OffsetTime.of(23, 59, 59, 0, ZoneOffset.ofHoursMinutes(-4, 0)),
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

    // DATE (LocalDate, LocalDateTime, OffsetDateTime, ZonedDateTime are supported)
    testIngestion("DATE", LocalDate.parse("2007-12-03"), "2007-12-03", new StringProvider());
    testIngestion(
        "DATE", LocalDateTime.parse("2007-12-03T10:15:30"), "2007-12-03", new StringProvider());
    testIngestion(
        "DATE",
        OffsetDateTime.parse("2007-12-03T10:15:30+01:00"),
        "2007-12-03",
        new StringProvider());
    testIngestion(
        "DATE",
        ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]"),
        "2007-12-03",
        new StringProvider());

    // TIMESTAMP_NTZ (LocalDate, LocalDateTime, OffsetDateTime, ZonedDateTime are supported)
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
        OffsetDateTime.parse("2007-12-03T10:15:30+01:00"),
        "2007-12-03 10:15:30.000000000 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ",
        ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]"),
        "2007-12-03 10:15:30.000000000 Z",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_NTZ",
        ZonedDateTime.parse("2007-12-03T10:15:30.123456789+01:00[Europe/Paris]"),
        "2007-12-03 10:15:30.123456789 Z",
        new StringProvider());

    useLosAngelesTimeZone();
    // TIMESTAMP_LTZ (LocalDate, LocalDateTime, OffsetDateTime, ZonedDateTime are supported)
    testIngestion(
        "TIMESTAMP_LTZ",
        LocalDate.parse("2007-12-03"),
        "2007-12-03 00:00:00.000000000 -0800",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ",
        LocalDateTime.parse("2007-12-03T10:15:30"),
        "2007-12-03 10:15:30.000000000 -0800",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ",
        OffsetDateTime.parse("2007-12-03T10:15:30+01:00"),
        "2007-12-03 01:15:30.000000000 -0800",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ",
        ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]"),
        "2007-12-03 01:15:30.000000000 -0800",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_LTZ",
        ZonedDateTime.parse("2007-12-03T10:15:30.123456789+01:00[Europe/Paris]"),
        "2007-12-03 01:15:30.123456789 -0800",
        new StringProvider());

    // TIMESTAMP_TZ (LocalDate, LocalDateTime, OffsetDateTime, ZonedDateTime are supported)
    testIngestion(
        "TIMESTAMP_TZ",
        LocalDate.parse("2007-12-03"),
        "2007-12-03 00:00:00.000000000 -0800",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ",
        LocalDateTime.parse("2007-12-03T10:15:30"),
        "2007-12-03 10:15:30.000000000 -0800",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ",
        OffsetDateTime.parse("2007-12-03T10:15:30+01:00"),
        "2007-12-03 10:15:30.000000000 +0100",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ",
        ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]"),
        "2007-12-03 10:15:30.000000000 +0100",
        new StringProvider());
    testIngestion(
        "TIMESTAMP_TZ",
        ZonedDateTime.parse("2007-12-03T10:15:30.123456789+01:00[Europe/Paris]"),
        "2007-12-03 10:15:30.123456789 +0100",
        new StringProvider());
  }

  @Test
  public void testTime() throws Exception {
    // All (7) documented time formats are supported
    // https://docs.snowflake.com/en/user-guide/date-time-input-output.html#time-formats

    testJdbcTypeCompatibility(
        "TIME",
        "20:57:01.123456789+07:00",
        "20:57:01.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME",
        "20:57:01.123456789",
        "20:57:01.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "20:57:01", "20:57:01.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "20:57", "20:57:00.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME",
        "07:57:01.123456789 AM",
        "07:57:01.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "04:01:07 AM", "04:01:07.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "04:01 PM", "16:01:00.000000000 Z", new StringProvider(), new StringProvider());

    // Test numeric strings
    testJdbcTypeCompatibility(
        "TIME", "0", "00:00:00.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "86399", "23:59:59.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "86401", "00:00:01.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "-86401", "23:59:59.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "-86399", "00:00:01.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME", "1662731080", "13:44:40.000000000 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME",
        "1662731080123456789",
        "13:44:40.123456789 Z",
        new StringProvider(),
        new StringProvider());
  }

  @Test
  public void testLimitedScale() throws Exception {
    // Of TIME
    testJdbcTypeCompatibility(
        "TIME(0)", "13:00:00.999", "13:00:00. Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME(0)", "13:00:00.999999999", "13:00:00. Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME(0)",
        "13:00:00.999999999+07:30",
        "13:00:00. Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME(0)",
        "1662731080123456789",
        "13:44:40. Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIME(4)", "13:00:00.999", "13:00:00.9990 Z", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "TIME(4)",
        "13:00:00.999999999",
        "13:00:00.9999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME(4)",
        "13:00:00.999999999+07:30",
        "13:00:00.9999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME(4)",
        "1662731080123456789",
        "13:44:40.1234 Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIME(9)",
        "13:00:00.999",
        "13:00:00.999000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME(9)",
        "13:00:00.999999999",
        "13:00:00.999999999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME(9)",
        "13:00:00.999999999+07:30",
        "13:00:00.999999999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME(9)",
        "1662731080123456789",
        "13:44:40.123456789 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIME(9)",
        "1662731080123456",
        "13:44:40.123456000 Z",
        new StringProvider(),
        new StringProvider());

    // Of TIMESTAMP_NTZ
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(0)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00. Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(0)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00. Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(0)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-07 13:00:00. Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(0)",
        "1662731080123456789",
        "2022-09-09 13:44:40. Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(4)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.9990 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(4)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.9999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(4)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-07 13:00:00.9999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(4)",
        "1662731080123456789",
        "2022-09-09 13:44:40.1234 Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(7)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.9990000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(7)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.9999999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(7)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-07 13:00:00.9999999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(7)",
        "1662731080123456789",
        "2022-09-09 13:44:40.1234567 Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(8)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.99900000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(8)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.99999999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(8)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-07 13:00:00.99999999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(8)",
        "1662731080123456789",
        "2022-09-09 13:44:40.12345678 Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(9)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.999000000 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(9)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.999999999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(9)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-07 13:00:00.999999999 Z",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_NTZ(9)",
        "1662731080123456789",
        "2022-09-09 13:44:40.123456789 Z",
        new StringProvider(),
        new StringProvider());

    // Of TIMESTAMP_LTZ
    useLosAngelesTimeZone();
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(0)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00. -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(0)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00. -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(0)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-06 22:30:00. -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(0)",
        "1662731080123456789",
        "2022-09-09 06:44:40. -0700",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(4)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.9990 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(4)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.9999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(4)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-06 22:30:00.9999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(4)",
        "1662731080123456789",
        "2022-09-09 06:44:40.1234 -0700",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(7)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.9990000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(7)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.9999999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(7)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-06 22:30:00.9999999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(7)",
        "1662731080123456789",
        "2022-09-09 06:44:40.1234567 -0700",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(8)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.99900000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(8)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.99999999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(8)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-06 22:30:00.99999999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(8)",
        "1662731080123456789",
        "2022-09-09 06:44:40.12345678 -0700",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(9)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.999000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(9)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.999999999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(9)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-06 22:30:00.999999999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_LTZ(9)",
        "1662731080123456789",
        "2022-09-09 06:44:40.123456789 -0700",
        new StringProvider(),
        new StringProvider());

    // Of TIMESTAMP_TZ
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(0)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00. -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(0)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00. -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(0)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-07 13:00:00. +0730",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(0)",
        "1662731080123456789",
        "2022-09-09 13:44:40. Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(4)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.9990 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(4)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.9999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(4)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-07 13:00:00.9999 +0730",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(4)",
        "1662731080123456789",
        "2022-09-09 13:44:40.1234 Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(7)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.9990000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(7)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.9999999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(7)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-07 13:00:00.9999999 +0730",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(7)",
        "1662731080123456789",
        "2022-09-09 13:44:40.1234567 Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(8)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.99900000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(8)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.99999999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(8)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-07 13:00:00.99999999 +0730",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(8)",
        "1662731080123456789",
        "2022-09-09 13:44:40.12345678 Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(9)",
        "2010-07-07 13:00:00.999",
        "2010-07-07 13:00:00.999000000 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(9)",
        "2010-07-07 13:00:00.999999999",
        "2010-07-07 13:00:00.999999999 -0700",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(9)",
        "2010-07-07 13:00:00.999999999+07:30",
        "2010-07-07 13:00:00.999999999 +0730",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(9)",
        "1662731080123456789",
        "2022-09-09 13:44:40.123456789 Z",
        new StringProvider(),
        new StringProvider());
  }

  @Test
  public void testDate() throws Exception {
    // Test timestamp formats
    testJdbcTypeCompatibility(
        "DATE", "2/18/2008 02:36:48", "2008-02-18", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "2013-04-28 20", "2013-04-28", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "2013-04-28 20:57", "2013-04-28", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "2013-04-28 20:57:01", "2013-04-28", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28 20:57:01 +07:00",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28 20:57:01 +0700",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "2013-04-28 20:57:01-07", "2013-04-28", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28 20:57:01-07:00",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28 20:57:01.123456",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28 20:57:01.123456789 +07:00",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28 20:57:01.123456789 +0700",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28 20:57:01.123456789+07",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28 20:57:01.123456789+07:00",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "2013-04-28 20:57+07:00", "2013-04-28", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "2013-04-28T20", "2013-04-28", new StringProvider(), new StringProvider());
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
    testJdbcTypeCompatibility(
        "DATE",
        "Mon Jul 08 18:09:51 +0000 2013",
        "2013-07-08",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "Thu, 21 Dec 2000 04:01:07 PM",
        "2000-12-21",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "Thu, 21 Dec 2000 04:01:07 PM +0200",
        "2000-12-21",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "Thu, 21 Dec 2000 04:01:07.123456789 PM",
        "2000-12-21",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "Thu, 21 Dec 2000 04:01:07.123456789 PM +0200",
        "2000-12-21",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "Thu, 21 Dec 2000 16:01:07",
        "2000-12-21",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "Thu, 21 Dec 2000 16:01:07 +0200",
        "2000-12-21",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "Thu, 21 Dec 2000 16:01:07.123456789",
        "2000-12-21",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "Thu, 21 Dec 2000 16:01:07.123456789 +0200",
        "2000-12-21",
        new StringProvider(),
        new StringProvider());

    // Test date formats
    testJdbcTypeCompatibility(
        "DATE", "2013-04-28", "2013-04-28", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "17-DEC-1980", "1980-12-17", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "12/17/1980", "1980-12-17", new StringProvider(), new StringProvider());

    // Test leap years
    testJdbcTypeCompatibility(
        "DATE",
        "2024-02-29T23:59:59.999999999Z",
        "2024-02-29",
        new StringProvider(),
        new StringProvider());
    expectArrowNotSupported("DATE", "2023-02-29T23:59:59.999999999Z");

    testJdbcTypeCompatibility(
        "DATE", "9999-12-31", "9999-12-31", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "9999-12-31T23:59:59.999999999Z",
        "9999-12-31",
        new StringProvider(),
        new StringProvider());

    // Test boundary date
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28T00:00:00+07:00",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "DATE",
        "2013-04-28T00:00:00-07:00",
        "2013-04-28",
        new StringProvider(),
        new StringProvider());

    // Test numeric strings
    testJdbcTypeCompatibility(
        "DATE", "0", "1970-01-01", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "86399", "1970-01-01", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "86401", "1970-01-02", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "-86401", "1969-12-30", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "-86399", "1969-12-31", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "1662731080", "2022-09-09", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "1662731080123456789", "2022-09-09", new StringProvider(), new StringProvider());
  }

  @Test
  @Ignore("SNOW-663646")
  public void testOldValues() throws Exception {
    testJdbcTypeCompatibility(
        "DATE", "1582-01-01", "1582-10-01", new StringProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "DATE", "1582-10-14", "1582-10-14", new StringProvider(), new StringProvider());
  }

  @Test
  public void testFutureDates() throws Exception {
    useLosAngelesTimeZone();

    testJdbcTypeCompatibility(
        "DATE", "99999-12-31", "99999-12-31", new StringProvider(), new StringProvider());

    testJdbcTypeCompatibility( // SB16
        "TIMESTAMP_NTZ",
        "9999-12-31 23:59:59.999999999",
        "9999-12-31 23:59:59.999999999 Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility( // SB8
        "TIMESTAMP_NTZ(7)",
        "9999-12-31 23:59:59.999999999",
        "9999-12-31 23:59:59.9999999 Z",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility( // SB16
        "TIMESTAMP_LTZ",
        "9999-12-31 23:59:59.999999999",
        "9999-12-31 23:59:59.999999999 -0800",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility( // SB8
        "TIMESTAMP_LTZ(7)",
        "9999-12-31 23:59:59.999999999",
        "9999-12-31 23:59:59.9999999 -0800",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ",
        "9999-12-31 23:59:59.999999999",
        "9999-12-31 23:59:59.999999999 -0800",
        new StringProvider(),
        new StringProvider());

    testJdbcTypeCompatibility(
        "TIMESTAMP_TZ(3)",
        "9999-12-31 23:59:59.999999999",
        "9999-12-31 23:59:59.999 -0800",
        new StringProvider(),
        new StringProvider());
  }

  /**
   * To make assertions of timestamps without timezone to work, make sure JDBC connection session
   * time zone is the same as the streaming ingest default timezone
   */
  private void useLosAngelesTimeZone() throws SQLException {
    conn.createStatement().execute("alter session set timezone = 'America/Los_Angeles';");
  }
}

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.DataValidationUtil.BYTES_16_MB;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.BYTES_8_MB;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.isAllowedSemiStructuredType;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseArray;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseBigDecimal;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseBinary;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseBoolean;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseDate;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseObject;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseReal;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseString;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseTime;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseTimestampNtzSb16;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseTimestampTz;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseVariant;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.xml.bind.DatatypeConverter;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;

public class DataValidationUtilTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private void expectErrorCodeAndMessage(
      ErrorCode expectedErrorCode, String expectedExceptionMessage, Runnable action) {
    try {
      action.run();
      Assert.fail("Expected Exception");
    } catch (SFException e) {
      assertEquals(expectedErrorCode.getMessageCode(), e.getVendorCode());
      if (expectedExceptionMessage != null) assertEquals(expectedExceptionMessage, e.getMessage());
    } catch (Exception e) {
      Assert.fail("Invalid error through");
    }
  }

  private void expectError(ErrorCode expectedErrorCode, Runnable action) {
    expectErrorCodeAndMessage(expectedErrorCode, null, action);
  }

  @Test
  public void testValidateAndParseTime() {
    assertEquals(5L, validateAndParseTime("COL", "00:00:05", 0).longValueExact());
    assertEquals(5000L, validateAndParseTime("COL", "00:00:05", 3).longValueExact());
    assertEquals(5000L, validateAndParseTime("COL", "00:00:05.000", 3).longValueExact());
    assertEquals(5123L, validateAndParseTime("COL", "00:00:05.123", 3).longValueExact());
    assertEquals(5123L, validateAndParseTime("COL", "00:00:05.123456", 3).longValueExact());
    assertEquals(
        5123456789L, validateAndParseTime("COL", "00:00:05.123456789", 9).longValueExact());

    assertEquals(72L, validateAndParseTime("COL", "72", 0).longValueExact());
    assertEquals(72000L, validateAndParseTime("COL", "72", 3).longValueExact());

    // Timestamps are rejected
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2/18/2008 02:36:48", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28 20", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28 20:57", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28 20:57:01", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28 20:57:01 +07:00", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28 20:57:01 +0700", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28 20:57:01-07", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28 20:57:01-07:00", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28 20:57:01.123456", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "2013-04-28 20:57:01.123456789 +07:00", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "2013-04-28 20:57:01.123456789 +0700", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "2013-04-28 20:57:01.123456789+07", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "2013-04-28 20:57:01.123456789+07:00", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28 20:57+07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28T20", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28T20:57", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28T20:57:01", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28T20:57:01-07:00", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28T20:57:01.123456", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "2013-04-28T20:57:01.123456789+07:00", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28T20:57+07:00", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "Mon Jul 08 18:09:51 +0000 2013", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "Thu, 21 Dec 2000 04:01:07 PM", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "Thu, 21 Dec 2000 04:01:07 PM +0200", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "Thu, 21 Dec 2000 04:01:07.123456789 PM", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "Thu, 21 Dec 2000 04:01:07.123456789 PM +0200", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "Thu, 21 Dec 2000 16:01:07", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "Thu, 21 Dec 2000 16:01:07 +0200", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "Thu, 21 Dec 2000 16:01:07.123456789", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", "Thu, 21 Dec 2000 16:01:07.123456789 +0200", 9));

    // Dates are rejected
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "2013-04-28", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "17-DEC-1980", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "12/17/1980", 9));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", LocalDate.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", LocalDateTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", OffsetDateTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", ZonedDateTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", new Date(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", 1.5f, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", 1.5, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "1.5", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "1.0", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", new Object(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", false, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", "foo", 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", java.sql.Time.valueOf("20:57:00"), 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", java.sql.Date.valueOf("2010-11-03"), 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("COL", java.sql.Timestamp.valueOf("2010-11-03 20:57:00"), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", BigInteger.ZERO, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", BigDecimal.ZERO, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("COL", 'c', 3));
  }

  @Test
  public void testValidateAndParseTimestampNtzSb16() {
    assertEquals(
        new TimestampWrapper(1609462800, 123000000, new BigInteger("1609462800123000000")),
        DataValidationUtil.validateAndParseTimestampNtzSb16(
            "COL", "2021-01-01 01:00:00.123", 9, true));

    // Time formats are not supported
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("COL", "20:57:01.123456789+07:00", 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("COL", "20:57:01.123456789", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", "20:57:01", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", "20:57", 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("COL", "07:57:01.123456789 AM", 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("COL", "04:01:07 AM", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", "04:01 AM", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", "04:01 PM", 3, false));

    // Test forbidden values
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("COL", LocalTime.now(), 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("COL", OffsetTime.now(), 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", new Date(), 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", 1.5f, 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", 1.5, 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", "1.5", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", "1.0", 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("COL", new Object(), 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", false, 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", "", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", "foo", 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("COL", java.sql.Time.valueOf("20:57:00"), 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () ->
            validateAndParseTimestampNtzSb16("COL", java.sql.Date.valueOf("2010-11-03"), 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () ->
            validateAndParseTimestampNtzSb16(
                "COL", java.sql.Timestamp.valueOf("2010-11-03 20:57:00"), 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("COL", BigInteger.ZERO, 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("COL", BigDecimal.ZERO, 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("COL", 'c', 3, false));
  }

  @Test
  public void testValidateAndPareTimestampTz() {
    TimestampWrapper result =
        DataValidationUtil.validateAndParseTimestampTz("COL", "2021-01-01 01:00:00.123 +0100", 4);
    assertEquals(1609459200, result.getEpoch());
    assertEquals(123000000, result.getFraction());
    assertEquals(Optional.of(3600000), result.getTimezoneOffset());
    assertEquals(Optional.of(1500), result.getTimeZoneIndex());

    // Time formats are not supported
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampTz("COL", "20:57:01.123456789+07:00", 3));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", "20:57:01.123456789", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", "20:57:01", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", "20:57", 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampTz("COL", "07:57:01.123456789 AM", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", "04:01:07 AM", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", "04:01 AM", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", "04:01 PM", 3));

    // Test forbidden values
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", LocalTime.now(), 3));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", OffsetTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", new Date(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", 1.5f, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", 1.5, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", "1.5", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", "1.0", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", new Object(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", false, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", "", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", "foo", 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampTz("COL", java.sql.Time.valueOf("20:57:00"), 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampTz("COL", java.sql.Date.valueOf("2010-11-03"), 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () ->
            validateAndParseTimestampTz(
                "COL", java.sql.Timestamp.valueOf("2010-11-03 20:57:00"), 3));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", BigInteger.ZERO, 3));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", BigDecimal.ZERO, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("COL", 'c', 3));
  }

  @Test
  public void testValidateAndParseBigDecimal() {
    assertEquals(new BigDecimal("1"), validateAndParseBigDecimal("COL", "1"));
    assertEquals(
        new BigDecimal("1000").toBigInteger(),
        validateAndParseBigDecimal("COL", "1e3").toBigInteger());
    assertEquals(
        new BigDecimal("-1000").toBigInteger(),
        validateAndParseBigDecimal("COL", "-1e3").toBigInteger());
    assertEquals(
        new BigDecimal("1").toBigInteger(),
        validateAndParseBigDecimal("COL", "1e0").toBigInteger());
    assertEquals(
        new BigDecimal("-1").toBigInteger(),
        validateAndParseBigDecimal("COL", "-1e0").toBigInteger());
    assertEquals(
        new BigDecimal("123").toBigInteger(),
        validateAndParseBigDecimal("COL", "1.23e2").toBigInteger());
    assertEquals(
        new BigDecimal("123.4").toBigInteger(),
        validateAndParseBigDecimal("COL", "1.234e2").toBigInteger());
    assertEquals(
        new BigDecimal("0.1234").toBigInteger(),
        validateAndParseBigDecimal("COL", "1.234e-1").toBigInteger());
    assertEquals(
        new BigDecimal("0.1234").toBigInteger(),
        validateAndParseBigDecimal("COL", "1234e-5").toBigInteger());
    assertEquals(
        new BigDecimal("0.1234").toBigInteger(),
        validateAndParseBigDecimal("COL", "1234E-5").toBigInteger());
    assertEquals(new BigDecimal("1"), validateAndParseBigDecimal("COL", 1));
    assertEquals(new BigDecimal("1.0"), validateAndParseBigDecimal("COL", 1D));
    assertEquals(new BigDecimal("1"), validateAndParseBigDecimal("COL", 1L));
    assertEquals(new BigDecimal("1.0"), validateAndParseBigDecimal("COL", 1F));
    assertEquals(
        BigDecimal.valueOf(10).pow(37),
        validateAndParseBigDecimal("COL", BigDecimal.valueOf(10).pow(37)));
    assertEquals(
        BigDecimal.valueOf(-1).multiply(BigDecimal.valueOf(10).pow(37)),
        validateAndParseBigDecimal(
            "COL", BigInteger.valueOf(-1).multiply(BigInteger.valueOf(10).pow(37))));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBigDecimal("COL", "honk"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBigDecimal("COL", "0x22"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBigDecimal("COL", true));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBigDecimal("COL", false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBigDecimal("COL", new Object()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBigDecimal("COL", 'a'));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBigDecimal("COL", new byte[4]));
  }

  @Test
  public void testValidateAndParseString() {
    assertEquals("honk", validateAndParseString("COL", "honk", Optional.empty()));

    // Check max String length
    StringBuilder longBuilder = new StringBuilder();
    for (int i = 0; i < BYTES_16_MB; i++) {
      longBuilder.append("č"); // max string length is measured in chars, not bytes
    }
    String maxString = longBuilder.toString();
    assertEquals(maxString, validateAndParseString("COL", maxString, Optional.empty()));

    // max length - 1 should also succeed
    longBuilder.setLength(BYTES_16_MB - 1);
    String maxStringMinusOne = longBuilder.toString();
    assertEquals(
        maxStringMinusOne, validateAndParseString("COL", maxStringMinusOne, Optional.empty()));

    // max length + 1 should fail
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseString("COL", longBuilder.append("aa").toString(), Optional.empty()));

    // Test max length validation
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseString("COL", "12345", Optional.of(4)));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseString("COL", false, Optional.of(4)));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseString("COL", 12345, Optional.of(4)));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseString("COL", 1.2345, Optional.of(4)));

    // Test unsupported values
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseString("COL", new Object(), Optional.empty()));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseString("COL", new byte[] {}, Optional.of(4)));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseString("COL", new char[] {}, Optional.of(4)));
  }

  @Test
  public void testValidateAndParseVariant() throws Exception {
    assertEquals("1", validateAndParseVariant("COL", 1));
    assertEquals("1", validateAndParseVariant("COL", "1"));
    assertEquals("1", validateAndParseVariant("COL", "                          1   "));
    String stringVariant = "{\"key\":1}";
    assertEquals(stringVariant, validateAndParseVariant("COL", stringVariant));

    // Test custom serializers
    assertEquals(
        "[-128,0,127]",
        validateAndParseVariant("COL", new byte[] {Byte.MIN_VALUE, 0, Byte.MAX_VALUE}));
    assertEquals(
        "\"2022-09-28T03:04:12.123456789-07:00\"",
        validateAndParseVariant(
            "COL",
            ZonedDateTime.of(2022, 9, 28, 3, 4, 12, 123456789, ZoneId.of("America/Los_Angeles"))));

    // Test valid JSON tokens
    assertEquals("null", validateAndParseVariant("COL", null));
    assertEquals("null", validateAndParseVariant("COL", "null"));
    assertEquals("true", validateAndParseVariant("COL", true));
    assertEquals("true", validateAndParseVariant("COL", "true"));
    assertEquals("false", validateAndParseVariant("COL", false));
    assertEquals("false", validateAndParseVariant("COL", "false"));
    assertEquals("{}", validateAndParseVariant("COL", "{}"));
    assertEquals("[]", validateAndParseVariant("COL", "[]"));
    assertEquals("[\"foo\",1,null]", validateAndParseVariant("COL", "[\"foo\",1,null]"));
    assertEquals("\"\"", validateAndParseVariant("COL", "\"\""));

    // Test missing values are null instead of empty string
    assertNull(validateAndParseVariant("COL", ""));
    assertNull(validateAndParseVariant("COL", "  "));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseVariant("COL", "{null}"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseVariant("COL", "}{"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseVariant("COL", readTree("{}")));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseVariant("COL", new Object()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseVariant("COL", "foo"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseVariant("COL", new Date()));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseVariant("COL", Collections.singletonList(new Object())));
    expectError(
        ErrorCode.INVALID_ROW,
        () ->
            validateAndParseVariant(
                "COL", Collections.singletonList(Collections.singletonMap("foo", new Object()))));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseVariant("COL", Collections.singletonMap(new Object(), "foo")));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseVariant("COL", Collections.singletonMap("foo", new Object())));
  }

  @Test
  public void testValidateAndParseArray() throws Exception {
    assertEquals("[1]", validateAndParseArray("COL", 1));
    assertEquals("[1]", validateAndParseArray("COL", "1"));
    assertEquals("[1]", validateAndParseArray("COL", "                          1   "));
    int[] intArray = new int[] {1, 2, 3};
    assertEquals("[1,2,3]", validateAndParseArray("COL", intArray));

    String[] stringArray = new String[] {"a", "b", "c"};
    assertEquals("[\"a\",\"b\",\"c\"]", validateAndParseArray("COL", stringArray));

    Object[] objectArray = new Object[] {1, 2, 3};
    assertEquals("[1,2,3]", validateAndParseArray("COL", objectArray));

    Object[] ObjectArrayWithNull = new Object[] {1, null, 3};
    assertEquals("[1,null,3]", validateAndParseArray("COL", ObjectArrayWithNull));

    Object[][] nestedArray = new Object[][] {{1, 2, 3}, null, {4, 5, 6}};
    assertEquals("[[1,2,3],null,[4,5,6]]", validateAndParseArray("COL", nestedArray));

    List<Integer> intList = Arrays.asList(1, 2, 3);
    assertEquals("[1,2,3]", validateAndParseArray("COL", intList));

    List<Object> objectList = Arrays.asList(1, 2, 3);
    assertEquals("[1,2,3]", validateAndParseArray("COL", objectList));

    List<Object> nestedList = Arrays.asList(Arrays.asList(1, 2, 3), 2, 3);
    assertEquals("[[1,2,3],2,3]", validateAndParseArray("COL", nestedList));

    // Test null values
    assertEquals("[null]", validateAndParseArray("COL", ""));
    assertEquals("[null]", validateAndParseArray("COL", " "));
    assertEquals("[null]", validateAndParseArray("COL", "null"));
    assertEquals("[null]", validateAndParseArray("COL", null));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseArray("COL", readTree("[]")));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseArray("COL", new Object()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseArray("COL", "foo")); // invalid JSO)N
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseArray("COL", new Date()));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseArray("COL", Collections.singletonList(new Object())));
    expectError(
        ErrorCode.INVALID_ROW,
        () ->
            validateAndParseArray(
                "COL", Collections.singletonList(Collections.singletonMap("foo", new Object()))));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseArray("COL", Collections.singletonMap(new Object(), "foo")));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseArray("COL", Collections.singletonMap("foo", new Object())));
  }

  @Test
  public void testValidateAndParseObject() throws Exception {
    String stringObject = "{\"key\":1}";
    assertEquals(stringObject, validateAndParseObject("COL", stringObject));

    String badObject = "foo";
    try {
      validateAndParseObject("COL", badObject);
      Assert.fail("Expected INVALID_ROW error");
    } catch (SFException err) {
      assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), err.getVendorCode());
    }

    char[] data = new char[20000000];
    Arrays.fill(data, 'a');
    String stringVal = new String(data);
    Map<String, String> mapVal = new HashMap<>();
    mapVal.put("key", stringVal);
    String tooLargeObject = objectMapper.writeValueAsString(mapVal);
    try {
      validateAndParseObject("COL", tooLargeObject);
      Assert.fail("Expected INVALID_ROW error");
    } catch (SFException err) {
      assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), err.getVendorCode());
    }

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseObject("COL", readTree("{}")));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseObject("COL", "[]"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseObject("COL", "1"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseObject("COL", 1));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseObject("COL", 1.5));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseObject("COL", false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseObject("COL", new Object()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseObject("COL", "foo"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseObject("COL", new Date()));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseObject("COL", Collections.singletonList(new Object())));
    expectError(
        ErrorCode.INVALID_ROW,
        () ->
            validateAndParseObject(
                "COL", Collections.singletonList(Collections.singletonMap("foo", new Object()))));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseObject("COL", Collections.singletonMap(new Object(), "foo")));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseObject("COL", Collections.singletonMap("foo", new Object())));
  }

  @Test
  public void testTooLargeVariant() {
    char[] stringContent = new char[16 * 1024 * 1024 - 16]; // {"a":"11","b":""}
    Arrays.fill(stringContent, 'c');

    // {"a":"11","b":""}
    Map<String, Object> m = new HashMap<>();
    m.put("a", "11");
    m.put("b", new String(stringContent));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseVariant("COL", m));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseArray("COL", m));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseObject("COL", m));
  }

  @Test
  public void testTooLargeMultiByteSemiStructuredValues() {
    // Variant max size is not in characters, but in bytes
    char[] stringContent = new char[9 * 1024 * 1024]; // 8MB < value < 16MB
    Arrays.fill(stringContent, 'Č');

    Map<String, Object> m = new HashMap<>();
    m.put("a", new String(stringContent));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: {a=ČČČČČČČČČČČČČČČČČ.... Value cannot"
            + " be ingested into Snowflake column COL of type VARIANT: Variant too long:"
            + " length=18874376 maxLength=16777152",
        () -> validateAndParseVariant("COL", m));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: [{\"a\":\"ČČČČČČČČČČČČČ.... Value"
            + " cannot be ingested into Snowflake column COL of type ARRAY: Array too large."
            + " length=18874378 maxLength=16777152",
        () -> validateAndParseArray("COL", m));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: {\"a\":\"ČČČČČČČČČČČČČČ.... Value"
            + " cannot be ingested into Snowflake column COL of type OBJECT: Object too large."
            + " length=18874376 maxLength=16777152",
        () -> validateAndParseObject("COL", m));
  }

  @Test
  public void testValidVariantType() {
    // Test primitive types
    Assert.assertTrue(isAllowedSemiStructuredType((byte) 1));
    Assert.assertTrue(isAllowedSemiStructuredType((short) 1));
    Assert.assertTrue(isAllowedSemiStructuredType(1));
    Assert.assertTrue(isAllowedSemiStructuredType(1L));
    Assert.assertTrue(isAllowedSemiStructuredType(1.25f));
    Assert.assertTrue(isAllowedSemiStructuredType(1.25d));
    Assert.assertTrue(isAllowedSemiStructuredType(false));
    Assert.assertTrue(isAllowedSemiStructuredType('c'));

    // Test boxed primitive types
    Assert.assertTrue(isAllowedSemiStructuredType(Byte.valueOf((byte) 1)));
    Assert.assertTrue(isAllowedSemiStructuredType(Short.valueOf((short) 1)));
    Assert.assertTrue(isAllowedSemiStructuredType(Integer.valueOf(1)));
    Assert.assertTrue(isAllowedSemiStructuredType(Long.valueOf(1L)));
    Assert.assertTrue(isAllowedSemiStructuredType(Float.valueOf(1.25f)));
    Assert.assertTrue(isAllowedSemiStructuredType(Double.valueOf(1.25d)));
    Assert.assertTrue(isAllowedSemiStructuredType(Boolean.valueOf(false)));
    Assert.assertTrue(isAllowedSemiStructuredType(Character.valueOf('c')));

    // Test primitive arrays
    Assert.assertTrue(isAllowedSemiStructuredType(new byte[] {1}));
    Assert.assertTrue(isAllowedSemiStructuredType(new short[] {1}));
    Assert.assertTrue(isAllowedSemiStructuredType(new int[] {1}));
    Assert.assertTrue(isAllowedSemiStructuredType(new long[] {1L}));
    Assert.assertTrue(isAllowedSemiStructuredType(new float[] {1.25f}));
    Assert.assertTrue(isAllowedSemiStructuredType(new double[] {1.25d}));
    Assert.assertTrue(isAllowedSemiStructuredType(new boolean[] {false}));
    Assert.assertTrue(isAllowedSemiStructuredType(new char[] {'c'}));

    // Test primitive lists
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList((byte) 1)));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList((short) 1)));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(1)));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(1L)));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(1.25f)));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(1.25d)));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(false)));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList('c')));

    // Test additional numeric types and their collections
    Assert.assertTrue(isAllowedSemiStructuredType(new BigInteger("1")));
    Assert.assertTrue(isAllowedSemiStructuredType(new BigInteger[] {new BigInteger("1")}));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(new BigInteger("1"))));
    Assert.assertTrue(isAllowedSemiStructuredType(new BigDecimal("1.25")));
    Assert.assertTrue(isAllowedSemiStructuredType(new BigDecimal[] {new BigDecimal("1.25")}));
    Assert.assertTrue(
        isAllowedSemiStructuredType(Collections.singletonList(new BigDecimal("1.25"))));

    // Test strings
    Assert.assertTrue(isAllowedSemiStructuredType("foo"));
    Assert.assertTrue(isAllowedSemiStructuredType(new String[] {"foo"}));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList("foo")));

    // Test date/time objects and their collections
    Assert.assertTrue(isAllowedSemiStructuredType(LocalTime.now()));
    Assert.assertTrue(isAllowedSemiStructuredType(OffsetTime.now()));
    Assert.assertTrue(isAllowedSemiStructuredType(LocalDate.now()));
    Assert.assertTrue(isAllowedSemiStructuredType(LocalDateTime.now()));
    Assert.assertTrue(isAllowedSemiStructuredType(ZonedDateTime.now()));
    Assert.assertTrue(isAllowedSemiStructuredType(OffsetDateTime.now()));
    Assert.assertTrue(isAllowedSemiStructuredType(new LocalTime[] {LocalTime.now()}));
    Assert.assertTrue(isAllowedSemiStructuredType(new OffsetTime[] {OffsetTime.now()}));
    Assert.assertTrue(isAllowedSemiStructuredType(new LocalDate[] {LocalDate.now()}));
    Assert.assertTrue(isAllowedSemiStructuredType(new LocalDateTime[] {LocalDateTime.now()}));
    Assert.assertTrue(isAllowedSemiStructuredType(new ZonedDateTime[] {ZonedDateTime.now()}));
    Assert.assertTrue(isAllowedSemiStructuredType(new OffsetDateTime[] {OffsetDateTime.now()}));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(LocalTime.now())));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(OffsetTime.now())));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(LocalDate.now())));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(LocalDateTime.now())));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(ZonedDateTime.now())));
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonList(OffsetDateTime.now())));

    // Test mixed collections
    Assert.assertTrue(
        isAllowedSemiStructuredType(
            new Object[] {
              1,
              false,
              new BigInteger("1"),
              LocalDateTime.now(),
              new Object[] {new Object[] {new Object[] {LocalDateTime.now(), false}}}
            }));
    Assert.assertFalse(
        isAllowedSemiStructuredType(
            new Object[] {
              1,
              false,
              new BigInteger("1"),
              LocalDateTime.now(),
              new Object[] {new Object[] {new Object[] {new Object(), false}}}
            }));
    Assert.assertTrue(
        isAllowedSemiStructuredType(
            Arrays.asList(
                new BigInteger("1"),
                "foo",
                false,
                Arrays.asList(13, Arrays.asList(Arrays.asList(false, 'c'))))));
    Assert.assertFalse(
        isAllowedSemiStructuredType(
            Arrays.asList(
                new BigInteger("1"),
                "foo",
                false,
                Arrays.asList(13, Arrays.asList(Arrays.asList(new Object(), 'c'))))));

    // Test maps
    Assert.assertTrue(isAllowedSemiStructuredType(Collections.singletonMap("foo", "bar")));
    Assert.assertFalse(isAllowedSemiStructuredType(Collections.singletonMap(new Object(), "foo")));
    Assert.assertFalse(isAllowedSemiStructuredType(Collections.singletonMap("foo", new Object())));
    Assert.assertTrue(
        isAllowedSemiStructuredType(
            Collections.singletonMap(
                "foo",
                new Object[] {
                  1,
                  false,
                  new BigInteger("1"),
                  LocalDateTime.now(),
                  new Object[] {new Object[] {new Object[] {LocalDateTime.now(), false}}}
                })));
    Assert.assertFalse(
        isAllowedSemiStructuredType(
            Collections.singletonMap(
                "foo",
                new Object[] {
                  1,
                  false,
                  new BigInteger("1"),
                  LocalDateTime.now(),
                  new Object[] {new Object[] {new Object[] {new Object(), false}}}
                })));
    Assert.assertTrue(
        isAllowedSemiStructuredType(
            Collections.singletonMap(
                "foo",
                Arrays.asList(
                    new BigInteger("1"),
                    "foo",
                    false,
                    Arrays.asList(13, Arrays.asList(Arrays.asList(false, 'c')))))));
    Assert.assertFalse(
        isAllowedSemiStructuredType(
            Collections.singletonMap(
                "foo",
                Arrays.asList(
                    new BigInteger("1"),
                    "foo",
                    false,
                    Arrays.asList(13, Arrays.asList(Arrays.asList(new Object(), 'c')))))));
  }

  @Test
  public void testValidateAndParseDate() {
    assertEquals(-923, validateAndParseDate("COL", "1967-06-23"));
    assertEquals(-923, validateAndParseDate("COL", "1967-06-23 01:01:01"));
    assertEquals(18464, validateAndParseDate("COL", "2020-07-21"));
    assertEquals(18464, validateAndParseDate("COL", "2020-07-21 23:31:00"));

    // Time formats are not supported
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", "20:57:01.123456789+07:00"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", "20:57:01.123456789"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", "20:57:01"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", "20:57"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", "07:57:01.123456789 AM"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", "04:01:07 AM"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", "04:01 AM"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", "04:01 PM"));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", new Object()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", LocalTime.now()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", OffsetTime.now()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", new java.util.Date()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", ""));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", "foo"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", "1.0"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", 'c'));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", 1));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", 1L));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", 1.25));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", BigInteger.valueOf(1)));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("COL", BigDecimal.valueOf(1.25)));
  }

  @Test
  public void testValidateAndParseBinary() {
    byte[] maxAllowedArray = new byte[BYTES_8_MB];
    byte[] maxAllowedArrayMinusOne = new byte[BYTES_8_MB - 1];

    assertArrayEquals(
        "honk".getBytes(StandardCharsets.UTF_8),
        validateAndParseBinary("COL", "honk".getBytes(StandardCharsets.UTF_8), Optional.empty()));

    assertArrayEquals(
        new byte[] {-1, 0, 1},
        validateAndParseBinary("COL", new byte[] {-1, 0, 1}, Optional.empty()));
    assertArrayEquals(
        DatatypeConverter.parseHexBinary(
            "1234567890abcdef"), // pragma: allowlist secret NOT A SECRET
        validateAndParseBinary(
            "COL", "1234567890abcdef", Optional.empty())); // pragma: allowlist secret NOT A SECRET

    assertArrayEquals(
        maxAllowedArray, validateAndParseBinary("COL", maxAllowedArray, Optional.empty()));
    assertArrayEquals(
        maxAllowedArrayMinusOne,
        validateAndParseBinary("COL", maxAllowedArrayMinusOne, Optional.empty()));

    // Too large arrays should be rejected
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseBinary("COL", new byte[1], Optional.of(0)));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseBinary("COL", new byte[BYTES_8_MB + 1], Optional.empty()));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseBinary("COL", new byte[8], Optional.of(7)));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary("COL", "aabb", Optional.of(1)));

    // unsupported data types should fail
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseBinary("COL", "000", Optional.empty()));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseBinary("COL", "abcg", Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary("COL", "c", Optional.empty()));
    expectError(
        ErrorCode.INVALID_ROW,
        () ->
            validateAndParseBinary(
                "COL", Arrays.asList((byte) 1, (byte) 2, (byte) 3), Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary("COL", 1, Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary("COL", 12, Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary("COL", 1.5, Optional.empty()));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseBinary("COL", BigInteger.ONE, Optional.empty()));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseBinary("COL", false, Optional.empty()));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseBinary("COL", new Object(), Optional.empty()));
  }

  @Test
  public void testValidateAndParseReal() throws Exception {
    // From number types
    assertEquals(1.23d, validateAndParseReal("COL", 1.23f), 0);
    assertEquals(1.23d, validateAndParseReal("COL", 1.23), 0);
    assertEquals(1.23d, validateAndParseReal("COL", 1.23d), 0);
    assertEquals(1.23d, validateAndParseReal("COL", new BigDecimal("1.23")), 0);
    assertEquals(Double.NaN, validateAndParseReal("COL", "Nan"), 0);
    assertEquals(Double.POSITIVE_INFINITY, validateAndParseReal("COL", "inF"), 0);
    assertEquals(Double.NEGATIVE_INFINITY, validateAndParseReal("COL", "-inF"), 0);

    // From string
    assertEquals(1.23d, validateAndParseReal("COL", "1.23"), 0);
    assertEquals(123d, validateAndParseReal("COL", "1.23E2"), 0);
    assertEquals(123d, validateAndParseReal("COL", "1.23e2"), 0);

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseReal("COL", "foo"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseReal("COL", 'c'));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseReal("COL", new Object()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseReal("COL", false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseReal("COL", true));
  }

  @Test
  public void testValidateAndParseBoolean() {

    for (Object input :
        Arrays.asList(
            true, "true", "True", "TruE", "t", "yes", "YeS", "y", "on", "1", 1.1, -1.1, -10, 10)) {
      assertEquals(1, validateAndParseBoolean("COL", input));
    }

    for (Object input :
        Arrays.asList(false, "false", "False", "FalsE", "f", "no", "NO", "n", "off", "0", 0)) {
      assertEquals(0, validateAndParseBoolean("COL", input));
    }

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBoolean("COL", new Object()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBoolean("COL", 't'));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBoolean("COL", 'f'));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBoolean("COL", new int[] {}));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBoolean("COL", "foobar"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBoolean("COL", ""));
  }

  /**
   * Tests that exception message are constructed correctly when ingesting forbidden Java type, as
   * well a value of an allowed type, but in invalid format
   */
  @Test
  public void testExceptionMessages() {
    // BOOLEAN
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type BOOLEAN. Allowed Java types: boolean,"
            + " Number, String",
        () -> validateAndParseBoolean("COL", new Object()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column COL of type BOOLEAN: Not a valid boolean, see"
            + " https://docs.snowflake.com/en/sql-reference/data-types-logical.html#conversion-to-boolean"
            + " for the list of supported formats",
        () -> validateAndParseBoolean("COL", "abc"));

    // TIME
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type TIME. Allowed Java types: String,"
            + " LocalTime, OffsetTime",
        () -> validateAndParseTime("COL", new Object(), 10));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column COL of type TIME: Not a valid time, see"
            + " https://docs.snowflake.com/en/user-guide/date-time-input-output.html#time-formats"
            + " for the list of supported formats",
        () -> validateAndParseTime("COL", "abc", 10));

    // DATE
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type DATE. Allowed Java types: String,"
            + " LocalDate, LocalDateTime, ZonedDateTime, OffsetDateTime",
        () -> validateAndParseDate("COL", new Object()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column COL of type DATE: Not a valid date, see"
            + " https://docs.snowflake.com/en/user-guide/date-time-input-output.html#date-formats"
            + " for the list of supported formats",
        () -> validateAndParseDate("COL", "abc"));

    // TIMESTAMP_NTZ
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type TIMESTAMP. Allowed Java types:"
            + " String, LocalDate, LocalDateTime, ZonedDateTime, OffsetDateTime",
        () -> validateAndParseTimestampNtzSb16("COL", new Object(), 3, true));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column COL of type TIMESTAMP: Not a valid timestamp, see"
            + " https://docs.snowflake.com/en/user-guide/date-time-input-output.html#timestamp-formats"
            + " for the list of supported formats",
        () -> validateAndParseTimestampNtzSb16("COL", "abc", 3, true));

    // TIMESTAMP_LTZ
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type TIMESTAMP. Allowed Java types:"
            + " String, LocalDate, LocalDateTime, ZonedDateTime, OffsetDateTime",
        () -> validateAndParseTimestampNtzSb16("COL", new Object(), 3, false));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column COL of type TIMESTAMP: Not a valid timestamp, see"
            + " https://docs.snowflake.com/en/user-guide/date-time-input-output.html#timestamp-formats"
            + " for the list of supported formats",
        () -> validateAndParseTimestampNtzSb16("COL", "abc", 3, false));

    // TIMESTAMP_TZ
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type TIMESTAMP. Allowed Java types:"
            + " String, LocalDate, LocalDateTime, ZonedDateTime, OffsetDateTime",
        () -> validateAndParseTimestampTz("COL", new Object(), 3));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column COL of type TIMESTAMP: Not a valid timestamp, see"
            + " https://docs.snowflake.com/en/user-guide/date-time-input-output.html#timestamp-formats"
            + " for the list of supported formats",
        () -> validateAndParseTimestampTz("COL", "abc", 3));

    // NUMBER
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type NUMBER. Allowed Java types: int,"
            + " long, byte, short, float, double, BigDecimal, BigInteger, String",
        () -> validateAndParseBigDecimal("COL", new Object()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column COL of type NUMBER: Not a valid number",
        () -> validateAndParseBigDecimal("COL", "abc"));

    // REAL
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type REAL. Allowed Java types: Number,"
            + " String",
        () -> validateAndParseReal("COL", new Object()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column COL of type REAL: Not a valid decimal number",
        () -> validateAndParseReal("COL", "abc"));

    // STRING
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type STRING. Allowed Java types: String,"
            + " Number, boolean, char",
        () -> validateAndParseString("COL", new Object(), Optional.empty()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column COL of type STRING: String too long: length=3 maxLength=2",
        () -> validateAndParseString("COL", "abc", Optional.of(2)));

    // BINARY
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type BINARY. Allowed Java types: byte[],"
            + " String",
        () -> validateAndParseBinary("COL", new Object(), Optional.empty()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: byte[2]. Value cannot be ingested into"
            + " Snowflake column COL of type BINARY: Binary too long: length=2 maxLength=1",
        () -> validateAndParseBinary("COL", new byte[] {1, 2}, Optional.of(1)));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: ghi. Value cannot be ingested into"
            + " Snowflake column COL of type BINARY: Not a valid hex string",
        () -> validateAndParseBinary("COL", "ghi", Optional.empty()));

    // VARIANT
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type VARIANT. Allowed Java types: String,"
            + " Primitive data types and their arrays, java.time.*, List<T>, Map<String, T>, T[]",
        () -> validateAndParseVariant("COL", new Object()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: ][. Value cannot be ingested into"
            + " Snowflake column COL of type VARIANT: Not a valid JSON",
        () -> validateAndParseVariant("COL", "]["));

    // ARRAY
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type ARRAY. Allowed Java types: String,"
            + " Primitive data types and their arrays, java.time.*, List<T>, Map<String, T>, T[]",
        () -> validateAndParseArray("COL", new Object()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: ][. Value cannot be ingested into"
            + " Snowflake column COL of type ARRAY: Not a valid JSON",
        () -> validateAndParseArray("COL", "]["));

    // OBJECT
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column COL of type OBJECT. Allowed Java types: String,"
            + " Primitive data types and their arrays, java.time.*, List<T>, Map<String, T>, T[]",
        () -> validateAndParseObject("COL", new Object()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: }{. Value cannot be ingested into"
            + " Snowflake column COL of type OBJECT: Not a valid JSON",
        () -> validateAndParseObject("COL", "}{"));
  }

  private JsonNode readTree(String value) {
    try {
      return objectMapper.readTree(value);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseBigDecimal;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseBoolean;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseDate;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseReal;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseTime;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseTimestampNtzSb16;
import static net.snowflake.ingest.streaming.internal.DataValidationUtil.validateAndParseTimestampTz;
import static org.junit.Assert.assertEquals;

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
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.xml.bind.DatatypeConverter;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;

public class DataValidationUtilTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final Object[] goodIntegersValue10 =
      new Object[] {10D, 10F, 10L, new BigInteger("10"), 10, "10", "1e1", "1.0e1"};

  private void expectError(ErrorCode expectedErrorCode, Function func, Object args) {
    expectError(expectedErrorCode, () -> func.apply(args));
  }

  private void expectErrorCodeAndMessage(
      ErrorCode expectedErrorCode, String expectedExceptionMessage, Runnable action) {
    try {
      action.run();
      Assert.fail("Expected Exception");
    } catch (SFException e) {
      assertEquals(expectedErrorCode.getMessageCode(), e.getVendorCode());
      if (expectedExceptionMessage != null)
        Assert.assertEquals(expectedExceptionMessage, e.getMessage());
    } catch (Exception e) {
      Assert.fail("Invalid error through");
    }
  }

  private void expectError(ErrorCode expectedErrorCode, Runnable action) {
    expectErrorCodeAndMessage(expectedErrorCode, null, action);
  }

  @Test
  public void testValidateAndParseTime() {
    assertEquals(5L, validateAndParseTime("00:00:05", 0).longValueExact());
    assertEquals(5000L, validateAndParseTime("00:00:05", 3).longValueExact());
    assertEquals(5000L, validateAndParseTime("00:00:05.000", 3).longValueExact());
    assertEquals(5123L, validateAndParseTime("00:00:05.123", 3).longValueExact());
    assertEquals(5123L, validateAndParseTime("00:00:05.123456", 3).longValueExact());
    assertEquals(5123456789L, validateAndParseTime("00:00:05.123456789", 9).longValueExact());

    assertEquals(72L, validateAndParseTime("72", 0).longValueExact());
    assertEquals(72000L, validateAndParseTime("72", 3).longValueExact());

    // Timestamps are rejected
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2/18/2008 02:36:48", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01 +07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01 +0700", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01-07", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01-07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01.123456", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("2013-04-28 20:57:01.123456789 +07:00", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("2013-04-28 20:57:01.123456789 +0700", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01.123456789+07", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("2013-04-28 20:57:01.123456789+07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57+07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20:57", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20:57:01", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20:57:01-07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20:57:01.123456", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("2013-04-28T20:57:01.123456789+07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20:57+07:00", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("Mon Jul 08 18:09:51 +0000 2013", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("Thu, 21 Dec 2000 04:01:07 PM", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("Thu, 21 Dec 2000 04:01:07 PM +0200", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("Thu, 21 Dec 2000 04:01:07.123456789 PM", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("Thu, 21 Dec 2000 04:01:07.123456789 PM +0200", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("Thu, 21 Dec 2000 16:01:07", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("Thu, 21 Dec 2000 16:01:07 +0200", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("Thu, 21 Dec 2000 16:01:07.123456789", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("Thu, 21 Dec 2000 16:01:07.123456789 +0200", 9));

    // Dates are rejected
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("17-DEC-1980", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("12/17/1980", 9));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(LocalDate.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(LocalDateTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(OffsetDateTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(ZonedDateTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(new Date(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(1.5f, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(1.5, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("1.5", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("1.0", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(new Object(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(false, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("foo", 3));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime(java.sql.Time.valueOf("20:57:00"), 3));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime(java.sql.Date.valueOf("2010-11-03"), 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime(java.sql.Timestamp.valueOf("2010-11-03 20:57:00"), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(BigInteger.ZERO, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(BigDecimal.ZERO, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime('c', 3));
  }

  @Test
  public void testValidateAndParseTimestampNtzSb16() {
    assertEquals(
        new TimestampWrapper(1609462800, 123000000, new BigInteger("1609462800123000000")),
        DataValidationUtil.validateAndParseTimestampNtzSb16("2021-01-01 01:00:00.123", 9, true));

    // Time formats are not supported
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("20:57:01.123456789+07:00", 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("20:57:01.123456789", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("20:57:01", 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("20:57", 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("07:57:01.123456789 AM", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("04:01:07 AM", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("04:01 AM", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("04:01 PM", 3, false));

    // Test forbidden values
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(LocalTime.now(), 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(OffsetTime.now(), 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(new Date(), 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(1.5f, 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(1.5, 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("1.5", 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("1.0", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(new Object(), 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(false, 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("", 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("foo", 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16(java.sql.Time.valueOf("20:57:00"), 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16(java.sql.Date.valueOf("2010-11-03"), 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () ->
            validateAndParseTimestampNtzSb16(
                java.sql.Timestamp.valueOf("2010-11-03 20:57:00"), 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(BigInteger.ZERO, 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(BigDecimal.ZERO, 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16('c', 3, false));
  }

  @Test
  public void testValidateAndPareTimestampTz() {
    TimestampWrapper result =
        DataValidationUtil.validateAndParseTimestampTz("2021-01-01 01:00:00.123 +0100", 4);
    assertEquals(1609459200, result.getEpoch());
    assertEquals(1230, result.getFraction());
    assertEquals(Optional.of(3600000), result.getTimezoneOffset());
    assertEquals(Optional.of(1500), result.getTimeZoneIndex());

    // Time formats are not supported
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("20:57:01.123456789+07:00", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("20:57:01.123456789", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("20:57:01", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("20:57", 3));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("07:57:01.123456789 AM", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("04:01:07 AM", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("04:01 AM", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("04:01 PM", 3));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(LocalTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(OffsetTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(new Date(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(1.5f, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(1.5, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("1.5", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("1.0", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(new Object(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(false, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("foo", 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampTz(java.sql.Time.valueOf("20:57:00"), 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampTz(java.sql.Date.valueOf("2010-11-03"), 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampTz(java.sql.Timestamp.valueOf("2010-11-03 20:57:00"), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(BigInteger.ZERO, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(BigDecimal.ZERO, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz('c', 3));
  }

  @Test
  public void testValidateAndParseBigDecimal() {
    assertEquals(new BigDecimal("1"), validateAndParseBigDecimal("1"));
    assertEquals(
        new BigDecimal("1000").toBigInteger(), validateAndParseBigDecimal("1e3").toBigInteger());
    assertEquals(
        new BigDecimal("-1000").toBigInteger(), validateAndParseBigDecimal("-1e3").toBigInteger());
    assertEquals(
        new BigDecimal("1").toBigInteger(), validateAndParseBigDecimal("1e0").toBigInteger());
    assertEquals(
        new BigDecimal("-1").toBigInteger(), validateAndParseBigDecimal("-1e0").toBigInteger());
    assertEquals(
        new BigDecimal("123").toBigInteger(), validateAndParseBigDecimal("1.23e2").toBigInteger());
    assertEquals(
        new BigDecimal("123.4").toBigInteger(),
        validateAndParseBigDecimal("1.234e2").toBigInteger());
    assertEquals(
        new BigDecimal("0.1234").toBigInteger(),
        validateAndParseBigDecimal("1.234e-1").toBigInteger());
    assertEquals(
        new BigDecimal("0.1234").toBigInteger(),
        validateAndParseBigDecimal("1234e-5").toBigInteger());
    assertEquals(
        new BigDecimal("0.1234").toBigInteger(),
        validateAndParseBigDecimal("1234E-5").toBigInteger());
    assertEquals(new BigDecimal("1"), validateAndParseBigDecimal(1));
    assertEquals(new BigDecimal("1.0"), validateAndParseBigDecimal(1D));
    assertEquals(new BigDecimal("1"), validateAndParseBigDecimal(1L));
    assertEquals(new BigDecimal("1.0"), validateAndParseBigDecimal(1F));
    assertEquals(
        BigDecimal.valueOf(10).pow(37), validateAndParseBigDecimal(BigDecimal.valueOf(10).pow(37)));
    assertEquals(
        BigDecimal.valueOf(-1).multiply(BigDecimal.valueOf(10).pow(37)),
        validateAndParseBigDecimal(
            BigInteger.valueOf(-1).multiply(BigInteger.valueOf(10).pow(37))));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, "honk");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, "0x22");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, true);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, false);
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, new Object());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, 'a');
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, new byte[4]);
  }

  @Test
  public void testValidateAndParseString() {
    Assert.assertEquals(
        "honk", DataValidationUtil.validateAndParseString("honk", Optional.empty()));

    // Check max String length
    StringBuilder longBuilder = new StringBuilder();
    for (int i = 0; i < DataValidationUtil.MAX_STRING_LENGTH + 1; i++) {
      longBuilder.append("a");
    }
    String tooLong = longBuilder.toString();

    try {
      DataValidationUtil.validateAndParseString(tooLong, Optional.empty());
      Assert.fail("Expected error for String too long");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }

    try {
      DataValidationUtil.validateAndParseString("123", Optional.of(2));
      Assert.fail("Expected error for String too long");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testValidateAndParseVariant() throws Exception {
    String stringVariant = "{\"key\":1}";
    Assert.assertEquals(stringVariant, DataValidationUtil.validateAndParseVariant(stringVariant));
    JsonNode nodeVariant = objectMapper.readTree(stringVariant);
    Assert.assertEquals(stringVariant, DataValidationUtil.validateAndParseVariant(nodeVariant));

    char[] data = new char[20000000];
    Arrays.fill(data, 'a');
    String stringVal = new String(data);
    try {
      DataValidationUtil.validateAndParseVariant(stringVal);
      Assert.fail("Expected INVALID_ROW error");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), err.getVendorCode());
    }
  }

  @Test
  public void testValidateAndParseArray() throws Exception {
    int invalidArray = 1;
    try {
      DataValidationUtil.validateAndParseArray(invalidArray);
      Assert.fail("Expected INVALID_ROW error");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), err.getVendorCode());
    }

    int[] intArray = new int[] {1, 2, 3};
    Assert.assertEquals("[1,2,3]", DataValidationUtil.validateAndParseArray(intArray));

    String[] stringArray = new String[] {"a", "b", "c"};
    Assert.assertEquals(
        "[\"a\",\"b\",\"c\"]", DataValidationUtil.validateAndParseArray(stringArray));

    Object[] objectArray = new Object[] {1, 2, 3};
    Assert.assertEquals("[1,2,3]", DataValidationUtil.validateAndParseArray(objectArray));

    Object[] ObjectArrayWithNull = new Object[] {1, null, 3};
    Assert.assertEquals(
        "[1,null,3]", DataValidationUtil.validateAndParseArray(ObjectArrayWithNull));

    Object[][] nestedArray = new Object[][] {{1, 2, 3}, null, {4, 5, 6}};
    Assert.assertEquals(
        "[[1,2,3],null,[4,5,6]]", DataValidationUtil.validateAndParseArray(nestedArray));

    List<Integer> intList = Arrays.asList(1, 2, 3);
    Assert.assertEquals("[1,2,3]", DataValidationUtil.validateAndParseArray(intList));

    List<Object> objectList = Arrays.asList(1, 2, 3);
    Assert.assertEquals("[1,2,3]", DataValidationUtil.validateAndParseArray(objectList));

    List<Object> nestedList = Arrays.asList(Arrays.asList(1, 2, 3), 2, 3);
    Assert.assertEquals("[[1,2,3],2,3]", DataValidationUtil.validateAndParseArray(nestedList));
  }

  @Test
  public void testValidateAndParseObject() throws Exception {
    String stringObject = "{\"key\":1}";
    Assert.assertEquals(stringObject, DataValidationUtil.validateAndParseObject(stringObject));
    JsonNode nodeObject = objectMapper.readTree(stringObject);
    Assert.assertEquals(stringObject, DataValidationUtil.validateAndParseObject(nodeObject));

    String badObject = "foo";
    try {
      DataValidationUtil.validateAndParseObject(badObject);
      Assert.fail("Expected INVALID_ROW error");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), err.getVendorCode());
    }

    char[] data = new char[20000000];
    Arrays.fill(data, 'a');
    String stringVal = new String(data);
    Map<String, String> mapVal = new HashMap<>();
    mapVal.put("key", stringVal);
    String tooLargeObject = objectMapper.writeValueAsString(mapVal);
    try {
      DataValidationUtil.validateAndParseObject(tooLargeObject);
      Assert.fail("Expected INVALID_ROW error");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), err.getVendorCode());
    }
  }

  @Test
  public void testValidateAndParseInteger() {
    for (Object input : goodIntegersValue10) {
      Assert.assertEquals(10, DataValidationUtil.validateAndParseInteger(input));
    }

    // Bad inputs
    // Double
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseInteger, 10.1D);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Double.valueOf(Integer.MAX_VALUE) + 1);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Double.valueOf(Integer.MIN_VALUE) - 1);

    // Float
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseInteger, 10.1F);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Float.valueOf(Integer.MAX_VALUE) * 2);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Float.valueOf(Integer.MIN_VALUE) * 2);

    // Long
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Long.valueOf(Integer.MAX_VALUE) + 1);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Long.valueOf(Integer.MIN_VALUE) - 1);

    // BigInteger
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        BigInteger.valueOf(Integer.MAX_VALUE).add(new BigInteger("1")));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        BigInteger.valueOf(Integer.MIN_VALUE).add(new BigInteger("-1")));

    // String
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        "Honk goes the noble goose");
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        BigInteger.valueOf(Integer.MAX_VALUE).add(new BigInteger("1")).toString());
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        BigInteger.valueOf(Integer.MIN_VALUE).add(new BigInteger("-1")).toString());
  }

  @Test
  public void testValidateAndParseDate() {
    assertEquals(-923, validateAndParseDate("1967-06-23"));
    assertEquals(-923, validateAndParseDate("1967-06-23 01:01:01"));
    assertEquals(18464, validateAndParseDate("2020-07-21"));
    assertEquals(18464, validateAndParseDate("2020-07-21 23:31:00"));

    // Time formats are not supported
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("20:57:01.123456789+07:00"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("20:57:01.123456789"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("20:57:01"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("20:57"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("07:57:01.123456789 AM"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("04:01:07 AM"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("04:01 AM"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("04:01 PM"));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, new Object());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, LocalTime.now());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, OffsetTime.now());
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, new java.util.Date());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, false);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, "");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, "foo");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, "1.0");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, 'c');
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, 1);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, 1L);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, 1.25);
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, BigInteger.valueOf(1));
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, BigDecimal.valueOf(1.25));
  }

  @Test
  public void testGetStringValue() throws Exception {
    Assert.assertEquals("123", DataValidationUtil.getStringValue("123"));
    Assert.assertEquals("123", DataValidationUtil.getStringValue(123));
    Assert.assertEquals("123", DataValidationUtil.getStringValue(new BigDecimal("123")));
    Assert.assertEquals("123", DataValidationUtil.getStringValue(new BigInteger("123")));
    Assert.assertEquals("123.0", DataValidationUtil.getStringValue(123f));
    Assert.assertEquals("123.0", DataValidationUtil.getStringValue(123d));
    Assert.assertEquals("123", DataValidationUtil.getStringValue(123l));
  }

  @Test
  public void testValidateAndParseBinary() {
    Assert.assertArrayEquals(
        "honk".getBytes(StandardCharsets.UTF_8),
        DataValidationUtil.validateAndParseBinary(
            "honk".getBytes(StandardCharsets.UTF_8), Optional.empty()));

    Assert.assertArrayEquals(
        DatatypeConverter.parseHexBinary("12"),
        DataValidationUtil.validateAndParseBinary("12", Optional.empty()));

    Assert.assertArrayEquals(
        DatatypeConverter.parseHexBinary("12"),
        DataValidationUtil.validateAndParseBinary(12, Optional.empty()));

    try {
      DataValidationUtil.validateAndParseBinary("1212", Optional.of(1));
      Assert.fail("Expected error for Binary too long");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }

    try {
      DataValidationUtil.validateAndParseBinary(123, Optional.empty());
      Assert.fail("Expected error for invalid Binary format");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testValidateAndParseReal() throws Exception {
    // From number types
    assertEquals(1.23d, validateAndParseReal(1.23f), 0);
    assertEquals(1.23d, validateAndParseReal(1.23), 0);
    assertEquals(1.23d, validateAndParseReal(1.23d), 0);
    assertEquals(1.23d, validateAndParseReal(new BigDecimal("1.23")), 0);

    // From string
    assertEquals(1.23d, validateAndParseReal("1.23"), 0);
    assertEquals(123d, validateAndParseReal("1.23E2"), 0);
    assertEquals(123d, validateAndParseReal("1.23e2"), 0);

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseReal, "foo");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseReal, 'c');
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseReal, new Object());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseReal, false);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseReal, true);
  }

  @Test
  public void testValidateAndParseBoolean() {

    for (Object input :
        Arrays.asList(
            true, "true", "True", "TruE", "t", "yes", "YeS", "y", "on", "1", 1.1, -1.1, -10, 10)) {
      assertEquals(1, validateAndParseBoolean(input));
    }

    for (Object input :
        Arrays.asList(false, "false", "False", "FalsE", "f", "no", "NO", "n", "off", "0", 0)) {
      assertEquals(0, validateAndParseBoolean(input));
    }

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, new Object());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, 't');
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, 'f');
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, new int[] {});
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, "foobar");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, "");
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
            + " be ingested into Snowflake column of type BOOLEAN. Allowed Java types: boolean,"
            + " Number, String",
        () -> validateAndParseBoolean(new Object()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column BOOLEAN",
        () -> validateAndParseBoolean("abc"));

    // TIME
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column of type TIME. Allowed Java types: String,"
            + " LocalTime, OffsetTime",
        () -> validateAndParseTime(new Object(), 10));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column TIME",
        () -> validateAndParseTime("abc", 10));

    // DATE
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column of type DATE. Allowed Java types: String,"
            + " LocalDate, LocalDateTime, ZonedDateTime, OffsetDateTime",
        () -> validateAndParseDate(new Object()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column DATE",
        () -> validateAndParseDate("abc"));

    // TIMESTAMP_NTZ
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column of type TIMESTAMP. Allowed Java types: String,"
            + " LocalDate, LocalDateTime, ZonedDateTime, OffsetDateTime",
        () -> validateAndParseTimestampNtzSb16(new Object(), 3, true));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column TIMESTAMP",
        () -> validateAndParseTimestampNtzSb16("abc", 3, true));

    // TIMESTAMP_LTZ
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column of type TIMESTAMP. Allowed Java types: String,"
            + " LocalDate, LocalDateTime, ZonedDateTime, OffsetDateTime",
        () -> validateAndParseTimestampNtzSb16(new Object(), 3, false));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column TIMESTAMP",
        () -> validateAndParseTimestampNtzSb16("abc", 3, false));

    // TIMESTAMP_TZ
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column of type TIMESTAMP. Allowed Java types: String,"
            + " LocalDate, LocalDateTime, ZonedDateTime, OffsetDateTime",
        () -> validateAndParseTimestampTz(new Object(), 3));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column TIMESTAMP",
        () -> validateAndParseTimestampTz("abc", 3));

    // NUMBER
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column of type NUMBER. Allowed Java types: int, long,"
            + " byte, short, float, double, BigDecimal, BigInteger, String",
        () -> validateAndParseBigDecimal(new Object()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column NUMBER",
        () -> validateAndParseBigDecimal("abc"));

    // REAL
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: Object of type java.lang.Object cannot"
            + " be ingested into Snowflake column of type REAL. Allowed Java types: Number, String",
        () -> validateAndParseReal(new Object()));
    expectErrorCodeAndMessage(
        ErrorCode.INVALID_ROW,
        "The given row cannot be converted to Arrow format: abc. Value cannot be ingested into"
            + " Snowflake column REAL",
        () -> validateAndParseReal("abc"));
  }
}
